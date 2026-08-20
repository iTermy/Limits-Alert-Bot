"""Tick-level replay of a signal's life, mirroring the live tick path.

The live check order (CLAUDE.md "Data Flow: Price Tick") is reproduced exactly:
limits fill first, then the stop loss, then auto-TP. Ties inside one tick resolve
in that order, which is what lets a tick that gaps through a limit and the stop
book both the fill and the loss.

Sides of the book:
  entry and SL read the ask for a long, the bid for a short
  exits read whichever side `Policy.exit_side` selects — "live" reproduces the
  bot (bid in both directions, per tp_monitor), "realistic" charges the spread a
  real close would pay (bid for a long, ask for a short, as trailing_monitor does)

The spread buffer widens the entry test by the live spread. Since that spread is
exactly ask - bid, `ask <= limit + spread` reduces to `bid <= limit`: with the
buffer on, a long fills when the BID touches the limit, which is one full spread
better than a real limit order would fill. Both regimes are simulated so the cost
of that assumption is measurable rather than baked in.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from backtest.ticks import TickStore, TickWindow

# Mirrors BaseThresholdConfig.get_pip_size. The stock branch must stay above the
# index-keyword branch (".NAS" contains "NAS").
_INDEX_KEYWORDS = ("NAS100", "US30", "US500", "SPX", "USTEC", "US2000", "DE30", "DE40",
                   "JP225", "HK50", "CHINA", "AUS2000", "UK100", "FRA40", "EU50", "CN50")

# Fractions of the TP threshold at which the first favourable and first adverse
# crossing are timestamped. 0.25 is the ordering bar used by mae_before_mfe;
# 0.5 is the bounce rule from the M1 manual-close analysis.
EXCURSION_BARS = (0.25, 0.5, 1.0)


def pip_size(symbol: str) -> float:
    s = symbol.upper()
    if ".NAS" in s or ".NYSE" in s:
        return 0.01
    if s.startswith("XAU") or s.startswith("GC"):
        return 0.01
    if s.startswith("XAG"):
        return 0.001
    if s.startswith("BTC"):
        return 1.0
    if any(k in s for k in ("ETH", "USDT")):
        return 0.1
    if any(k in s for k in _INDEX_KEYWORDS):
        return 1.0
    if "OIL" in s or "XTI" in s:
        return 0.01
    if s.endswith("JPY"):
        return 0.01
    return 0.0001


def is_pips_instrument(symbol: str) -> bool:
    """Forex quotes TP in pips; everything else in price units (dollars)."""
    s = symbol.upper()
    if ".NAS" in s or ".NYSE" in s:
        return False
    if s.startswith(("XAU", "XAG", "GC", "BTC")):
        return False
    if any(k in s for k in ("ETH", "USDT")):
        return False
    if any(k in s for k in _INDEX_KEYWORDS):
        return False
    if "OIL" in s or "XTI" in s:
        return False
    return len(s) == 6


@dataclass
class Policy:
    """A counterfactual exit/entry ruleset. Defaults reproduce live behaviour."""

    spread_buffer: bool = True          # live setting; False = fill at the bare level
    # How the entry order exists in the world:
    #   "signal" — the level itself, filled per `spread_buffer`. Reproduces the TM
    #              bot's signalling rule, which records the level as the fill price.
    #   "broker" — what the execution bot really places: the order rests one spread
    #              inside the level, so it triggers when the FAR side of the book
    #              reaches the level and transacts on the NEAR side, and its stop is
    #              shifted a spread further out. Entry is a spread worse than the
    #              level, the stop a spread more generous, and the fill lands where
    #              the TM bot's buffered rule says it does.
    entry_model: str = "signal"
    # Refuse limits that could not have been placed when the signal arrived: a buy
    # limit resting above the ask, a sell limit below the bid, or one whose stop
    # sits on the wrong side of its own entry. The execution bot skips exactly
    # these (`if adj_price >= tick.ask: return "skipped"`) and MT5 rejects the
    # rest. Replayed without the guard they fill instantly at the market and then
    # "stop out" at a price beyond the fill, booking large fictitious profits.
    require_placeable: bool = False
    exit_side: str = "live"             # live (bid both ways) | realistic (bid long / ask short)
    # The execution bot checks take-profit on a 1 s timer and then closes at
    # market, so it cannot take a spike that does not survive to the next check.
    # 0 replays exits tick-exact, which credits the strategy with those spikes.
    # The stop loss rests at the broker and always triggers tick-exact.
    exit_poll_seconds: float = 0.0
    tp_multiplier: float = 1.0          # scale the signal's own TP threshold
    tp_mode: str = "fixed"              # fixed | trailing | none
    # Trailing arms where the fixed TP would have fired and then ratchets, which is
    # what trailing_monitor shadows and therefore the question the user is asking.
    trail_distance: float = 1.0         # trailing gap, in units of the TP threshold
    trail_poll_seconds: float = 0.0     # cadence the trail's high water mark advances at
    sl_multiplier: float = 1.0          # scale |limit1 - stop_loss|
    max_fill_depth: Optional[int] = None    # ignore fills deeper than this
    skip_if_limits_over: Optional[int] = None   # drop the signal entirely
    breakeven_at: Optional[float] = None  # arm a BE stop once favourable move >= this x TP
    # Close at market this long after the first fill if nothing else has closed
    # it. These are level-bounce trades that are expected to react fast, so a
    # position still open much later is a thesis that did not happen.
    time_stop_minutes: Optional[float] = None
    # Fraction taken at the fixed-TP point when trailing, with the rest left to
    # run. 0 trails the whole position, 1 is plain fixed TP.
    partial_close: float = 0.0
    # Generous by default: a week-end expiry signal can fill days after it was
    # posted, and a short horizon silently turns those into false no-fills.
    horizon_hours: float = 400.0        # cap on hold time for counterfactuals
    honor_expiry: bool = True           # close at the signal's own expiry_time


@dataclass
class Fill:
    seq: int
    price: float
    idx: int
    time: datetime


@dataclass
class Outcome:
    signal_id: int
    instrument: str
    direction: str
    status: str = "no_data"     # filled | stopped | tp | expired | no_fill | no_data | skipped
    exit_reason: str = ""
    fills: list = field(default_factory=list)
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    entry_time: Optional[datetime] = None
    sl_price: Optional[float] = None
    thr_price: float = 0.0               # TP threshold in price units
    # In-trade excursion, measured over [first fill, exit] only. Reported as raw
    # prices so downstream can anchor on the first fill, the deepest fill or the
    # mean without the engine having picked one (DATA_ANALYSIS.md §5).
    mfe_price: Optional[float] = None
    mae_price: Optional[float] = None
    mfe_time: Optional[datetime] = None
    mae_time: Optional[datetime] = None
    post_exit_mfe_price: Optional[float] = None   # follow-through after the exit
    fav_bar_times: dict = field(default_factory=dict)   # bar fraction -> first favourable crossing
    adv_bar_times: dict = field(default_factory=dict)   # bar fraction -> first adverse crossing
    pnl_price: float = 0.0               # summed over fills, signed
    risk_price: float = 0.0              # 1R = full-fill risk over every limit
    risk_taken: float = 0.0              # risk on the limits that actually filled
    rho: Optional[float] = None
    n_ticks: int = 0

    @property
    def n_fills(self) -> int:
        return len(self.fills)


def _tp_level(direction: str, filled: list, thr_price: float) -> float:
    """Price the exit series must reach for auto-TP, given the filled limits.

    Mirrors tp_monitor: the last filled limit by sequence_number must clear the
    threshold, and any earlier limits must sum to >= 0 at the same moment. For a
    long the second condition is exactly `price >= mean(earlier fills)`.
    """
    last = filled[-1].price
    earlier = [f.price for f in filled[:-1]]
    if direction == "long":
        level = last + thr_price
        if earlier:
            level = max(level, float(np.mean(earlier)))
        return level
    level = last - thr_price
    if earlier:
        level = min(level, float(np.mean(earlier)))
    return level


def _trail_exit(series: np.ndarray, direction: str, start: int, anchor: float,
                gap: float, ratchet=None) -> int:
    """First index at or after `start` where a ratcheting trail from `anchor` is hit.

    The stop at tick t is `max(anchor, high water mark) - gap` for a long, so it
    never loosens, and the trigger is tick-exact because the stop rests at the
    broker. `ratchet` is the polled view the bot updates that stop from: with it,
    the high water mark only advances once per poll, which is the real lag. Left
    None the mark advances every tick, which quietly assumes the bot tightened
    the stop at the exact top of every spike.
    """
    seg = series[start:]
    if not len(seg):
        return -1

    if ratchet is None:
        hwm = np.maximum.accumulate(seg) if direction == "long" else np.minimum.accumulate(seg)
    else:
        j = int(np.searchsorted(ratchet.idx, start, side="left"))
        pidx, pval = ratchet.idx[j:], ratchet.val[j:]
        if not len(pidx):
            hwm = np.full(len(seg), anchor)
        else:
            run = (np.maximum.accumulate(pval) if direction == "long"
                   else np.minimum.accumulate(pval))
            # Each tick carries the mark from the most recent poll at or before it;
            # ticks before the first poll carry the anchor.
            slot = np.searchsorted(pidx, np.arange(start, start + len(seg)), side="right") - 1
            hwm = np.where(slot >= 0, run[np.clip(slot, 0, len(run) - 1)], anchor)

    if direction == "long":
        stop = np.maximum(hwm, anchor) - gap
        hit = np.flatnonzero(seg <= stop)
    else:
        stop = np.minimum(hwm, anchor) + gap
        hit = np.flatnonzero(seg >= stop)
    return start + int(hit[0]) if len(hit) else -1


def _bar_crossings(win: TickWindow, series: str, direction: str, anchor: float,
                   thr_price: float, start: int, stop: int) -> tuple:
    """First favourable and adverse crossing of each EXCURSION_BARS fraction.

    Anchored on the position's breakeven (the mean fill), which is the level a
    break-even rule would actually be managed against.
    """
    src = win.bid if series == "bid" else win.ask
    seg = src[start:stop + 1]
    fav, adv = {}, {}
    if not len(seg) or thr_price <= 0:
        return fav, adv
    run_max = np.maximum.accumulate(seg)
    run_min = np.minimum.accumulate(seg)
    for frac in EXCURSION_BARS:
        d = frac * thr_price
        up, down = anchor + d, anchor - d
        if direction == "long":
            i_fav = int(np.searchsorted(run_max, up, side="left"))
            i_adv = int(np.searchsorted(-run_min, -down, side="left"))
        else:
            i_fav = int(np.searchsorted(-run_min, -down, side="left"))
            i_adv = int(np.searchsorted(run_max, up, side="left"))
        if i_fav < len(seg):
            fav[frac] = win.at(start + i_fav)[0]
        if i_adv < len(seg):
            adv[frac] = win.at(start + i_adv)[0]
    return fav, adv


def build_window(signal: dict, store: TickStore, policy: Policy):
    """The tick window a replay of `signal` under `policy` needs.

    Split out from `simulate` so a policy sweep loads and slices each signal's
    ticks once instead of once per policy.
    """
    start = signal["created_at"]
    end = start + timedelta(hours=policy.horizon_hours)
    expiry = signal.get("expiry_time")
    if policy.honor_expiry and expiry is not None and not pd.isna(expiry):
        end = min(end, expiry)
    if end <= start:
        end = start + timedelta(minutes=1)
    return store.window(signal["instrument"], start, end)


def simulate(signal: dict, limits: list, store: TickStore, policy: Policy,
             basis: float = 0.0, win: Optional[TickWindow] = None) -> Outcome:
    """Replay one signal against archived ticks under `policy`.

    `basis` is the ICMarkets price minus the price the bot's own feed showed, from
    backtest.calibrate. Signal levels are quoted in feed space, so they are shifted
    into IC space to be compared against archived ticks; P&L is unaffected because
    entries and exits then share one frame. Reported prices are shifted back.

    `win` may be supplied by a caller replaying the same signal under many
    policies; it must have been built for a policy with the same horizon.
    """
    sid = signal["id"]
    sym = signal["instrument"]
    direction = signal["direction"].lower()
    out = Outcome(signal_id=sid, instrument=sym, direction=direction)

    limits = sorted(limits, key=lambda r: r["sequence_number"])
    if policy.skip_if_limits_over and len(limits) > policy.skip_if_limits_over:
        out.status = "skipped"
        return out
    if not limits:
        return out

    # 1R is the full-fill risk over every limit, matching DATA_ANALYSIS.md §3, so
    # that policies which fill to different depths stay comparable on one base.
    levels = [float(r["price_level"]) + basis for r in limits]
    sl = float(signal["stop_loss"]) + basis
    if policy.sl_multiplier != 1.0:
        sl = levels[0] - (levels[0] - sl) * policy.sl_multiplier
    out.sl_price = sl - basis
    out.risk_price = float(sum(abs(lv - sl) for lv in levels))

    if win is None:
        win = build_window(signal, store, policy)
    if win is None or len(win) < 2:
        return out
    out.n_ticks = len(win)

    # Entry and SL read the ask for a long, the bid for a short; the spread buffer
    # flips the entry (only) to the other side of the book.
    entry_series = "ask" if direction == "long" else "bid"
    broker = policy.entry_model == "broker"
    # A broker-model order rests a spread inside the level, so the far side of the
    # book is what reaches it — the same trigger the TM bot's spread buffer uses.
    fill_series = ("bid" if direction == "long" else "ask") \
        if (policy.spread_buffer or broker) else entry_series
    exit_series = "bid" if policy.exit_side == "live" or direction == "long" else "ask"
    exit_src = win.bid if exit_series == "bid" else win.ask
    # The side the position actually transacts on when it opens.
    near_src = win.ask if direction == "long" else win.bid
    touch = win.first_at_or_below if direction == "long" else win.first_at_or_above

    if policy.require_placeable:
        # Judged at the moment the signal arrived, which is when the orders would
        # have gone out. A limit already past the market is one the provider
        # treats as hit and would retract anyway.
        b0, a0 = float(win.bid[0]), float(win.ask[0])
        shift = (a0 - b0) if broker else 0.0
        keep = []
        for i, lv in enumerate(levels):
            rest = lv + shift if direction == "long" else lv - shift
            on_side = rest < a0 if direction == "long" else rest > b0
            stop_ok = sl < rest if direction == "long" else sl > rest
            if on_side and stop_ok:
                keep.append(i)
        if not keep:
            out.status = "unplaceable"
            return out
        limits = [limits[i] for i in keep]
        levels = [levels[i] for i in keep]
        out.risk_price = float(sum(abs(lv - sl) for lv in levels))

    fill_idx = [touch(fill_series, lv) for lv in levels]

    if broker:
        # Each limit is placed as its own order carrying its own stop, shifted a
        # spread further out at placement. Measured at the first fill, which is
        # the only moment the spread is observable and the position exists.
        touched = [i for i in fill_idx if i >= 0]
        if touched:
            s0 = float(win.ask[min(touched)] - win.bid[min(touched)])
            sl = sl - s0 if direction == "long" else sl + s0
            out.sl_price = sl - basis
    # The stop rests at the broker, which closes a long by selling at the bid and
    # a short by buying at the ask — so those are the sides it triggers on. The
    # bot's own convention is the opposite side, and reproducing it is what the
    # "live" exit model is for; using it for a real account would report every
    # stop a full spread late.
    sl_series = entry_series if policy.exit_side == "live" else exit_series
    sl_idx = (win.first_at_or_below(sl_series, sl) if direction == "long"
              else win.first_at_or_above(sl_series, sl))

    # Instant-entry signals carry their own TP price and the threshold never
    # applies to them (DATA_ANALYSIS.md §4). The value arrives from pandas as
    # NaN rather than None, so it needs an isna check, not an identity test.
    own_tp = signal.get("take_profit")
    fixed_tp = None if own_tp is None or pd.isna(own_tp) else float(own_tp) + basis

    thr = float(signal["tp_threshold"]) * policy.tp_multiplier
    thr_price = thr * pip_size(sym) if is_pips_instrument(sym) else thr
    out.thr_price = thr_price

    events = sorted([(i, k) for k, i in enumerate(fill_idx) if i >= 0])
    if policy.max_fill_depth:
        events = events[: policy.max_fill_depth]

    filled: list = []
    cursor = 0
    exit_idx = None

    def close(idx: int, price: float, reason: str, status: str):
        nonlocal exit_idx
        out.status = status
        out.exit_reason = reason
        out.exit_price = price
        out.exit_time = win.at(idx)[0]
        exit_idx = idx

    def _signed(price: float) -> float:
        """Per-unit P&L of the open position at `price`, signed by direction.

        Used only to label an exit a win or a loss when the rule that produced it
        is neither a target nor a stop.
        """
        mean_fill = float(np.mean([f.price for f in filled]))
        return (price - mean_fill) if direction == "long" else (mean_fill - price)

    def _poll():
        """The polled view of the exit series, built only once something fills.

        Most signals never fill a limit, and building a per-second grid across a
        week-long window for each of those costs more than the entire replay.
        """
        return win.polled(policy.exit_poll_seconds, exit_series)

    def _above(lvl: float, start: int) -> int:
        return (_poll().first_at_or_above(lvl, start) if policy.exit_poll_seconds
                else win.first_at_or_above(exit_series, lvl, start))

    def _below(lvl: float, start: int) -> int:
        return (_poll().first_at_or_below(lvl, start) if policy.exit_poll_seconds
                else win.first_at_or_below(exit_series, lvl, start))

    def tp_touch(start: int) -> int:
        """First index at or after `start` where the fixed-TP condition holds."""
        lvl = fixed_tp if fixed_tp is not None else _tp_level(direction, filled, thr_price)
        return _above(lvl, start) if direction == "long" else _below(lvl, start)

    def be_touch(start: int) -> int:
        """First index at or after `start` where an armed breakeven stop fires.

        Arms only once price has run `breakeven_at` x the threshold in favour,
        mirroring the live rule that refuses to arm a stop the next tick would hit.
        """
        mean_fill = float(np.mean([f.price for f in filled]))
        trigger = (mean_fill + policy.breakeven_at * thr_price if direction == "long"
                   else mean_fill - policy.breakeven_at * thr_price)
        armed = _above(trigger, start) if direction == "long" else _below(trigger, start)
        if armed < 0:
            return -1
        return _below(mean_fill, armed) if direction == "long" else _above(mean_fill, armed)

    def resolve(start: int, limit_idx: int) -> bool:
        """Evaluate SL / TP / BE for an open position over [start, limit_idx).

        `limit_idx` bounds the search to the tick before the next fill; -1 means
        run to the end of the window. Returns True when the position closed.
        """
        stop = sl_idx if sl_idx >= start else -1
        if limit_idx >= 0 and stop >= limit_idx:
            stop = -1

        cand = []
        if policy.tp_mode in ("fixed", "trailing"):
            t = tp_touch(start)
            if t >= 0 and (limit_idx < 0 or t < limit_idx):
                cand.append((t, "tp"))
        if policy.breakeven_at is not None:
            b = be_touch(start)
            if b >= 0 and (limit_idx < 0 or b < limit_idx):
                cand.append((b, "be"))
        if policy.time_stop_minutes is not None:
            deadline = filled[0].time + timedelta(minutes=policy.time_stop_minutes)
            ts = int(np.searchsorted(win.time_ms, int(deadline.timestamp() * 1000)))
            if ts < len(win) and ts >= start and (limit_idx < 0 or ts < limit_idx):
                cand.append((ts, "time"))

        # The stop loss wins a tie, mirroring the live order: a tick that gaps
        # through both books the loss the position actually took.
        if stop >= 0 and all(stop <= i for i, _ in cand):
            close(stop, sl, "stop_loss", "stopped")
            return True
        if not cand:
            return False

        idx, kind = min(cand)
        if kind == "be":
            close(idx, float(np.mean([f.price for f in filled])), "breakeven", "tp")
            return True
        if kind == "time":
            close(idx, float(exit_src[idx]), "time_stop",
                  "tp" if _signed(exit_src[idx]) > 0 else "stopped")
            return True
        if policy.tp_mode == "fixed":
            close(idx, float(exit_src[idx]), "auto_tp", "tp")
            return True

        # Trailing: the fixed TP is the arming point, not the exit. From there the
        # stop ratchets and the position runs until the trail or the real SL.
        gap = (abs(fixed_tp - filled[-1].price) if fixed_tp is not None
               else thr_price) * policy.trail_distance
        ratchet = (win.polled(policy.trail_poll_seconds, exit_series)
                   if policy.trail_poll_seconds else None)
        taken = float(exit_src[idx])
        tidx = _trail_exit(exit_src, direction, idx, taken, gap, ratchet)
        stop_after = sl_idx if sl_idx >= idx else -1
        if stop_after >= 0 and (tidx < 0 or stop_after <= tidx):
            rest_idx, rest_px, reason, status = stop_after, sl, "partial_stop", "stopped"
        elif tidx >= 0:
            rest_idx, rest_px, reason, status = tidx, float(exit_src[tidx]), "trail", "tp"
        else:
            rest_idx = len(win) - 1
            rest_px, reason, status = float(exit_src[rest_idx]), "horizon", "expired"

        # A part taken at the arming price and the rest run on is, for P&L, one
        # position closed at the blended price — which keeps every downstream
        # measure (R, excursion, attribution) working unchanged.
        p = policy.partial_close
        if p > 0:
            blended = p * taken + (1 - p) * rest_px
            close(rest_idx, blended, f"partial{p:g}_{reason}",
                  "tp" if _signed(blended) > 0 else status)
        else:
            close(rest_idx, rest_px, reason, status)
        return True

    for fidx, k in events:
        # Between the cursor and this fill, an open position can close.
        if filled and resolve(cursor, fidx):
            break

        # Same-tick ordering: the fill lands before the stop is evaluated.
        t, _, _ = win.at(fidx)
        price = float(near_src[fidx]) if broker else levels[k]
        # A gap through the level can fill beyond the stop itself, leaving a
        # position whose stop sits on its profitable side. The broker rejects
        # that order rather than opening it; replayed, it "stops out" for a large
        # gain that never existed.
        if policy.require_placeable and (
                price <= sl if direction == "long" else price >= sl):
            continue
        filled.append(Fill(seq=limits[k]["sequence_number"],
                           price=price, idx=fidx, time=t))
        if out.entry_time is None:
            out.entry_time = t
        cursor = fidx
    else:
        # All fills processed without an exit — carry the position to the horizon.
        if filled and not resolve(cursor, -1):
            last = len(win) - 1
            close(last, float(exit_src[last]), "horizon", "expired")

    out.fills = filled
    if not filled:
        out.status = "no_fill" if out.status == "no_data" else out.status
        return out

    # Excursion is bounded by the exit: a trade that ran on after its TP did not
    # hand that move to the position, and counting it inflates every MFE-based
    # argument for holding longer. The follow-through is reported separately.
    e_idx = filled[0].idx
    seg = exit_src[e_idx:exit_idx + 1]
    if direction == "long":
        hi, lo = int(seg.argmax()), int(seg.argmin())
        out.mfe_price, out.mae_price = float(seg[hi]), float(seg[lo])
    else:
        lo, hi = int(seg.argmin()), int(seg.argmax())
        out.mfe_price, out.mae_price = float(seg[lo]), float(seg[hi])
    out.mfe_time = win.at(e_idx + (hi if direction == "long" else lo))[0]
    out.mae_time = win.at(e_idx + (lo if direction == "long" else hi))[0]

    tail = exit_src[exit_idx:]
    if len(tail) > 1:
        out.post_exit_mfe_price = float(tail.max() if direction == "long" else tail.min())

    mean_fill = float(np.mean([f.price for f in filled]))
    out.fav_bar_times, out.adv_bar_times = _bar_crossings(
        win, exit_series, direction, mean_fill, thr_price, e_idx, exit_idx)

    sgn = 1 if direction == "long" else -1
    out.pnl_price = float(sum(sgn * (out.exit_price - f.price) for f in filled))
    out.risk_taken = float(sum(abs(f.price - sl) for f in filled))
    out.rho = out.pnl_price / out.risk_price if out.risk_price else None

    # Back to feed space so exits compare against recorded tp_price.
    if basis:
        out.exit_price -= basis
        out.mfe_price -= basis
        out.mae_price -= basis
        if out.post_exit_mfe_price is not None:
            out.post_exit_mfe_price -= basis
        for f in out.fills:
            f.price -= basis
    return out
