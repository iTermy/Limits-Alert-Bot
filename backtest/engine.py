"""Tick-level replay of a signal's life, mirroring the live tick path.

The live check order (CLAUDE.md "Data Flow: Price Tick") is reproduced exactly:
limits fill first, then the stop loss, then auto-TP. Ties inside one tick resolve
in that order, which is what lets a tick that gaps through a limit and the stop
book both the fill and the loss.

Sides of the book, mirroring streaming_monitor and tp_monitor:
  entry/SL evaluated on the ask for a long, the bid for a short
  auto-TP evaluated on the BID in both directions

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

    spread_buffer: bool = True          # live setting; False = realistic limit fill
    tp_multiplier: float = 1.0          # scale the signal's own TP threshold
    tp_mode: str = "fixed"              # fixed | trailing | none
    trail_distance: float = 1.0         # trailing gap, in units of the TP threshold
    sl_multiplier: float = 1.0          # scale |limit1 - stop_loss|
    max_fill_depth: Optional[int] = None    # ignore fills deeper than this
    skip_if_limits_over: Optional[int] = None   # drop the signal entirely
    # Generous by default: a week-end expiry signal can fill days after it was
    # posted, and a short horizon silently truncates those into false no-fills.
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
    mfe_price: Optional[float] = None    # best favourable excursion from deepest fill
    mae_price: Optional[float] = None
    pnl_price: float = 0.0               # summed over fills, signed
    risk_price: float = 0.0              # 1R = full-fill risk
    rho: Optional[float] = None
    n_ticks: int = 0

    @property
    def n_fills(self) -> int:
        return len(self.fills)


def _tp_level(direction: str, filled: list, thr_price: float) -> float:
    """Price the BID must reach for auto-TP, given the currently filled limits.

    Mirrors tp_monitor: the last filled limit by sequence_number must clear the
    threshold, and any earlier limits must sum to >= 0 at the same moment.
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


def _trail_exit(win: TickWindow, direction: str, start_idx: int, anchor: float,
                gap: float, sl_level: float, sl_idx: int) -> tuple:
    """Walk a trailing stop from `start_idx`. Returns (idx, price, reason).

    The stop ratchets on the bid and never loosens. Scanning in blocks keeps this
    off a per-tick Python loop while preserving exact ratchet semantics.
    """
    bid = win.bid
    n = len(bid)
    stop = anchor - gap if direction == "long" else anchor + gap
    i = start_idx
    BLOCK = 4096
    while i < n:
        end = min(i + BLOCK, n)
        chunk = bid[i:end]
        if direction == "long":
            hit = np.flatnonzero(chunk <= stop)
            if len(hit):
                j = i + int(hit[0])
                return j, float(bid[j]), "trail"
            new_stop = float(np.max(chunk)) - gap
            stop = max(stop, new_stop)
        else:
            hit = np.flatnonzero(chunk >= stop)
            if len(hit):
                j = i + int(hit[0])
                return j, float(bid[j]), "trail"
            new_stop = float(np.min(chunk)) + gap
            stop = min(stop, new_stop)
        i = end
    return -1, None, ""


def simulate(signal: dict, limits: list, store: TickStore, policy: Policy,
             basis: float = 0.0) -> Outcome:
    """Replay one signal against archived ticks under `policy`.

    `basis` is the ICMarkets price minus the price the bot's own feed showed, from
    backtest.calibrate. Signal levels are quoted in feed space, so they are shifted
    into IC space to be compared against archived ticks; P&L is unaffected because
    entries and exits then share one frame. Reported exit prices are shifted back.
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

    # 1R is the full-fill risk over every limit, matching DATA_ANALYSIS.md §3.
    levels = [float(r["price_level"]) + basis for r in limits]
    sl = float(signal["stop_loss"]) + basis
    if policy.sl_multiplier != 1.0:
        sl = levels[0] - (levels[0] - sl) * policy.sl_multiplier
    out.risk_price = float(sum(abs(lv - sl) for lv in levels))

    start = signal["created_at"]
    end = start + timedelta(hours=policy.horizon_hours)
    expiry = signal.get("expiry_time")
    if policy.honor_expiry and expiry is not None and not pd.isna(expiry):
        end = min(end, expiry)
    if end <= start:
        end = start + timedelta(minutes=1)

    win = store.window(sym, start, end)
    if win is None or len(win) < 2:
        return out
    out.n_ticks = len(win)

    # Entry and SL read the ask for a long, the bid for a short; the spread buffer
    # flips the entry (only) to the other side of the book.
    entry_series = "ask" if direction == "long" else "bid"
    fill_series = ("bid" if direction == "long" else "ask") if policy.spread_buffer else entry_series
    touch = win.first_at_or_below if direction == "long" else win.first_at_or_above

    fill_idx = [touch(fill_series, lv) for lv in levels]
    # A long stops when price falls to the SL; a short when it rises to it.
    sl_idx = (win.first_at_or_below(entry_series, sl) if direction == "long"
              else win.first_at_or_above(entry_series, sl))

    # Instant-entry signals carry their own TP price and the threshold never
    # applies to them (DATA_ANALYSIS.md §4). The value arrives from pandas as
    # NaN rather than None, so it needs an isna check, not an identity test.
    own_tp = signal.get("take_profit")
    fixed_tp = None if own_tp is None or pd.isna(own_tp) else float(own_tp) + basis

    thr = float(signal["tp_threshold"]) * policy.tp_multiplier
    thr_price = thr * pip_size(sym) if is_pips_instrument(sym) else thr

    events = sorted([(i, k) for k, i in enumerate(fill_idx) if i >= 0])
    if policy.max_fill_depth:
        events = events[: policy.max_fill_depth]

    filled: list = []
    cursor = 0

    def close(idx: int, price: float, reason: str, status: str):
        out.status = status
        out.exit_reason = reason
        out.exit_price = price
        out.exit_time = win.at(idx)[0]

    for fidx, k in events:
        # Between the cursor and this fill, an open position can stop out or hit TP.
        if filled:
            stop_before = sl_idx if 0 <= sl_idx < fidx and sl_idx >= cursor else -1
            tp_before = -1
            if policy.tp_mode == "fixed":
                lvl = fixed_tp if fixed_tp is not None else _tp_level(direction, filled, thr_price)
                tp_before = (win.first_at_or_above("bid", lvl, cursor) if direction == "long"
                             else win.first_at_or_below("bid", lvl, cursor))
                if not (0 <= tp_before < fidx):
                    tp_before = -1
            if stop_before >= 0 and (tp_before < 0 or stop_before <= tp_before):
                close(stop_before, sl, "stop_loss", "stopped")
                break
            if tp_before >= 0:
                close(tp_before, float(win.bid[tp_before]), "auto_tp", "tp")
                break

        # Same-tick ordering: the fill lands before the stop is evaluated.
        t, b, a = win.at(fidx)
        filled.append(Fill(seq=limits[k]["sequence_number"],
                           price=levels[k], idx=fidx, time=t))
        if out.entry_time is None:
            out.entry_time = t
        cursor = fidx
    else:
        # All fills processed without an exit — carry the position to the horizon.
        if filled:
            stop_after = sl_idx if sl_idx >= cursor else -1
            if policy.tp_mode == "trailing":
                anchor = filled[-1].price
                gap = (abs(fixed_tp - anchor) if fixed_tp is not None
                       else thr_price) * policy.trail_distance
                tidx, tpx, treason = _trail_exit(win, direction, cursor, anchor, gap, sl, sl_idx)
                if stop_after >= 0 and (tidx < 0 or stop_after <= tidx):
                    close(stop_after, sl, "stop_loss", "stopped")
                elif tidx >= 0:
                    close(tidx, tpx, treason, "tp")
            elif policy.tp_mode == "fixed":
                lvl = fixed_tp if fixed_tp is not None else _tp_level(direction, filled, thr_price)
                tidx = (win.first_at_or_above("bid", lvl, cursor) if direction == "long"
                        else win.first_at_or_below("bid", lvl, cursor))
                if stop_after >= 0 and (tidx < 0 or stop_after <= tidx):
                    close(stop_after, sl, "stop_loss", "stopped")
                elif tidx >= 0:
                    close(tidx, float(win.bid[tidx]), "auto_tp", "tp")
            if out.exit_price is None:
                last = len(win) - 1
                close(last, float(win.bid[last]), "horizon", "expired")

    out.fills = filled
    if not filled:
        out.status = "no_fill" if out.status == "no_data" else out.status
        return out

    # Excursion from the deepest fill (the runner), per DATA_ANALYSIS.md §5.
    e_idx = filled[0].idx
    seg_bid = win.bid[e_idx:]
    if direction == "long":
        out.mfe_price = float(seg_bid.max())
        out.mae_price = float(seg_bid.min())
    else:
        out.mfe_price = float(seg_bid.min())
        out.mae_price = float(seg_bid.max())

    sgn = 1 if direction == "long" else -1
    out.pnl_price = float(sum(sgn * (out.exit_price - f.price) for f in filled))
    out.rho = out.pnl_price / out.risk_price if out.risk_price else None

    # Back to feed space so exits compare against recorded tp_price.
    if basis:
        out.exit_price -= basis
        out.mfe_price -= basis
        out.mae_price -= basis
    return out
