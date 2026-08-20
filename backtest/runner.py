"""Replay many policies over the signal set, loading each signal's ticks once.

A policy sweep asks the same signal the same question a few dozen times. Slicing
its ticks once per policy dominates the runtime, so the window is built once and
shared. Signals are walked in (symbol, time) order to keep the week cache warm —
interleaving symbols evicts it on every step.

Policies in one sweep must agree on `horizon_hours` and `honor_expiry`, since
those decide the window itself rather than what happens inside it.
"""

import time
from datetime import timedelta

import pandas as pd

from backtest.basis import BasisModel, is_measurable
from backtest.engine import Policy, build_window, is_pips_instrument, pip_size, simulate
from backtest.ticks import TickStore


def _apply_cancel(signal: dict, honor_cancel: bool) -> dict:
    """Cap the replay at the moment the bot pulled the order, if asked."""
    if not honor_cancel:
        return signal
    cancel = signal.get("cancel_at")
    if cancel is None or pd.isna(cancel):
        return signal
    s = dict(signal)
    expiry = s.get("expiry_time")
    s["expiry_time"] = cancel if expiry is None or pd.isna(expiry) else min(expiry, cancel)
    return s


def thr_price_of(signal: dict) -> float:
    sym = signal["instrument"]
    thr = float(signal["tp_threshold"])
    return thr * pip_size(sym) if is_pips_instrument(sym) else thr


def run(signals: list, limits: dict, policies: dict, store: TickStore = None,
        basis_model: BasisModel = None, honor_cancel: bool = False,
        progress_every: int = 400) -> dict:
    """Replay `signals` under every policy in `policies`.

    Returns {label: [Outcome]}, each list in the same signal order. Signals whose
    broker basis is too uncertain to decide an exit are still replayed; the
    `measurable` flag from `describe` is what filters them, so the cost of
    dropping them stays visible.
    """
    store = store or TickStore()
    basis_model = basis_model or BasisModel.load()

    shape = next(iter(policies.values()))
    for p in policies.values():
        if (p.horizon_hours, p.honor_expiry) != (shape.horizon_hours, shape.honor_expiry):
            raise ValueError("all policies in one sweep must share the replay window")

    ordered = sorted(signals, key=lambda s: (s["instrument"], s["created_at"]))
    results = {label: [] for label in policies}
    t0 = time.time()

    for n, sig in enumerate(ordered, 1):
        sig = _apply_cancel(sig, honor_cancel)
        b, _, _ = basis_model.basis_for(sig["instrument"], sig["created_at"], sig["id"])
        win = build_window(sig, store, shape)
        rows = limits[sig["id"]]
        for label, pol in policies.items():
            results[label].append(simulate(sig, rows, store, pol, basis=b, win=win))
        if progress_every and n % progress_every == 0:
            print(f"  {n:,}/{len(ordered):,} signals  {time.time() - t0:6.1f}s", flush=True)

    print(f"  replayed {len(ordered):,} signals x {len(policies)} policies "
          f"in {time.time() - t0:.1f}s", flush=True)
    return {"_signals": ordered, **results}


def describe(signals: list, outcomes: list, basis_model: BasisModel) -> pd.DataFrame:
    """Flatten outcomes into one analysis row per signal."""
    rows = []
    for sig, o in zip(signals, outcomes):
        thr_price = thr_price_of(sig)
        b, unc, nb = basis_model.basis_for(sig["instrument"], sig["created_at"], sig["id"])
        sgn = 1 if o.direction == "long" else -1
        fills = [f.price for f in o.fills]
        mean_fill = sum(fills) / len(fills) if fills else None
        deepest = (min(fills) if o.direction == "long" else max(fills)) if fills else None

        row = dict(
            signal_id=sig["id"], instrument=sig["instrument"], type=sig["type"],
            direction=o.direction, created_at=sig["created_at"],
            expiry_type=sig["expiry_type"], data_version=sig["data_version"],
            threshold_stamped=sig["threshold_stamped"], tp_threshold=sig["tp_threshold"],
            thr_price=thr_price, minutes_to_news=sig["minutes_to_news"],
            real_status=sig["status"], real_closed_reason=sig["closed_reason"],
            real_limits_hit=sig["limits_hit"], total_limits=sig["total_limits"],
            basis=b, basis_uncertainty=unc, basis_obs=nb,
            measurable=is_measurable(unc, thr_price) if unc else True,
            status=o.status, exit_reason=o.exit_reason, n_fills=o.n_fills,
            entry_time=o.entry_time, exit_time=o.exit_time, exit_price=o.exit_price,
            sl_price=o.sl_price, first_fill=fills[0] if fills else None,
            deepest_fill=deepest, mean_fill=mean_fill,
            pnl_price=o.pnl_price, risk_price=o.risk_price, risk_taken=o.risk_taken,
            rho=o.rho, n_ticks=o.n_ticks,
        )
        if o.entry_time is not None and o.exit_time is not None:
            row["minutes_in_trade"] = (o.exit_time - o.entry_time).total_seconds() / 60
        if fills and o.mfe_price is not None:
            # Excursion in units of the TP threshold, anchored on the mean fill —
            # the level the position's own P&L is measured against.
            row["mfe_thr"] = sgn * (o.mfe_price - mean_fill) / thr_price
            row["mae_thr"] = sgn * (o.mae_price - mean_fill) / thr_price
            row["mfe_time"] = o.mfe_time
            row["mae_time"] = o.mae_time
            row["mae_first"] = (o.mae_time < o.mfe_time) if o.mae_time and o.mfe_time else None
            if o.post_exit_mfe_price is not None and o.exit_price is not None:
                row["post_exit_thr"] = sgn * (o.post_exit_mfe_price - o.exit_price) / thr_price
        for frac in (0.25, 0.5, 1.0):
            row[f"fav_{frac}"] = o.fav_bar_times.get(frac)
            row[f"adv_{frac}"] = o.adv_bar_times.get(frac)
        rows.append(row)
    return pd.DataFrame(rows)


def fill_rows(signals: list, outcomes: list) -> pd.DataFrame:
    """One row per simulated fill — the basis for any per-limit sizing question."""
    rows = []
    for sig, o in zip(signals, outcomes):
        if not o.fills or o.exit_price is None:
            continue
        sgn = 1 if o.direction == "long" else -1
        for f in o.fills:
            rows.append(dict(
                signal_id=sig["id"], instrument=sig["instrument"], type=sig["type"],
                direction=o.direction, created_at=sig["created_at"],
                seq=f.seq, depth=len(o.fills), fill_price=f.price, fill_time=f.time,
                exit_price=o.exit_price, sl_price=o.sl_price,
                pnl_price=sgn * (o.exit_price - f.price),
                risk_price=abs(f.price - o.sl_price),
                signal_risk=o.risk_price, status=o.status, exit_reason=o.exit_reason,
            ))
    return pd.DataFrame(rows)
