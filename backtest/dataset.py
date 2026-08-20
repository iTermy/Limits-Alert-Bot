"""Assemble replayable signals from the local pull.

The TP threshold is the one thing that cannot be reconstructed after the fact:
`tp_threshold_used` was only stamped from 2026-07-14, so earlier signals fall back
to today's config, which is config-at-analysis rather than config-at-time. Those
signals are flagged so exit-tuning work can exclude them.
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.ticks import TickStore
from price_feeds.config.tp_config import TPConfig

SIG_DIR = r"C:\Python Stuff\TM-Backtest-Data\signals"

# tp_threshold_used is trustworthy only from this date (DATA_ANALYSIS.md §2).
CONFIG_AT_TIME_FROM = pd.Timestamp("2026-07-14", tz="UTC")


def load_frames() -> tuple:
    sig = pd.read_pickle(os.path.join(SIG_DIR, "signals.pkl"))
    lim = pd.read_pickle(os.path.join(SIG_DIR, "limits.pkl"))
    sc = pd.read_pickle(os.path.join(SIG_DIR, "status_changes.pkl"))
    return sig, lim, sc


def build(only_archived: bool = True) -> tuple:
    """Return (signals list of dicts, {signal_id: [limit dicts]})."""
    sig, lim, sc = load_frames()
    store = TickStore()
    have = store.available_symbols()

    sig["created_at"] = pd.to_datetime(sig["created_at"], utc=True)
    if only_archived:
        sig = sig[sig.instrument.isin(have)]

    # Retro cancelled -> profit corrections are excluded from TM-side
    # profitability per DATA_ANALYSIS.md §3.
    retro = set(sc[(sc.old_status == "cancelled") & (sc.new_status == "profit")].signal_id)
    sig = sig[~sig.id.isin(retro)]

    tp_config = TPConfig()
    records = []
    for r in sig.itertuples(index=False):
        thr = r.tp_threshold_used
        stamped = pd.notna(thr) and r.created_at >= CONFIG_AT_TIME_FROM
        if not stamped:
            thr = tp_config.get_tp_value(r.instrument, signal_type=r.type)
        records.append(dict(
            id=r.id, instrument=r.instrument, direction=r.direction,
            stop_loss=r.stop_loss, created_at=r.created_at, expiry_time=r.expiry_time,
            type=r.type, status=r.status, closed_reason=r.closed_reason,
            limits_hit=r.limits_hit, total_limits=r.total_limits,
            tp_price=r.tp_price, take_profit=r.take_profit, closed_at=r.closed_at,
            tp_threshold=float(thr), threshold_stamped=bool(stamped),
        ))

    limits_by_signal = {}
    for sid, grp in lim.groupby("signal_id"):
        limits_by_signal[sid] = grp.sort_values("sequence_number").to_dict("records")

    records = [r for r in records if r["id"] in limits_by_signal]
    return records, limits_by_signal
