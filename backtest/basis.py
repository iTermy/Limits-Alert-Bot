"""Time-local broker basis between the bot's feed and the ICMarkets archive.

Indices, crypto and Exness oil were priced off feeds other than ICMarkets, so
their signal levels sit in a different price frame from the ticks they are
replayed against. `backtest.calibrate` measures one constant per symbol, which is
correct only if the offset holds still — and it does not. Measured monthly, JP225
swings 207 points and BTCUSDT 119, both far wider than those symbols' own TP
thresholds, so a constant basis would decide their exits by itself.

The basis is therefore estimated per signal from recorded fills near it in time.
Two rules keep that honest:

  * **Leave-one-out.** A signal's own fills are excluded from its estimate,
    otherwise the replay reproduces the fill it was calibrated on and validation
    scores its own answer.
  * **A quality figure travels with it.** `basis_for` returns the spread of the
    observations it used; when that spread is large relative to the TP threshold
    the signal is not measurable at all, and saying so is more useful than
    quoting a P&L computed off a guess.
"""

import os
from datetime import timedelta

import numpy as np
import pandas as pd

from backtest.symbols import NON_NATIVE

SIG_DIR = r"C:\Python Stuff\TM-Backtest-Data\signals"

# Half-width of the window of nearby fills used for one estimate. Wide enough to
# hold several observations on a thin symbol, short enough that a drifting basis
# is tracked rather than averaged away.
LOCAL_WINDOW = pd.Timedelta(days=10)
MIN_LOCAL_OBS = 3

# A signal is unmeasurable when the local basis is this uncertain relative to the
# threshold that decides its exit.
QUALITY_LIMIT = 0.5


class BasisModel:
    """Per-symbol basis observations, queried by timestamp."""

    def __init__(self, obs: dict):
        self._obs = obs      # symbol -> (times ns int64, values, signal_ids)

    @classmethod
    def build(cls, signals: pd.DataFrame, limits: pd.DataFrame, store) -> "BasisModel":
        """Measure `IC price - recorded fill price` at every recorded fill."""
        from backtest.calibrate import WRITE_LATENCY_S, price_at

        hits = limits[(limits.status == "hit") & limits.hit_time.notna()
                      & limits.hit_price.notna()]
        meta = signals.set_index("id")[["instrument", "direction"]]
        hits = hits.join(meta, on="signal_id", how="inner")

        obs = {}
        for sym, grp in hits.groupby("instrument"):
            times, vals, sids = [], [], []
            for r in grp.itertuples(index=False):
                px = price_at(store, sym, r.hit_time + timedelta(seconds=WRITE_LATENCY_S), 0)
                if not px:
                    continue
                # The bot records the ask for a long and the bid for a short, so
                # the residual on that side is basis rather than a half-spread.
                side = px[1] if r.direction == "long" else px[0]
                times.append(pd.Timestamp(r.hit_time).value)
                vals.append(side - r.hit_price)
                sids.append(r.signal_id)
            if not times:
                continue
            order = np.argsort(times)
            obs[sym] = (np.asarray(times)[order], np.asarray(vals, float)[order],
                        np.asarray(sids)[order])
        return cls(obs)

    def save(self, path: str = None) -> None:
        path = path or os.path.join(SIG_DIR, "basis_obs.pkl")
        pd.to_pickle(self._obs, path)

    @classmethod
    def load(cls, path: str = None) -> "BasisModel":
        path = path or os.path.join(SIG_DIR, "basis_obs.pkl")
        return cls(pd.read_pickle(path))

    def basis_for(self, symbol: str, when, exclude_signal_id=None) -> tuple:
        """(basis, uncertainty, n) for `symbol` at `when`, leaving out one signal.

        IC-native symbols return exactly zero: their levels are already in the
        archive's frame, and fitting a basis to them would only fit noise.
        """
        if symbol not in NON_NATIVE:
            return 0.0, 0.0, 0
        rec = self._obs.get(symbol)
        if rec is None:
            return 0.0, float("inf"), 0

        times, vals, sids = rec
        keep = sids != exclude_signal_id if exclude_signal_id is not None else slice(None)
        times, vals = times[keep], vals[keep]
        if not len(times):
            return 0.0, float("inf"), 0

        t = pd.Timestamp(when).value
        near = np.abs(times - t) <= LOCAL_WINDOW.value
        if near.sum() < MIN_LOCAL_OBS:
            # Too thin locally: fall back to the nearest observations in time,
            # which still tracks drift better than a global median does.
            order = np.argsort(np.abs(times - t))[:MIN_LOCAL_OBS]
            sel = vals[order]
        else:
            sel = vals[near]
        if not len(sel):
            return 0.0, float("inf"), 0
        spread = float(np.subtract(*np.percentile(sel, [75, 25]))) if len(sel) > 1 else float("inf")
        return float(np.median(sel)), spread, int(len(sel))


def is_measurable(uncertainty: float, thr_price: float) -> bool:
    """Whether a basis estimate is tight enough to decide this signal's exit."""
    if thr_price <= 0:
        return False
    return uncertainty <= QUALITY_LIMIT * thr_price
