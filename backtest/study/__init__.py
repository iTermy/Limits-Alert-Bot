"""Shared conventions for reading the replay output.

Every question below is answered on one universe and one risk convention, and
they are defined here rather than in each script so two analyses cannot quietly
disagree about what a trade or an R is.

**Universe.** `policy='real'`, at least one fill, and a broker basis tight enough
to decide the exit. Signals whose basis is too uncertain are counted and reported
but never averaged into a result.

**R.** 1R is the risk of the fully filled position: the sum of |limit - stop| over
*every* limit on the signal, not just the ones that filled. A full-fill stop-out
is therefore exactly -1R, a partial-fill stop-out is less, and policies that fill
to different depths stay comparable because the denominator does not move.
"""

import os

import numpy as np
import pandas as pd

SIG_DIR = r"C:\Python Stuff\TM-Backtest-Data\signals"

# Instrument groups used throughout. Broker basis makes some symbols unmeasurable
# regardless of group; that is handled per signal, not here.
def asset_class(symbol: str) -> str:
    s = symbol.upper()
    if ".NAS" in s or ".NYSE" in s:
        return "stock"
    if s.startswith(("XAU", "GC")):
        return "gold"
    if s.startswith("XAG"):
        return "silver"
    if s.startswith(("BTC", "ETH")) or "USDT" in s:
        return "crypto"
    if "OIL" in s or "XTI" in s:
        return "oil"
    if any(k in s for k in ("NAS100", "US30", "US500", "SPX", "USTEC", "JP225", "DE30",
                            "DE40", "UK100", "F40", "AUS2", "AU200", "HK50", "HKG")):
        return "index"
    return "forex"


# A stop further than this many TP thresholds from the entry is not a trade this
# strategy makes; it is a parse error. Fifteen entered signals over five months
# carry levels like gold at 154,524 or USDCHF at 1.79, which fill on the replay's
# first tick because the "limit" sits on the wrong side of the market — and which
# MT5 would reject outright, since a buy limit cannot rest above the bid.
MAX_STOP_IN_THRESHOLDS = 30.0


def corrupt_levels(df: pd.DataFrame) -> pd.Series:
    return ((df.mean_fill - df.sl_price).abs() / df.thr_price) > MAX_STOP_IN_THRESHOLDS


def load(policy: str = "real", entered_only: bool = True, measurable_only: bool = True,
         drop_instant: bool = True, drop_corrupt: bool = True) -> pd.DataFrame:
    """The analysis universe, with the standard filters applied."""
    df = pd.read_pickle(os.path.join(SIG_DIR, "master_signals.pkl"))
    df = df[df.policy == policy].copy()

    # Instant-entry signals enter at market against a price the sender fixed, so
    # they share neither the entry model nor the exit rule under test.
    sig = pd.read_pickle(os.path.join(SIG_DIR, "signals.pkl"))
    instant = set(sig.loc[sig.take_profit.notna(), "id"])
    df["instant"] = df.signal_id.isin(instant)
    if drop_instant:
        df = df[~df.instant]

    if entered_only:
        df = df[df.n_fills > 0]
    if measurable_only:
        df = df[df.measurable]
    if drop_corrupt:
        df = df[~corrupt_levels(df).fillna(False)]

    df["asset"] = df.instrument.map(asset_class)
    df["created_at"] = pd.to_datetime(df.created_at, utc=True)
    et = df.created_at.dt.tz_convert("America/New_York")
    df["hour_et"] = et.dt.hour
    df["dow"] = et.dt.dayofweek
    df["month"] = df.created_at.dt.to_period("M").astype(str)
    return df.sort_values("created_at").reset_index(drop=True)


def load_fills() -> pd.DataFrame:
    return pd.read_pickle(os.path.join(SIG_DIR, "master_fills.pkl"))


def summarize(rho: pd.Series) -> dict:
    """Headline statistics for a set of per-signal R outcomes."""
    r = pd.Series(rho).dropna()
    if not len(r):
        return dict(n=0)
    wins, losses = r[r > 0], r[r < 0]
    return dict(
        n=len(r), total_R=r.sum(), mean_R=r.mean(), median_R=r.median(),
        win_rate=(r > 0).mean(),
        avg_win=wins.mean() if len(wins) else np.nan,
        avg_loss=losses.mean() if len(losses) else np.nan,
        std=r.std(),
        sharpe=r.mean() / r.std() if r.std() else np.nan,
    )


def boot_ci(rho: pd.Series, n_boot: int = 4000, alpha: float = 0.05, seed: int = 0) -> tuple:
    """Bootstrap CI for the mean. The return distribution is far from normal —
    a high win rate with rare full-size losses — so a t-interval understates the
    tails that decide whether the edge is real."""
    r = pd.Series(rho).dropna().to_numpy()
    if len(r) < 5:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    means = rng.choice(r, size=(n_boot, len(r)), replace=True).mean(axis=1)
    return tuple(np.quantile(means, [alpha / 2, 1 - alpha / 2]))


def table(df: pd.DataFrame, by, min_n: int = 1, sort: str = "total_R") -> pd.DataFrame:
    """Per-group summary with a bootstrap interval on the mean."""
    rows = []
    for key, grp in df.groupby(by):
        s = summarize(grp.rho)
        if s["n"] < min_n:
            continue
        lo, hi = boot_ci(grp.rho)
        s[by if isinstance(by, str) else "group"] = key
        s["ci_lo"], s["ci_hi"] = lo, hi
        rows.append(s)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).set_index(by if isinstance(by, str) else "group")
    cols = ["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate", "avg_win", "avg_loss", "sharpe"]
    return out[cols].sort_values(sort, ascending=False).round(3)
