"""The shape of the return distribution, and what size it can actually carry.

Expectancy says whether to trade at all. It does not say how much, and for a
strategy that wins ~90% of the time in small increments and loses whole units
rarely, those are different questions with different answers: the arithmetic mean
is earned by an ensemble of many accounts, while a single account compounds the
time average, which turns negative well before the mean does.

Usage:  python -m backtest.study.risk
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backtest.study import SIG_DIR, boot_ci, load, summarize

pd.set_option("display.width", 200)


def section(t: str) -> None:
    print("\n" + "=" * 84)
    print(t)
    print("=" * 84)


def growth(f: float, r: np.ndarray) -> float:
    """Time-average log growth per trade at risk fraction `f` of equity per 1R."""
    x = 1.0 + f * r
    if (x <= 0).any():
        return -np.inf
    return float(np.mean(np.log(x)))


def max_drawdown(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    return float((equity / peak - 1.0).min())


def concurrency(df: pd.DataFrame) -> pd.Series:
    """Number of positions open simultaneously, sampled at every entry."""
    starts = df.entry_time.dropna().sort_values()
    ends = df.dropna(subset=["entry_time", "exit_time"]).set_index("entry_time").exit_time
    counts = []
    ent = df.dropna(subset=["entry_time", "exit_time"])
    a = ent.entry_time.to_numpy()
    b = ent.exit_time.to_numpy()
    for t in a:
        counts.append(int(((a <= t) & (b > t)).sum()))
    return pd.Series(counts, index=pd.DatetimeIndex(a))


def block_bootstrap(r: np.ndarray, n_paths: int, block: int, rng) -> np.ndarray:
    """Resample in blocks so any clustering of losses survives the resampling."""
    n = len(r)
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, max(n - block, 1), size=(n_paths, n_blocks))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]).reshape(n_paths, -1)
    return r[np.clip(idx[:, :n], 0, n - 1)]


def main() -> int:
    df = load("real")
    r = df.rho.dropna().to_numpy()
    section("1. THE RETURN DISTRIBUTION")
    s = summarize(pd.Series(r))
    print(f"  n {s['n']:,}   mean {s['mean_R']:+.4f}R   median {s['median_R']:+.4f}R   "
          f"std {s['std']:.3f}")
    print(f"  skew {pd.Series(r).skew():+.2f}   excess kurtosis {pd.Series(r).kurtosis():+.2f}")
    print("\n  quantiles of R:")
    q = pd.Series(r).quantile([.01, .05, .10, .25, .50, .75, .90, .95, .99]).round(3)
    print("   " + q.to_string().replace("\n", "\n   "))

    print("\n  histogram:")
    edges = [-np.inf, -1.0, -0.75, -0.5, -0.25, -0.05, 0.0, 0.05, 0.25, 0.5, np.inf]
    cut = pd.cut(r, edges)
    h = cut.value_counts().sort_index()
    for interval, count in h.items():
        bar = "#" * int(60 * count / h.max())
        print(f"   {str(interval):>16s} {count:>5d} {bar}")

    print("\n  concentration — how much of the result rides on how few trades:")
    srt = np.sort(r)[::-1]
    tot = r.sum()
    for k in (1, 5, 10, 25):
        share = srt[:max(1, int(len(r) * k / 100))].sum()
        print(f"    top {k:>2d}% of trades contribute {share:+8.1f}R "
              f"of {tot:+.1f}R total")
    worst = np.sort(r)[: max(1, int(len(r) * 0.05))].sum()
    print(f"    worst  5% of trades contribute {worst:+8.1f}R")

    section("2. ARE TRADES INDEPENDENT?")
    ser = pd.Series(r)
    print("  autocorrelation of R by lag:")
    print("   " + " ".join(f"lag{k}:{ser.autocorr(k):+.3f}" for k in range(1, 8)))
    losses = (ser < 0).astype(int)
    print(f"\n  loss rate {losses.mean():.1%}; "
          f"probability a loss follows a loss {losses[losses.shift(1) == 1].mean():.1%}")
    print("""
  If losses cluster, position sizing based on independent draws understates
  drawdown, and the block bootstrap below is the honest version.""")

    section("3. HOW MANY POSITIONS ARE OPEN AT ONCE")
    conc = concurrency(df)
    print(f"  median {conc.median():.0f}   mean {conc.mean():.1f}   "
          f"90th pct {conc.quantile(0.9):.0f}   max {conc.max():.0f}")
    print("""
  Risk per trade is not risk per account. Sizing every signal at f of equity
  means the account can carry several times f at once, and the Kelly fraction
  below is per *simultaneous* unit — divide it by typical concurrency before
  using it as a per-trade setting.""")

    section("4. WHAT SIZE COMPOUNDS")
    print("  g(f) = mean log growth per trade, risking f of equity per 1R.\n")
    fs = np.array([0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.15, 0.20, 0.25, 0.30,
                   0.40, 0.50, 0.60, 0.75])
    g = np.array([growth(f, r) for f in fs])
    best_i = int(np.nanargmax(np.where(np.isfinite(g), g, -np.inf)))
    print("      f      g(f) per trade    per 100 trades")
    for f, gg in zip(fs, g):
        mark = "  <-- optimum" if f == fs[best_i] else ""
        val = f"{gg:+.5f}" if np.isfinite(gg) else "   ruin"
        pct = f"{(np.exp(gg * 100) - 1) * 100:+8.1f}%" if np.isfinite(gg) else "    ruin"
        print(f"   {f:5.3f}   {val}        {pct}{mark}")

    # Refine the optimum and find where compounding stops paying.
    grid = np.linspace(0.001, 0.99, 2000)
    gg = np.array([growth(f, r) for f in grid])
    finite = np.isfinite(gg)
    f_star = grid[finite][int(np.argmax(gg[finite]))]
    zero = grid[finite][gg[finite] > 0]
    print(f"\n  Kelly-optimal f*        : {f_star:.3f} of equity per 1R")
    print(f"  half-Kelly (recommended): {f_star / 2:.3f}")
    if len(zero):
        print(f"  growth turns negative at: {zero.max():.3f}")
    print(f"  arithmetic mean          : {r.mean():+.4f}R per trade")
    print(f"  growth at f*             : {growth(f_star, r):+.5f} log-units per trade")
    print("""
  The gap between those last two is the ergodicity cost: the mean is what an
  ensemble of accounts earns between them, the growth rate is what one account
  keeps. Sizing above f* raises the mean and lowers what you actually compound.""")

    section("5. DRAWDOWN")
    rng = np.random.default_rng(7)
    for f in (f_star / 4, f_star / 2, f_star):
        eq = np.cumprod(1 + f * r)
        paths = block_bootstrap(r, 4000, block=20, rng=rng)
        dds = np.array([max_drawdown(np.cumprod(1 + f * p)) for p in paths])
        print(f"  f = {f:.3f}:  realised max drawdown {max_drawdown(eq):+.1%},  "
              f"final equity x{eq[-1]:.2f}")
        print(f"             bootstrap drawdown  median {np.median(dds):+.1%}  "
              f"95th pct {np.quantile(dds, 0.05):+.1%}  worst {dds.min():+.1%}")
    print("\n  (bootstrap resamples in blocks of 20 trades so loss clustering survives)")

    section("6. EQUITY CURVE BY MONTH")
    m = df.dropna(subset=["rho"]).groupby("month").rho.agg(["size", "sum", "mean"]).round(3)
    m.columns = ["trades", "total_R", "mean_R"]
    m["cumulative_R"] = m.total_R.cumsum().round(2)
    print(m.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
