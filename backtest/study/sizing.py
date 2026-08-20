"""How to spread risk across the ladder, and whether to take all of it.

Lot weights are a pure post-processing of the replay: the auto-TP rule tests each
limit's P&L per unit, so it does not depend on how much size sits on each limit.
Capping fill depth does change the exit, so that part is replayed properly.

Every scheme is normalised to the same risk: if all limits fill and price reaches
the stop, the signal loses exactly 1R. That is the only way front-loading and
back-loading can be compared, since otherwise the "better" scheme is just the one
carrying more size.

Usage:  python -m backtest.study.sizing
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backtest.basis import BasisModel
from backtest.dataset import build
from backtest.engine import Policy
from backtest.runner import describe, run
from backtest.study import SIG_DIR, boot_ci, load, load_fills, summarize
from backtest.ticks import TickStore

pd.set_option("display.width", 220)
BASE = dict(entry_model="broker", exit_side="realistic", exit_poll_seconds=1.0,
            require_placeable=True)


def weights(n: int, risk: np.ndarray, scheme: str) -> np.ndarray:
    """Unnormalised lot weights over `n` limits with per-unit risks `risk`."""
    i = np.arange(1, n + 1)
    if scheme == "fixed lot":
        w = np.ones(n)
    elif scheme == "equal risk":
        w = 1.0 / risk
    elif scheme == "front linear":
        w = (n - i + 1).astype(float)
    elif scheme == "back linear":
        w = i.astype(float)
    elif scheme == "front x2":
        w = 2.0 ** (n - i)
    elif scheme == "back x2":
        w = 2.0 ** (i - 1)
    elif scheme == "first only":
        w = np.zeros(n); w[0] = 1.0
    elif scheme == "last only":
        w = np.zeros(n); w[-1] = 1.0
    else:
        raise ValueError(scheme)
    return w


SCHEMES = ["fixed lot", "equal risk", "front linear", "back linear",
           "front x2", "back x2", "first only", "last only"]


def section(t: str) -> None:
    print("\n" + "=" * 88)
    print(t)
    print("=" * 88)


def main() -> int:
    real = load("real")
    fills = load_fills()
    fills = fills[fills.signal_id.isin(set(real.signal_id))]

    # Per-signal ladder geometry, from every limit the signal offered — the
    # normalisation must not depend on which limits happened to fill.
    _, limits_by_signal = build()
    section("1. LOT WEIGHTING ACROSS THE LADDER")
    print("All schemes carry the same total risk: full fill to the stop = -1R.\n")

    fills_by_signal = {sid: g for sid, g in fills.groupby("signal_id")}
    rows = []
    per_signal = {}
    for scheme in SCHEMES:
        out = {}
        for sid, grp in fills_by_signal.items():
            lim = limits_by_signal.get(sid)
            if lim is None:
                continue
            sl = grp.sl_price.iloc[0]
            lvls = np.array([float(r["price_level"]) for r in
                             sorted(lim, key=lambda r: r["sequence_number"])])
            risk = np.abs(lvls - sl)
            if not risk.all():
                continue
            w = weights(len(lvls), risk, scheme)
            scale = (w * risk).sum()
            if scale <= 0:
                continue
            w = w / scale                      # total risk == 1
            seqs = grp.seq.to_numpy() - 1
            seqs = seqs[(seqs >= 0) & (seqs < len(w))]
            if not len(seqs):
                continue
            pnl = float((w[seqs] * grp.pnl_price.to_numpy()[: len(seqs)]).sum())
            out[sid] = pnl
        per_signal[scheme] = pd.Series(out)
        s = summarize(per_signal[scheme])
        lo, hi = boot_ci(per_signal[scheme])
        s.update(scheme=scheme, ci_lo=lo, ci_hi=hi)
        rows.append(s)

    tbl = pd.DataFrame(rows).set_index("scheme")
    print(tbl[["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate",
               "avg_win", "avg_loss", "sharpe"]].round(4).to_string())

    print("\npaired against the current scheme (fixed lot per limit):")
    base = per_signal["fixed lot"]
    for scheme in SCHEMES:
        if scheme == "fixed lot":
            continue
        diff = (per_signal[scheme] - base).dropna()
        lo, hi = boot_ci(diff)
        print(f"  {scheme:14s} {diff.mean():+.4f}R  CI [{lo:+.4f}, {hi:+.4f}]  "
              f"better on {(diff > 0).mean():.1%}")

    section("2. WHERE THE MONEY IS MADE ALONG THE LADDER")
    print("Per-limit R, each fill measured against its own distance to the stop.\n")
    f = fills.copy()
    f["unit_R"] = f.pnl_price / f.risk_price
    print(f.groupby(f.seq.clip(upper=6)).unit_R.agg(["size", "mean", "median"]).round(4).to_string())
    print("\n  ...restricted to signals that filled deep (4+ limits):")
    deep = f[f.depth >= 4]
    if len(deep):
        print(deep.groupby(deep.seq.clip(upper=6)).unit_R.agg(
            ["size", "mean", "median"]).round(4).to_string())

    section("3. CAPPING FILL DEPTH AND SKIPPING WIDE LADDERS")
    print("These change the exit, so they are replayed rather than reweighted.\n")
    signals, limits = build()
    model = BasisModel.load()
    keep = set(real.signal_id)
    subset = [s for s in signals if s["id"] in keep]

    pols = {"take everything": Policy(**BASE)}
    for d in (1, 2, 3, 4):
        pols[f"cap at {d} fills"] = Policy(max_fill_depth=d, **BASE)
    for k in (3, 4, 5, 6):
        pols[f"skip ladders > {k}"] = Policy(skip_if_limits_over=k, **BASE)

    res = run(subset, limits, pols, TickStore(), model)
    ordered = res["_signals"]

    def eligible_risk(sid: int, sl: float, cap_n) -> float:
        """Risk of the deepest position a policy is allowed to build.

        A cap means fewer limits fill, so the position risks less than the full
        ladder. Left on the full-ladder denominator a capped policy looks worse
        purely for carrying less size; renormalising asks the real question,
        which is whether the freed risk was better spent elsewhere.
        """
        lim = limits_by_signal.get(sid)
        lvls = [float(r["price_level"]) for r in sorted(lim, key=lambda r: r["sequence_number"])]
        if cap_n:
            lvls = lvls[:cap_n]
        return sum(abs(lv - sl) for lv in lvls)

    rows = []
    for label, pol in pols.items():
        d = describe(ordered, res[label], model)
        d = d[d.n_fills > 0].copy()
        if pol.max_fill_depth:
            denom = d.apply(lambda r: eligible_risk(r.signal_id, r.sl_price,
                                                    pol.max_fill_depth), axis=1)
            d["rho"] = d.pnl_price / denom.replace(0, np.nan)
        s = summarize(d.rho)
        lo, hi = boot_ci(d.rho)
        s.update(policy=label, ci_lo=lo, ci_hi=hi)
        rows.append(s)
    cap = pd.DataFrame(rows).set_index("policy")
    print(cap[["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate",
               "avg_win", "avg_loss", "sharpe"]].round(4).to_string())
    print("""
  Capped policies are renormalised so a full stop-out still costs 1R — otherwise
  they would score lower just for deploying less size.
  A skip rule trades fewer signals, so total R falls even when it is right;
  mean_R and Sharpe are what say whether the skipped signals were worth taking.""")
    cap.to_pickle(os.path.join(SIG_DIR, "sweep_sizing.pkl"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
