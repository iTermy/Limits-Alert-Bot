"""Settle trailing against fixed take-profit with a paired test at the best arms.

The broad sweep paired only at an arming point of 1.0x the threshold, which is
not where trailing looked best. A per-signal difference is the right test here:
the two rules are run on the identical trade list, so the comparison does not
inherit the noise of which signals happened to occur.

Reported on both universes, because a rule that only wins on signals the bot
cancelled before they filled is not a rule anyone can run.

Usage:  python -m backtest.study.trailing_vs_fixed
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backtest.basis import BasisModel
from backtest.dataset import build
from backtest.engine import Policy
from backtest.runner import describe, run
from backtest.study import SIG_DIR, boot_ci, load, summarize
from backtest.ticks import TickStore

pd.set_option("display.width", 200)
BASE = dict(entry_model="broker", exit_side="realistic", exit_poll_seconds=1.0,
            trail_poll_seconds=2.0, require_placeable=True)

CANDIDATES = {
    "fixed x1.0 (current)": Policy(tp_mode="fixed", **BASE),
    "fixed x0.8": Policy(tp_mode="fixed", tp_multiplier=0.8, **BASE),
    "trail arm0.5 gap0.75": Policy(tp_mode="trailing", tp_multiplier=0.5,
                                   trail_distance=0.75, **BASE),
    "trail arm0.75 gap0.5": Policy(tp_mode="trailing", tp_multiplier=0.75,
                                   trail_distance=0.5, **BASE),
    "trail arm0.75 gap1.0": Policy(tp_mode="trailing", tp_multiplier=0.75,
                                   trail_distance=1.0, **BASE),
    "trail arm0.75 gap1.5": Policy(tp_mode="trailing", tp_multiplier=0.75,
                                   trail_distance=1.5, **BASE),
}


def report(frames: dict, ids: set, label: str) -> None:
    print("\n" + "=" * 84)
    print(label)
    print("=" * 84)
    rows = []
    for name, d in frames.items():
        sub = d[d.signal_id.isin(ids)] if ids is not None else d
        s = summarize(sub.rho)
        lo, hi = boot_ci(sub.rho)
        s.update(policy=name, ci_lo=lo, ci_hi=hi)
        rows.append(s)
    tbl = pd.DataFrame(rows).set_index("policy")
    print(tbl[["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate",
               "avg_win", "avg_loss", "sharpe"]].round(4).to_string())

    print("\n  paired against the current rule (same signals, per-signal difference):")
    base = frames["fixed x1.0 (current)"]
    base = base[base.signal_id.isin(ids)] if ids is not None else base
    base = base.set_index("signal_id").rho
    for name, d in frames.items():
        if name.startswith("fixed x1.0"):
            continue
        alt = d[d.signal_id.isin(ids)] if ids is not None else d
        alt = alt.set_index("signal_id").rho
        diff = (alt - base).dropna()
        lo, hi = boot_ci(diff)
        verdict = "better" if lo > 0 else ("worse" if hi < 0 else "not separable")
        print(f"    {name:22s} {diff.mean():+.4f}R  CI [{lo:+.4f}, {hi:+.4f}]  "
              f"wins on {(diff > 0).mean():5.1%}  -> {verdict}")


def main() -> int:
    signals, limits = build()
    model = BasisModel.load()
    universe = load("real")
    keep = set(universe.signal_id)
    subset = [s for s in signals if s["id"] in keep]
    print(f"replaying {len(subset):,} signals under {len(CANDIDATES)} exit rules")

    res = run(subset, limits, CANDIDATES, TickStore(), model)
    ordered = res["_signals"]
    frames = {}
    for name in CANDIDATES:
        d = describe(ordered, res[name], model)
        frames[name] = d[d.n_fills > 0]

    report(frames, None, "EVERY SIGNAL THAT FILLED (orders rest to expiry)")

    master = pd.read_pickle(os.path.join(SIG_DIR, "master_signals.pkl"))
    held = master[(master.policy == "held") & (master.n_fills > 0)]
    report(frames, set(held.signal_id),
           "ONLY TRADES THE BOT WOULD ACTUALLY HAVE TAKEN (cancels honoured)")

    print("\n" + "=" * 84)
    print("BY SIGNAL TYPE (mean R)")
    print("=" * 84)
    piv = pd.concat([d.assign(policy=n) for n, d in frames.items()])
    print(piv.pivot_table(index="type", columns="policy", values="rho",
                          aggfunc="mean").round(4).to_string())
    print("\ntrade counts by type:")
    print(frames["fixed x1.0 (current)"].type.value_counts().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
