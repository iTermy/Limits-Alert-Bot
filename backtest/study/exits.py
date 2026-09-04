"""Fixed take-profit against trailing, and how each should be configured.

Only signals that filled at least one limit are replayed: entry does not depend
on the exit rule, so a signal that never filled contributes nothing to any arm
and only costs time.

Usage:  python -m backtest.study.exits [--types toll,standard]
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backtest.basis import BasisModel
from backtest.dataset import build
from backtest.engine import Policy
from backtest.runner import describe, run
from backtest.study import SIG_DIR, boot_ci, load, summarize
from backtest.ticks import TickStore

pd.set_option("display.width", 220)

# Execution truth: resting limit entries, broker-side stop, take-profit polled
# on the bot's 1 s timer, trailing stop ratcheted on its 2 s timer.
BASE = dict(entry_model="broker", exit_side="realistic", exit_poll_seconds=1.0,
            trail_poll_seconds=2.0, require_placeable=True)

TP_MULTIPLIERS = [0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0]
TRAIL_ARMS = [0.5, 0.75, 1.0, 1.5]
TRAIL_GAPS = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]
TIME_STOPS = [5, 15, 30, 60, 240]
PARTIALS = [0.25, 0.5, 0.75]


def policies() -> dict:
    pols = {"hold to expiry": Policy(tp_mode="none", **BASE)}
    for m in TP_MULTIPLIERS:
        pols[f"fixed x{m}"] = Policy(tp_mode="fixed", tp_multiplier=m, **BASE)
    for arm in TRAIL_ARMS:
        for gap in TRAIL_GAPS:
            pols[f"trail arm{arm} gap{gap}"] = Policy(
                tp_mode="trailing", tp_multiplier=arm, trail_distance=gap, **BASE)
    # A stalled level-bounce trade is a thesis that did not happen; these ask
    # whether giving up on it early is better than waiting for the stop.
    for m in TIME_STOPS:
        pols[f"fixed +{m}min stop"] = Policy(tp_mode="fixed", time_stop_minutes=m, **BASE)
    # Take part at the target, run the rest — the execution bot can already do
    # this, so it is a setting rather than a build.
    for p in PARTIALS:
        pols[f"partial {p:g} + trail1.0"] = Policy(
            tp_mode="trailing", trail_distance=1.0, partial_close=p, **BASE)
    return pols


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--types", help="comma-separated signal types to restrict to")
    args = ap.parse_args()

    signals, limits = build()
    model = BasisModel.load()

    master = pd.read_pickle(os.path.join(SIG_DIR, "master_signals.pkl"))
    # One universe definition for every study, so two of them cannot disagree
    # about which signals are trades.
    keep = set(load("real").signal_id)
    subset = [s for s in signals if s["id"] in keep]
    if args.types:
        wanted = {t.strip() for t in args.types.split(",")}
        subset = [s for s in subset if s["type"] in wanted]
    print(f"replaying {len(subset):,} entered signals under {len(policies())} exit policies\n")

    res = run(subset, limits, policies(), TickStore(), model)
    ordered = res["_signals"]

    frames = {}
    rows = []
    for label in policies():
        d = describe(ordered, res[label], model)
        d = d[d.n_fills > 0]
        frames[label] = d
        s = summarize(d.rho)
        lo, hi = boot_ci(d.rho)
        s.update(policy=label, ci_lo=lo, ci_hi=hi,
                 stop_rate=(d.status == "stopped").mean(),
                 expired=(d.status == "expired").mean(),
                 med_minutes=d.minutes_in_trade.median())
        rows.append(s)

    out = pd.DataFrame(rows).set_index("policy")
    cols = ["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate", "avg_win",
            "avg_loss", "stop_rate", "sharpe", "med_minutes"]
    out = out[cols].round(3)
    out.to_pickle(os.path.join(SIG_DIR, "sweep_exits.pkl"))

    print("=" * 100)
    print("ALL EXIT POLICIES, ranked by total R")
    print("=" * 100)
    print(out.sort_values("total_R", ascending=False).to_string())

    print("\n" + "=" * 100)
    print("FIXED TP: how far should the target sit?")
    print("=" * 100)
    print(out.loc[[f"fixed x{m}" for m in TP_MULTIPLIERS]].to_string())

    print("\n" + "=" * 100)
    print("TRAILING: mean R by arming point (rows) and trail gap (columns)")
    print("=" * 100)
    grid = pd.DataFrame(
        [[out.loc[f"trail arm{a} gap{g}", "mean_R"] for g in TRAIL_GAPS] for a in TRAIL_ARMS],
        index=[f"arm {a}" for a in TRAIL_ARMS], columns=[f"gap {g}" for g in TRAIL_GAPS])
    print(grid.round(4).to_string())
    print("\nsame grid, total R:")
    grid_t = pd.DataFrame(
        [[out.loc[f"trail arm{a} gap{g}", "total_R"] for g in TRAIL_GAPS] for a in TRAIL_ARMS],
        index=[f"arm {a}" for a in TRAIL_ARMS], columns=[f"gap {g}" for g in TRAIL_GAPS])
    print(grid_t.round(1).to_string())

    print("\n" + "=" * 100)
    print("BEST POLICY PER SIGNAL TYPE (mean R, n >= 25)")
    print("=" * 100)
    per_type = {}
    for label, d in frames.items():
        for t, grp in d.groupby("type"):
            if len(grp) >= 25:
                per_type.setdefault(t, {})[label] = (grp.rho.mean(), grp.rho.sum(), len(grp))
    for t, scores in sorted(per_type.items()):
        rank = sorted(scores.items(), key=lambda kv: -kv[1][0])
        n = rank[0][1][2]
        print(f"\n  {t}  (n={n})")
        for label, (mean, tot, _) in rank[:6]:
            print(f"     {label:24s} mean {mean:+.4f}R   total {tot:+7.1f}R")
        base = scores.get("fixed x1.0")
        if base:
            print(f"     {'(current: fixed x1.0)':24s} mean {base[0]:+.4f}R   total {base[1]:+7.1f}R")

    print("\n" + "=" * 100)
    print("SANITY: does the ranking survive on trades the bot would actually have taken?")
    print("=" * 100)
    print("The sweep above lets every order rest to expiry, so it includes signals")
    print("the bot cancelled before they filled. A recommendation that only wins on")
    print("those is not actionable.\n")
    held = master[master.policy == "held"]
    achievable = set(held.loc[held.n_fills > 0, "signal_id"])
    rows = []
    for label, d in frames.items():
        sub = d[d.signal_id.isin(achievable)]
        if len(sub) < 20:
            continue
        rows.append(dict(policy=label, n=len(sub), mean_R=sub.rho.mean(),
                         total_R=sub.rho.sum(), sharpe=sub.rho.mean() / sub.rho.std()))
    ach = pd.DataFrame(rows).set_index("policy").sort_values("mean_R", ascending=False)
    print(ach.round(4).head(12).to_string())
    print("\n  current rule for comparison:")
    print(ach.loc[["fixed x1.0"]].round(4).to_string())

    print("\n" + "=" * 100)
    print("PAIRED TEST: trailing minus fixed, on the same signals")
    print("=" * 100)
    print("A per-signal difference removes the noise of which trades happened to")
    print("come up, which is what a table of separate means cannot do.\n")
    base = frames["fixed x1.0"].set_index("signal_id").rho
    for label in [f"trail arm1.0 gap{g}" for g in TRAIL_GAPS]:
        alt = frames[label].set_index("signal_id").rho
        diff = (alt - base).dropna()
        lo, hi = boot_ci(diff)
        better = (diff > 0).mean()
        print(f"  {label:24s} mean diff {diff.mean():+.4f}R  CI [{lo:+.4f}, {hi:+.4f}]  "
              f"better on {better:.1%} of signals  (n={len(diff)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
