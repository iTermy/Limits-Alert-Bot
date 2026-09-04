"""Where the stop belongs, and whether protecting a trade early pays.

Two separate questions that the same data answers:
  * moving the stop — replayed, because it changes which trades survive
  * a breakeven stop armed after the trade has run in your favour — also
    replayed, since it is an exit rule, not a reweighting

The descriptive half asks how far winners are allowed to go against you before
they win, which is what decides whether a tighter stop is even possible.

Usage:  python -m backtest.study.stops
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
from backtest.study import SIG_DIR, boot_ci, load, summarize
from backtest.ticks import TickStore

pd.set_option("display.width", 220)
BASE = dict(entry_model="broker", exit_side="realistic", exit_poll_seconds=1.0,
            require_placeable=True)

SL_MULTS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]
BE_ARMS = [0.25, 0.5, 0.75, 1.0, 1.5]


def section(t: str) -> None:
    print("\n" + "=" * 92)
    print(t)
    print("=" * 92)


def main() -> int:
    real = load("real")

    section("1. HOW FAR DO WINNERS GO AGAINST YOU FIRST?")
    print("MAE from the mean fill, in units of that signal's own TP threshold.")
    print("A stop can only be tightened to where it would not have cut the winners.\n")
    d = real.dropna(subset=["mae_thr"])
    won = d[d.rho > 0]
    lost = d[d.rho <= 0]
    print("  winners' adverse excursion:")
    print("   " + won.mae_thr.describe(percentiles=[.5, .75, .9, .95, .99]).round(3)
          .to_string().replace("\n", "\n   "))
    print("\n  losers' adverse excursion:")
    print("   " + lost.mae_thr.describe(percentiles=[.5, .75, .9]).round(3)
          .to_string().replace("\n", "\n   "))

    print("\n  share of winners that would have been cut by a stop at N x TP threshold:")
    for lvl in (0.5, 1.0, 1.5, 2.0, 3.0, 4.0):
        print(f"    stop at {lvl:>4.1f} x TP : cuts {(won.mae_thr < -lvl).mean():6.1%} of winners")

    print("\n  distance from the mean fill to the actual stop, in TP thresholds:")
    real["sl_dist_thr"] = (real.mean_fill - real.sl_price).abs() / real.thr_price
    print("   " + real.sl_dist_thr.describe(percentiles=[.1, .25, .5, .75, .9]).round(2)
          .to_string().replace("\n", "\n   "))

    section("2. DOES A BOUNCE PREDICT SURVIVAL?")
    print("The earlier M1 study found a trade that had already bounced half its TP")
    print("almost never stopped out. Retested here on ticks.\n")
    d = real.copy()
    for frac in (0.25, 0.5, 1.0):
        reached = d[f"fav_{frac}"].notna()
        stopped = d.status == "stopped"
        n_r, n_n = int(reached.sum()), int((~reached).sum())
        if n_r and n_n:
            print(f"  ran {frac:>4} x TP in favour first : stop-out {stopped[reached].mean():6.1%} "
                  f"(n={n_r:,})   mean {d.rho[reached].mean():+.4f}R")
            print(f"  never did                    : stop-out {stopped[~reached].mean():6.1%} "
                  f"(n={n_n:,})   mean {d.rho[~reached].mean():+.4f}R")

    print("\n  which extreme came first, and what it was worth:")
    ord_ = d.dropna(subset=["mae_first"])
    print(ord_.groupby("mae_first").rho.agg(["size", "mean", lambda s: (s < 0).mean()])
          .rename(columns={"<lambda_0>": "loss_rate"}).round(4).to_string())

    section("3. MOVING THE STOP")
    print("sl x0.5 = half as far from limit 1 (tighter); x2.0 = twice as far.\n")
    signals, limits = build()
    model = BasisModel.load()
    keep = set(real.signal_id)
    subset = [s for s in signals if s["id"] in keep]

    pols = {}
    for m in SL_MULTS:
        pols[f"sl x{m}"] = Policy(sl_multiplier=m, **BASE)
    for a in BE_ARMS:
        pols[f"be arm {a}"] = Policy(breakeven_at=a, **BASE)
    for a in (0.5, 1.0):
        pols[f"be arm {a} + trail"] = Policy(breakeven_at=a, tp_mode="trailing",
                                             trail_distance=1.0, trail_poll_seconds=2.0, **BASE)

    res = run(subset, limits, pols, TickStore(), model)
    ordered = res["_signals"]

    frames, rows = {}, []
    for label in pols:
        dd = describe(ordered, res[label], model)
        dd = dd[dd.n_fills > 0]
        frames[label] = dd
        s = summarize(dd.rho)
        lo, hi = boot_ci(dd.rho)
        s.update(policy=label, ci_lo=lo, ci_hi=hi,
                 stop_rate=(dd.status == "stopped").mean(),
                 be_rate=(dd.exit_reason == "breakeven").mean())
        rows.append(s)
    out = pd.DataFrame(rows).set_index("policy")
    cols = ["n", "total_R", "mean_R", "ci_lo", "ci_hi", "win_rate", "avg_win",
            "avg_loss", "stop_rate", "be_rate", "sharpe"]
    print("  stop-loss placement:")
    print(out.loc[[f"sl x{m}" for m in SL_MULTS], cols].round(4).to_string())
    print("\n  breakeven stop, armed once the trade has run N x TP in favour:")
    be_rows = [f"be arm {a}" for a in BE_ARMS] + [f"be arm {a} + trail" for a in (0.5, 1.0)]
    print(out.loc[be_rows, cols].round(4).to_string())
    out.to_pickle(os.path.join(SIG_DIR, "sweep_stops.pkl"))

    section("4. PAIRED AGAINST THE CURRENT RULE")
    base = frames["sl x1.0"].set_index("signal_id").rho
    for label in [f"sl x{m}" for m in SL_MULTS if m != 1.0] + be_rows:
        alt = frames[label].set_index("signal_id").rho
        diff = (alt - base).dropna()
        lo, hi = boot_ci(diff)
        print(f"  {label:22s} {diff.mean():+.4f}R  CI [{lo:+.4f}, {hi:+.4f}]  "
              f"better on {(diff > 0).mean():5.1%}  (n={len(diff)})")

    section("5. BY TYPE — does the right stop differ by signal type?")
    for t in sorted(real.type.unique()):
        sub = {lab: f[f.type == t] for lab, f in frames.items()}
        n = len(sub["sl x1.0"])
        if n < 25:
            continue
        rank = sorted(((lab, f.rho.mean(), f.rho.sum()) for lab, f in sub.items()),
                      key=lambda x: -x[1])
        print(f"\n  {t} (n={n})")
        for lab, mean, tot in rank[:4]:
            print(f"     {lab:22s} mean {mean:+.4f}R  total {tot:+7.1f}R")
        cur = sub["sl x1.0"].rho
        print(f"     {'(current: sl x1.0)':22s} mean {cur.mean():+.4f}R  total {cur.sum():+7.1f}R")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
