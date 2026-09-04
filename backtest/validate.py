"""Score the replay against outcomes the bot produced on its own.

Only closures the bot decided without a human are usable as ground truth
(`closed_reason='automatic'`, status profit or stop_loss). If the engine cannot
reproduce those, nothing built on top of it means anything.

Usage:  python -m backtest.validate
"""

import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.basis import BasisModel
from backtest.dataset import build
from backtest.engine import Policy, pip_size, simulate
from backtest.ticks import TickStore

LIVE = Policy(spread_buffer=True, tp_mode="fixed", honor_expiry=True, horizon_hours=400)


def main() -> int:
    signals, limits = build()
    model = BasisModel.load()

    def basis_of(s):
        # Leave-one-out: a signal's own fills are excluded from its basis, so the
        # replay is never scored against a level calibrated on the answer.
        return model.basis_for(s["instrument"], s["created_at"], s["id"])[0]

    print(f"replayable signals: {len(signals):,}")

    truth = [s for s in signals
             if s["closed_reason"] == "automatic" and s["status"] in ("profit", "stop_loss")]
    print(f"ground-truth set (bot closed it automatically): {len(truth):,}")
    stamped = [s for s in truth if s["threshold_stamped"]]
    print(f"  of which have config-at-time thresholds: {len(stamped):,}\n")

    store = TickStore()

    # Which entry model actually reproduces reality? The spread buffer flips the
    # fill test to the far side of the book, so it is a hypothesis to test, not a
    # setting to inherit.
    print("entry-model comparison (outcome agreement):")
    for label, pol in [("buffer ON  (fill on far side)", LIVE),
                       ("buffer OFF (real limit fill)",
                        Policy(spread_buffer=False, tp_mode="fixed", horizon_hours=400))]:
        for use_basis in (False, True):
            agree = fills = 0
            for s in truth:
                o = simulate(s, limits[s["id"]], store, pol,
                             basis=basis_of(s) if use_basis else 0.0)
                agree += o.status == ("tp" if s["status"] == "profit" else "stopped")
                fills += o.n_fills == s["limits_hit"]
            tag = "with basis" if use_basis else "no basis  "
            print(f"   {label}  {tag}: outcome {agree / len(truth):6.1%}   "
                  f"fills {fills / len(truth):6.1%}")
    print()

    rows = []
    t0 = time.time()
    for s in truth:
        out = simulate(s, limits[s["id"]], store, LIVE, basis=basis_of(s))
        expected = "tp" if s["status"] == "profit" else "stopped"
        rows.append(dict(
            signal_id=s["id"], instrument=s["instrument"], type=s["type"],
            stamped=s["threshold_stamped"], expected=expected, got=out.status,
            agree=out.status == expected,
            real_fills=s["limits_hit"], sim_fills=out.n_fills,
            fills_agree=out.n_fills == s["limits_hit"],
            real_tp=s["tp_price"], sim_exit=out.exit_price,
            rho=out.rho, n_ticks=out.n_ticks,
        ))
    el = time.time() - t0
    df = pd.DataFrame(rows)
    print(f"replayed {len(df)} signals in {el:.1f}s ({el / max(len(df), 1) * 1000:.0f} ms each)\n")

    print("=" * 66)
    print("OUTCOME AGREEMENT")
    print("=" * 66)
    print(f"  overall            : {df.agree.mean():6.1%}  (n={len(df)})")
    sub = df[df.stamped]
    if len(sub):
        print(f"  config-at-time only: {sub.agree.mean():6.1%}  (n={len(sub)})")
    print(f"  fill-count match   : {df.fills_agree.mean():6.1%}")
    print("\n  by expected outcome:")
    print(df.groupby("expected").agree.agg(["mean", "size"]).round(3).to_string())
    print("\n  confusion (expected -> got):")
    print(pd.crosstab(df.expected, df.got).to_string())

    print("\n  worst instruments (n >= 5):")
    g = df.groupby("instrument").agree.agg(["mean", "size"])
    print(g[g["size"] >= 5].sort_values("mean").head(8).round(3).to_string())

    won = df[(df.expected == "tp") & df.agree & df.real_tp.notna() & df.sim_exit.notna()].copy()
    if len(won):
        won["err"] = (won.sim_exit - won.real_tp).abs() / won.instrument.map(pip_size)
        print(f"\n  TP price error on agreeing wins: median {won.err.median():.1f} pips, "
              f"90th pct {won.err.quantile(0.9):.1f} pips (n={len(won)})")

    out_path = os.path.join(r"C:\Python Stuff\TM-Backtest-Data\signals", "validation.pkl")
    df.to_pickle(out_path)
    print(f"\nsaved -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
