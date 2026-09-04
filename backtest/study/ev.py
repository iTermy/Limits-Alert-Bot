"""Where the edge is, and what it survives.

Usage:  python -m backtest.study.ev
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backtest.study import SIG_DIR, asset_class, boot_ci, load, summarize, table

pd.set_option("display.width", 200)


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def main() -> int:
    master = pd.read_pickle(os.path.join(SIG_DIR, "master_signals.pkl"))
    real = load("real")

    section("1. WHAT THE ASSUMPTIONS COST")
    print("Same signals, same exits — only the execution model changes.\n")
    rows = []
    for pol in ("live", "real", "bare", "held"):
        d = load(pol)
        s = summarize(d.rho)
        s["policy"] = pol
        rows.append(s)
    comp = pd.DataFrame(rows).set_index("policy")
    print(comp[["n", "total_R", "mean_R", "win_rate", "avg_win", "avg_loss"]].round(3).to_string())
    print("""
  live  the TM bot's own frame: a fill is recorded at the level itself, exits
        read the bid in both directions, nothing is polled.
  real  what the execution bot places: each limit rests one spread inside its
        level, so it fills where the buffered rule says but transacts a spread
        worse, with its stop shifted a spread further out; take-profit polled on
        a 1 s timer and closed at market.  <-- every result below uses this
  bare  an order resting at the bare level, no spread shift; the gap to `real`
        is what that adjustment is worth.
  held  as real but orders are pulled when the bot pulled them; the gap to real
        is what the near-miss / news / spread-hour cancels were worth.""")

    section("2. COVERAGE AND WHAT IS EXCLUDED")
    all_real = master[master.policy == "real"]
    ent = all_real[all_real.n_fills > 0]
    print(f"  signals replayed              : {len(all_real):,}")
    print(f"  entered under `real`          : {len(ent):,}")
    print(f"  of those, basis unmeasurable  : {int((~ent.measurable).sum()):,}")
    bad = ent[~ent.measurable]
    if len(bad):
        print("\n  unmeasurable by instrument (broker basis drifts wider than the TP threshold):")
        print("   " + bad.instrument.value_counts().head(10).to_string().replace("\n", "\n   "))
    print(f"\n  analysis universe             : {len(real):,} entered, measurable, non-instant")
    print(f"  the bot actually entered      : {int((all_real.real_limits_hit > 0).sum()):,}")
    print("""
  The replay enters far more often than the bot did because it lets every order
  rest to expiry, while the bot cancelled most signals first. That is the point:
  it is the counterfactual the cancels were never measured against.""")

    section("3. THE HEADLINE")
    s = summarize(real.rho)
    lo, hi = boot_ci(real.rho)
    print(f"  n                {s['n']:,}")
    print(f"  total            {s['total_R']:+.1f}R")
    print(f"  mean             {s['mean_R']:+.4f}R   95% CI [{lo:+.4f}, {hi:+.4f}]")
    print(f"  median           {s['median_R']:+.4f}R")
    print(f"  win rate         {s['win_rate']:.1%}")
    print(f"  avg win / loss   {s['avg_win']:+.3f}R / {s['avg_loss']:+.3f}R")
    print(f"  std / Sharpe     {s['std']:.3f} / {s['sharpe']:.3f} per trade")
    verdict = "positive" if lo > 0 else ("negative" if hi < 0 else "NOT distinguishable from zero")
    print(f"\n  -> the edge is {verdict} at 95% confidence.")

    section("4. BY SIGNAL TYPE")
    print(table(real, "type", min_n=10).to_string())

    section("5. BY ASSET CLASS")
    print(table(real, "asset", min_n=10).to_string())

    section("6. BY INSTRUMENT (n >= 20)")
    print(table(real, "instrument", min_n=20).to_string())

    section("7. BY FILL DEPTH")
    print("Does taking more of the ladder help or hurt?\n")
    d = real.assign(depth=real.n_fills.clip(upper=6))
    print(table(d, "depth", sort="depth").to_string())
    print("\n  same, restricted to gold tolls (the historical profit engine):")
    g = d[(d.type == "toll") & (d.asset == "gold")]
    if len(g) > 20:
        print(table(g, "depth", sort="depth").to_string())

    section("8. BY LADDER SIZE (limits the signal offered, not what filled)")
    d = real.assign(ladder=real.total_limits.clip(upper=8))
    print(table(d, "ladder", sort="ladder").to_string())

    section("9. WHEN")
    print("by hour of entry, New York time (n >= 15):")
    print(table(real, "hour_et", min_n=15, sort="hour_et").to_string())
    print("\nby weekday (0=Mon):")
    print(table(real, "dow", sort="dow").to_string())
    print("\nby month:")
    print(table(real, "month", sort="month").to_string())

    section("10. DIRECTION AND EXIT MIX")
    print(table(real, "direction").to_string())
    print("\nhow trades ended:")
    mix = real.groupby("exit_reason").rho.agg(["size", "mean", "sum"]).round(3)
    mix.columns = ["n", "mean_R", "total_R"]
    print(mix.sort_values("total_R", ascending=False).to_string())

    section("11. WHAT THE CANCELS WERE WORTH")
    print("""Every cancel is a trade the bot refused. Those refusals have never been
scored, because once a signal is cancelled nothing records what price did next.
The replay does: `real` lets the order rest, `held` pulls it when the bot did.
A signal that `real` entered and `held` never did is a trade the cancel avoided —
and its R is what avoiding it was worth (negative R avoided = the cancel paid).\n""")
    real_all = load("real", measurable_only=True)
    held_all = load("held", measurable_only=True)
    held_ids = set(held_all.signal_id)
    avoided = real_all[~real_all.signal_id.isin(held_ids)]

    sc = pd.read_pickle(os.path.join(SIG_DIR, "status_changes.pkl"))
    cancels = sc[sc.new_status == "cancelled"].sort_values("changed_at")
    reason = cancels.groupby("signal_id").reason.last()
    av = avoided.assign(cancel_reason=avoided.signal_id.map(reason).fillna("unrecorded"))

    print(f"  trades the cancels prevented : {len(av):,}")
    if len(av):
        s = summarize(av.rho)
        lo, hi = boot_ci(av.rho)
        print(f"  their combined result        : {s['total_R']:+.1f}R "
              f"(mean {s['mean_R']:+.4f}R, CI [{lo:+.4f}, {hi:+.4f}], win {s['win_rate']:.1%})")
        print(f"\n  -> cancelling was worth {-s['total_R']:+.1f}R overall.\n")
        print("  by cancel reason:")
        by = av.groupby(av.cancel_reason.str.slice(0, 28)).rho.agg(["size", "mean", "sum"])
        by.columns = ["n", "mean_R", "total_R_forgone"]
        print("   " + by.sort_values("n", ascending=False).head(12).round(3)
              .to_string().replace("\n", "\n   "))

    section("12. DOES THE EDGE HOLD OUT OF SAMPLE?")
    print("Split by time — the second half is the only honest test of the first.\n")
    mid = real.created_at.quantile(0.5)
    for label, part in (("first half ", real[real.created_at <= mid]),
                        ("second half", real[real.created_at > mid])):
        st = summarize(part.rho)
        lo, hi = boot_ci(part.rho)
        print(f"  {label}  n={st['n']:>4}  total {st['total_R']:+7.1f}R  "
              f"mean {st['mean_R']:+.4f}R  CI [{lo:+.4f}, {hi:+.4f}]  win {st['win_rate']:.1%}")

    print("\n  by type, first vs second half (mean R):")
    piv = real.assign(half=np.where(real.created_at <= mid, "first", "second")) \
              .pivot_table(index="type", columns="half", values="rho",
                           aggfunc=["mean", "size"]).round(4)
    print(piv.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
