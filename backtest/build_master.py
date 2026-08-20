"""Replay every in-window signal and persist the frames the analysis reads.

Three universes, because they answer different questions and disagreeing about
which one is "the result" is how backtests mislead:

  live    reproduces the TM bot exactly — the level counts as filled when the far
          side of the book touches it and the level itself is recorded as the fill
          price, exits read the bid in both directions, nothing is polled. This is
          the frame the bot's own P&L records are in.
  real    what the execution bot actually does: every limit rests one spread
          inside its level, so it fills where the TM bot says it does but
          transacts a spread worse, with its stop shifted a spread further out;
          the stop rests at the broker and triggers on the side the position
          closes against; the take-profit is checked on a 1 s timer and closed at
          market.
  bare    an order resting at the bare level with no spread shift. The gap to
          `real` is what the execution bot's spread adjustment costs.
  held    `real`, but orders are pulled when the bot actually pulled them. The
          difference between this and `real` is the price of the cancel machinery
          (near-miss, news, spread hour), which has never been measured.

`real` is the honest basis for every policy question. The others price a specific
assumption rather than leaving it to be argued about. 1R is the signal's nominal
full-fill risk in every universe, so the denominators are comparable.

Usage:  python -m backtest.build_master
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtest.basis import BasisModel
from backtest.dataset import SIG_DIR, build
from backtest.engine import Policy
from backtest.runner import describe, fill_rows, run
from backtest.ticks import TickStore

LIVE = Policy(spread_buffer=True, entry_model="signal", exit_side="live")
REAL = Policy(entry_model="broker", exit_side="realistic", exit_poll_seconds=1.0,
              require_placeable=True)
BARE = Policy(spread_buffer=False, entry_model="signal", exit_side="realistic",
              exit_poll_seconds=1.0, require_placeable=True)


def main() -> int:
    signals, limits = build()
    model = BasisModel.load()
    store = TickStore()
    print(f"replayable signals: {len(signals):,}\n")

    print("universe: live + real + bare (orders rest until expiry)")
    res = run(signals, limits, {"live": LIVE, "real": REAL, "bare": BARE}, store, model)
    ordered = res["_signals"]

    frames = []
    for label in ("live", "real", "bare"):
        df = describe(ordered, res[label], model)
        df.insert(0, "policy", label)
        frames.append(df)

    print("\nuniverse: held (orders pulled when the bot pulled them)")
    res_h = run(signals, limits, {"held": REAL}, store, model, honor_cancel=True)
    df = describe(res_h["_signals"], res_h["held"], model)
    df.insert(0, "policy", "held")
    frames.append(df)

    master = pd.concat(frames, ignore_index=True)
    master.to_pickle(os.path.join(SIG_DIR, "master_signals.pkl"))

    fills = fill_rows(ordered, res["real"])
    fills.to_pickle(os.path.join(SIG_DIR, "master_fills.pkl"))

    print(f"\nmaster_signals {master.shape}  master_fills {fills.shape}")
    for label, grp in master.groupby("policy"):
        ent = grp[grp.n_fills > 0]
        print(f"  {label:5s}: entered {len(ent):>5,}  measurable {int(ent.measurable.sum()):>5,}  "
              f"tp {int((ent.status == 'tp').sum()):>4}  stopped {int((ent.status == 'stopped').sum()):>4}  "
              f"expired {int((ent.status == 'expired').sum()):>4}")
    print(f"\nsaved -> {SIG_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
