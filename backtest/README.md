# backtest — tick-level replay of signal outcomes

Replays stored signals against archived ICMarkets bid/ask ticks so exit rules,
stop levels, sizing and cancel policy can be tested against what price actually
did, instead of only against what the bot happened to do at the time.

Data lives **outside the repo** at `C:\Python Stuff\TM-Backtest-Data\` (ticks ~3.2 GB).

## Running

```
python -m backtest.archive_ticks     # pull ticks -> parquet (resumable, ~4 min)
python -m backtest.pull_signals      # pull signal tables -> pickles (one shot)
python -m backtest.calibrate         # clock / side-of-book / broker basis + drift
python -m backtest.validate          # score the engine against real outcomes
python -m backtest.build_master      # replay every signal -> analysis frames
python -m backtest.study.ev          # where the edge is
python -m backtest.study.exits       # fixed TP vs trailing, and how to configure each
python -m backtest.study.stops       # stop placement and breakeven rules
python -m backtest.study.sizing      # lot weighting across the ladder, fill depth
python -m backtest.study.risk        # distribution, drawdown, what size compounds
```

## Validation status

Scored against the 274 signals the bot closed on its own
(`closed_reason='automatic'`, status profit or stop_loss):

| | |
|---|---|
| outcome agreement | **95.6%** |
| config-at-time subset (n=33) | **97.0%** |
| fill-count match | 75.9% (84.8% where the threshold is config-at-time) |

For comparison the earlier M1-bar replay reached 94.5% on an easier test.

## Coverage

Ticks exist from **2026-04-01**; nothing earlier is recoverable, so 701 March
signals (151 entered) are permanently out of scope. The window is *rolling* — the
M1 history receded three weeks over the 16 days to 2026-08-20 — so re-archiving
extends coverage forward but never backward.

All 91 signal instruments are archived except those in `symbols.UNAVAILABLE`:
`GCQ26` (39 signals — an expired COMEX contract IC no longer carries) and seven
one-off parser typos (`EURBGBG`, `GBPCFH`, `GPBNZD`, `JPYUSD`, …). Those typos are
worth fixing upstream: they are signals that never matched any feed at all.

## What calibration established

These were measured, not assumed. Each one silently destroys the analysis if wrong.

1. **MT5 tick times are broker server time (EET/EEST), not UTC.** Uncorrected, the
   median gold fill sat **$34.72** away from the archived price; converting through
   `Europe/Athens` brings it to **$0.26**. `archive_ticks` converts at write time,
   so everything downstream is true UTC. A corrected week spans Sunday 22:00 UTC to
   Friday 20:56 UTC — the real forex session, which is the quick sanity check.
2. **`hit_time` trails the tick by ~2 s** (Supabase pooler write latency). The error
   curve is a clean V bottoming at −2 s. Only matters when comparing recorded
   timestamps to ticks, not to the replay itself.
3. **Side of the book**: longs record the **ask** (+0.02 residual on gold), shorts the
   **bid** (0.000). Exactly `current_price = ask if long else bid`.
4. **Broker basis, and it moves.** Forex, gold, silver and stocks are IC-native and
   calibrate to *exactly* 0.00000. The instruments priced off other feeds do not —
   and a single constant is not good enough for them, which the monthly breakdown in
   `calibrate` shows:

   | symbol | median basis | spread of monthly medians |
   |---|---|---|
   | JP225 | −38.20 | **207** |
   | BTCUSDT | +23.01 | **119** |
   | NAS100USD | −31.70 | 43 |
   | DE30EUR | −11.10 | 13 |
   | US30USD | −14.30 | 6.7 |
   | SPX500USD | −6.05 | 2.2 |

   `backtest.basis` therefore estimates the basis **per signal** from recorded fills
   within ±10 days, leave-one-out so a signal's own fill never calibrates its own
   replay, and carries an uncertainty with it. `is_measurable` drops a signal when
   that uncertainty exceeds half its TP threshold — locally NAS100USD is tight
   (IQR ≈ 2) and survives, JP225 does not.
5. **Horizon.** A week-end expiry signal can fill days after posting. A 48 h cap
   silently turned those into false no-fills and cost ~6 points of agreement.

## What the execution model is

Taken from the MT5 execution bot, not assumed — it decides every exit number:

- **Entries are resting limit orders** at the broker, so they fill tick-exact and
  get no spread buffer. `Policy(spread_buffer=True)` reproduces the *TM bot's*
  signalling rule, under which `ask <= limit + spread` reduces to `bid <= limit` —
  one full spread better than a real order fills.
- **The stop rests at the broker** and triggers on the side the position closes
  against: bid for a long, ask for a short. The TM bot's own convention is the
  opposite side, which would report every stop a spread late.
- **The take-profit is bot-driven**, checked on a 1 s timer and closed at market
  (`close_position` sells at bid / buys at ask). It is *not* a broker-side TP, so a
  spike that does not survive to the next check cannot be taken.
  `Policy(exit_poll_seconds=1.0)` models this; replaying exits tick-exact instead
  credits the strategy with prices no order ever got.
- **Trailing** ratchets the broker-side stop on a 2 s timer, so the mark lags but
  the trigger is tick-exact (`trail_poll_seconds=2.0`).

`build_master` therefore produces four universes — `live` (reproduces the bot),
`real` (the above), `tick` (as real but tick-exact exits), `held` (as real but
orders pulled when the bot pulled them) — so each assumption has a price rather
than an argument.

## Known modelling gaps

- **Fill count matches 75.9% overall but 84.8% where the TP threshold is
  config-at-time.** The gap is mostly threshold drift, not a modelling defect:
  `tp_threshold_used` was only stamped from 2026-07-14, and earlier signals fall
  back to today's config. A historical threshold smaller than today's makes the bot
  exit earlier and the sim fill deeper; larger does the reverse. Diagnosed directly —
  of 121 fills the sim missed, only **one** was a level the archive never reached;
  the rest were levels price did reach after the sim had already exited. This biases
  *reproducing history*, not *comparing policies*, since a sweep applies the same
  threshold to every arm.
- **Pre-2026-07-14 thresholds are today's config.** Sweeps over `tp_multiplier`
  therefore answer "today's config scaled by k", which is the actionable question;
  reconstructing what the bot historically earned needs the stamped subset.
- **Coverage starts 2026-04-01** (above).
