# backtest — tick-level replay of signal outcomes

Replays stored signals against archived ICMarkets bid/ask ticks so exit rules,
stop levels, sizing and cancel policy can be tested against what price actually
did, instead of only against what the bot happened to do at the time.

Data lives **outside the repo** at `C:\Python Stuff\TM-Backtest-Data\` (ticks ~1.5 GB).

## Running

```
python -m backtest.archive_ticks     # pull ticks -> parquet (resumable, ~3 min)
python -m backtest.pull_signals      # pull signal tables -> pickles (one shot)
python -m backtest.calibrate         # clock / side-of-book / broker basis
python -m backtest.validate          # score the engine against real outcomes
```

## Validation status

Scored against the 244 signals the bot closed on its own
(`closed_reason='automatic'`, status profit or stop_loss):

| | |
|---|---|
| outcome agreement | **95.1%** |
| config-at-time subset (n=28) | **96.4%** |
| fill-count match | 74.6% |

For comparison the earlier M1-bar replay reached 94.5% on an easier test.

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
4. **Broker basis.** Forex and gold are IC-native and calibrate to *exactly* 0.00000.
   The instruments the bot priced off other feeds do not:
   SPX500USD −6.05, NAS100USD −31.70, JP225 −38.20, DE30EUR −11.10, BTCUSDT +23.01
   (price units). `simulate(..., basis=)` shifts signal levels into IC space; without
   it JP225 agreement drops from 75% to 50%.
5. **Horizon.** A week-end expiry signal can fill days after posting. A 48 h cap
   silently turned those into false no-fills and cost ~6 points of agreement.

## Known modelling gaps

- **Fill count matches only 74.6%.** The replay sees every tick; the live bot polled
  at 100 ms per symbol, so brief wicks that a resting limit order *would* have filled
  were invisible to it. Sim fills deeper than reality on 11% of signals and shallower
  on 20%. The shallow side is not yet explained — bot downtime and the news /
  spread-hour guards are the leading candidates. Worth resolving before per-limit
  sizing conclusions, since it directly biases fill depth.
- **The spread buffer makes entries optimistic.** With it on, `ask <= limit + spread`
  reduces exactly to `bid <= limit`, so the bot signals a long fill one full spread
  before a real limit order at that price would fill. `Policy(spread_buffer=False)`
  models the realistic fill; both agree with history to within 0.4 points, so history
  cannot distinguish them — but they differ for real money.
- **TP thresholds before 2026-07-14 are not config-at-time.** `tp_threshold_used` was
  only stamped from then; earlier signals fall back to today's config
  (`threshold_stamped=False`). Exit-tuning work should exclude them or treat them as
  a sensitivity band.
- **Coverage starts 2026-04-01** — the terminal has no ticks before that, so 701
  March signals (151 entered) are permanently out of reach. The window is *rolling*:
  the M1 history receded three weeks over the 16 days to 2026-08-20, so re-archiving
  extends coverage forward but never backward.
