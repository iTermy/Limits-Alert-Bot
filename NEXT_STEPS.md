# NEXT_STEPS — instrumentation & changes to make the next analysis better

Ordered by impact. Items 1–3 close the gaps that most limited the 2026-07-11 analysis (see STRATEGY_ANALYSIS.md).

## 1. Record an exit price for EVERY closure (biggest gap)

40% of entered signals (hit ≥1 limit) have no reconstructable outcome: 90 hit-then-cancelled signals carry zero exit information, and 102 manual profits mostly lack a price. Fix:

- On **any** status change of a signal with `limits_hit > 0` (manual cancel, news cancel, expiry, breakeven, `!setstatus`), snapshot bid/ask from `live_prices` into new columns `close_bid` / `close_ask` / `close_price_feed` on `signals`. This is one read of a table the bot already maintains — no new feed work.
- Then a cancelled-after-hit signal has a mark-to-market P&L and the "true expectancy" question closes.

## 2. Persist config-at-time (TP threshold drift broke history)

Toll-metals TP changed $4→$5 on 2026-06-06, creating a ±10R band on 4 months of results because old exits are modeled with today's config.

- Stamp `tp_threshold_used` (value + unit, resolved via TPConfig) onto the signal row at creation, and again at close if it changed.
- Same for alert distance and NM config if they'll ever be tuned. Alternative: append every `!tp set` / `!alertdist set` / `!nmconfig set` to a `config_history` table (timestamp, key, old, new) — cheaper and covers all knobs at once.

## 3. Fix and extend excursion tracking

- **Bug**: `approach_velocity` and `pre_hit_mae` have unit-scale outliers (values ~20,000× typical) on some rows — likely pip-size or elapsed-time division error in `price_feeds/excursion_monitor.py`. Audit before the SL re-sim.
- **Sequencing**: record `mae_before_mfe BOOLEAN` (or timestamps of running max/min updates — `mfe_time`/`mae_time` exist but per-final-extreme only, not "which extreme came first past a threshold"). Without ordering, break-even-move rules can't be evaluated.
- **Post-exit continuation**: keep sampling for N minutes after TP/close and record `post_exit_mfe_pips` — directly measures pips left on the table beyond the current runner logic.
- **Post-cancel counterfactual**: after a near-miss / news / spread-hour cancel, watch the level for 60–120 min and record would-have-hit / would-have-won. This is the only way to know whether NM cancels (387 so far) are saving or costing money.
- Oil is excluded (no MT5 bars in main process) — the Exness worker already polls ticks; have it stream M1 bars back over the same stdout pipe.

## 4. tp_outcomes hardening (execution bot)

- **Dedupe guard**: account 52932310 wrote 1,092 rows (mostly `trigger`) in 2 weeks — add a UNIQUE(signal_id, mt5_account, stage) index (trigger rows may need a sequence/attempt column instead of pure unique).
- **Normalize symbols**: `XAUUSDm`, `USDCAD.pro`, `USDCADp`, `.raw` suffixes fragment per-symbol cuts — store a `symbol_normalized` column resolved through the existing symbol mapper.
- **Record the user's exit config** on each final row: trailing enabled/level, partial-close %, lot mode. Right now trailing vs tp_full performance is confounded with *which users* enable trailing.
- **Slippage**: store intended price vs actual fill on entry and exit (`entry_slippage_points`, `exit_slippage_points`). The 88.5%→59% win-rate gap between tracking and execution is unexplained in detail; slippage/spread is the prime suspect and is currently invisible.
- Add `account_equity` (or risk-money) at placement so per-user results can be weighted and "closed laptop mid-run" accounts detected (position present with no bot heartbeat).

## 5. Market-condition context at signal time

Aimed at the found weaknesses (8–9 AM ET, Thursdays, June regime shift), keep it cheap — one row per signal at creation:

- Minutes until next high-impact calendar event for the signal's currency (news manager already fetches the calendar — just compute the delta).
- ATR percentile vs trailing 30 days (regime), realized 5-min volatility.
- Session + day-of-week (derivable, but storing avoids timezone mistakes in later analysis).
- Level metadata from the provider side if available: how many times the level was touched before, age of the level, distance to round number. These directly test the "levels get weaker with depth" mechanism (limits_hit ≥ 4 → negative expectancy).

## 6. Strategy/product changes suggested by the data (decide, then implement)

- Execution bot: default `trailing` ON; default-skip signals with 6+ limits (or auto-halve risk); per-type risk multipliers in `lot_sizing` config with `standard`/stocks defaulting low.
- Server bot: consider a `!report` warning band when weekly R < 0 two weeks running (June stall was only visible in hindsight).
- Signal intake: 19-limit and 10+ limit signals should probably be rejected or truncated at parse time — they are negative-expectancy as a class.

## 7. Analyst access & repeatability

- The `execution_bot_ro` role lacks SELECT on `signal_excursions`, `signal_volume_samples`, `status_changes`, `performance_metrics`, `trailing_simulations` — this analysis had to use the owner DSN. Create a proper read-only `analyst` role with SELECT on all tables.
- The cleaning rules from this analysis (core = automatic closes with ≥1 hit; exclusion lists; $4/$5 sensitivity) live in STRATEGY_ANALYSIS.md §1 — reuse them so future numbers are comparable.

## Indicator wishlist — deliberately short

More indicators ≠ more insight at n≈350 closes; the ones above are tied to observed effects. If adding pure indicators anyway, in priority order: distance to session VWAP, round-number proximity, prior-touch count of the level, H1 swing-structure direction (replaces the noisy `htf_trend`), tick-volume delta over the approach window. Skip generic RSI/EMA additions — the existing `rsi_at_hit`/`ema_distance_atr` showed no separation on 46 entered signals.
