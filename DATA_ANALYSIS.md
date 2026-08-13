# DATA_ANALYSIS.md — how to analyze this system's data

Context file for future analysis sessions. Read this before querying anything. Written 2026-07-12, the day the clean-data instrumentation shipped.

---

## 1. The strategy (what the data describes)

Signals are **level-bounce mean-reversion trades**: a provider posts limit orders at high-probability price levels with one shared stop-loss. Price is expected to react off the level fast — winning trades resolve in a **median of ~3 minutes** after first fill (tolls: under 1 min). TP is a fixed threshold beyond the last filled limit (auto-TP fires when the last hit limit's P&L ≥ threshold AND earlier limits sum ≥ 0). Win rate is high (~88% on tracked auto-closes) but wins are small (~+0.21R) and stop-outs large (~−0.90R), so expectancy is thin and data quality matters.

Signal types: `standard, scalp, swing, toll, pa, 1-1, risky`. **Toll (gold levels on XAUUSD) has been the profit engine**; `standard` has been ~break-even-negative; more limits on a signal = worse R (win-size compression: TP is fixed off the last limit while each extra limit extends the risk).

Two bots write to this DB:
- **TM Bot** (this repo): parses Discord signals, tracks prices, fires alerts, records signal lifecycle + analytics (`signals`, `limits`, `status_changes`, `signal_excursions`, `signal_volume_samples`, `trailing_simulations`, `config_history`, `live_prices`, `feed_health`, `bot_mode_status`).
- **MT5 Execution Bot** (`C:\Personal Projects\MT5-Auto-Execution-Bot`): individual users' bots that place real trades from these signals; writes `tp_outcomes` (real fills, exits, R-multiples, slippage, equity). Multiple accounts per signal → **always deduplicate to unique signals** or aggregate per-signal (median across accounts) before drawing conclusions.

## 2. Data eras — THE most important filter

`signals.data_version`:
- **1** = created before 2026-07-12. Known problems: no exit price on manual/cancel closes, TP-config drift unrecorded (toll-metals TP was $4 before 2026-06-06, $5 after), excursion pip-size bug (fixed + backfilled, see §6), assorted manual corrections.
- **2** = created 2026-07-12 or later, with full instrumentation.

**Primary analysis uses `data_version >= 2` only.** Era-1 data may be referenced for context but not for optimization decisions.

One wrinkle inside era 2: the column defaults to 2, so the ~28 signals created on **2026-07-13 before the instrumentation code was deployed that day** are marked era 2 but carry no `tp_threshold_used` / `tp_threshold_unit`. Add `AND created_at >= '2026-07-14'` whenever the analysis needs the config-at-time stamps; every signal from that date on has them. The 2026-07-11 full analysis of era-1 data (+19.7…+29.3R over 4.3 months, toll/gold = the edge, 6+ limit signals negative, 8–9 AM ET and Thursdays weak, trailing > fixed TP in execution) lives in git history — `git show 642f4d2:STRATEGY_ANALYSIS.md`.

## 3. Outcome definitions and exit-price rules

A signal "entered" iff `limits_hit > 0` (equivalently: any `limits` row with status `hit`). Non-entered signals are not trades.

Exit price per close type (era 2):
| Close | How to get exit price | Notes |
|---|---|---|
| `profit` / `closed_reason='automatic'` | `tp_price` | Auto-TP close price |
| `profit` / `closed_reason='manual'` | **`tp_price`** (live bid for long / ask for short, captured at command time) | `manual_tp_price` is a retrospective override ("where it should have TP'd") — **advisory only, never the analysis exit**. Compare the two to detect TP thresholds set too far. Manual profit = whole position closed at that price (no trailing assumption). |
| `stop_loss` | `stop_loss` column | |
| `breakeven` | mid of `close_bid`/`close_ask` | Typically a small loss; treat as its actual (small) P&L, not 0, when close prices exist. Two populations since 2026-08-13: hand-marked (`be` reply) and **breakeven-stop** closes, which carry `be_stop_armed_at IS NOT NULL`, `closed_reason='automatic'`, a `status_changes.reason='breakeven_stop'` row, and `signal_excursions.exit_reason='be_stop'`. Split them — the second is a rule, not a judgement call. |
| `cancelled` with hits | mid of `close_bid`/`close_ask` | Mark-to-market at cancel time — no more unknown outcomes |
| any close without hits | not a trade | |

`close_bid` / `close_ask` / `close_feed` are stamped on **every** terminal transition of an entered signal (auto-TP, SL, NM, news/spread cancels, manual commands, expiry, message-delete cancel).

**Exclusions:**
- Signals whose `status_changes` include `cancelled → profit` (manual retro-correction; happens when price came within points of limit 1 and some users' bots filled but the tracker didn't): **exclude from TM-side profitability**. They still legitimately appear in `tp_outcomes` (those users really traded them).
- Signals reactivated after cancel (`status_changes` has `cancelled → active/hit`): analyze with care; excursion rows reset on re-approach.

**R-multiple convention** (matches prior analyses): 1R = full-fill risk = Σ|limit_i − stop_loss| over ALL limits. Per-signal `rho = Σ(exit − limit_i over HIT limits, signed by direction) / 1R`. "Total" sizing R = rho (stop-out = −1R). "Fixed" sizing R = N·rho. Total + skip 6+ limit signals was the best risk-matched scheme on era-1 data.

## 4. Config-at-time

- `signals.tp_threshold_used` / `tp_threshold_unit` — the TP threshold resolved for this signal at save time. Use this, never today's `tp_configuration.json`, for historical modeling. **Ignore it when `take_profit` is non-NULL** — that signal exits at its own price and the threshold never applied to it.
- `signals.take_profit` — the sender's fixed TP price (instant-entry signals only; NULL everywhere else). These signals also enter at market rather than at a limit, so their single `limits` row records the fill price, not a resting order. Filter on `take_profit IS NOT NULL` to separate them from limit-based PA signals, which share `type = 'pa'`.
- `signals.minutes_to_news` — minutes until the next news event affecting this instrument at save time (NULL = none upcoming/unknown).
- `config_history` — every `!tp set/remove`, `!alertdist set/remove`, `!nmconfig set/remove`, `!goldtollssl`, `!riskygoldsl` change with old/new values, timestamp, who.

## 5. Excursion layer (`signal_excursions`, one row per signal)

Tracks from first approaching alert: approach velocity, pre-hit MAE, then from entry (first hit limit): MFE/MAE in pips + ATR multiples, entry context (ATR, RSI, EMA-distance, wick, H1 trend, volume spike, spread, session). Oil gets MFE/MAE but no bar-derived context (no MT5 bars for Exness symbols).

New in era 2:
- `mae_before_mfe` — TRUE if the adverse excursion crossed 25% of the TP threshold before the favorable one did. This is the key input for evaluating break-even-move rules (era-1 data couldn't order the extremes).
- `post_exit_mfe_pips` / `post_exit_end_time` — favorable follow-through beyond the exit price for 60 min after close (M1 bars). Directly measures pips left on the table; compare against `tp_threshold_used` to size trailing benefit.
- **`entry_price` is the FIRST fill; TP and trailing anchor on the DEEPEST fill.** `mark_entry` takes the lowest-`sequence_number` hit limit, while auto-TP measures the last hit limit's P&L and `trailing_monitor._deepest_hit_price` anchors on the deepest one (long: lowest hit price, short: highest). On a multi-fill signal these are far apart and **MFE/MAE are not comparable to the signal's realized P&L or to R**. Worked example — signal 3476 (US30USD short, 6 fills): excursion entry 52982.3 with `mfe_pips` = 6, yet auto-TP fired at 53041.3 for a real profit measured off the 53194.3 fill. The row is correct; reading its MFE as "the trade only ever went 6 pips my way" is not. For per-signal excursion work either restrict to `limits_hit = 1`, or recompute the anchor from `limits` (`MIN/MAX(hit_price)` by direction) and shift `mfe_pips`/`mae_pips` by `(anchor − entry_price)/pip_size`.
- **`exit_reason` ending in `:reconciled`** means the exit was *derived* at close time, not observed on a tick. A background reconciler (15 min cadence, `ExcursionDatabase.close_orphaned`) closes any row whose signal reached a final status without a live path finalizing it — expiry, message-deletion, or a manual command issued after the signal left the monitor's in-memory set. The exit price follows the §3 rules (stop level for SL, else `tp_price`, else the close-snapshot mid); the reason is the signal's `closed_reason` plus the suffix. Treat `mfe_pips`/`mae_pips` on these rows as lower bounds — a row only needs reconciling because the in-memory state was gone, and ratcheting stops with it. Before the reconciler shipped (2026-08-11) these rows stayed in `approach`/`in_trade` indefinitely; 61 historical rows (5 of them entered signals) were closed by the first sweep. Rows finalized on a live path re-assert their in-memory extremes at close, so a dropped intermediate ratchet write no longer sticks.
- **Pips are pips of the row's own `pip_size`** — always multiply by `pip_size` to get price units before cross-instrument comparison. A 2026-07-12 backfill rescaled rows written with wrong pip sizes (DE30EUR, GCQ26, USOILSPOT, all stocks). If the bot ran with pre-fix code after the backfill, re-run the rescale (idempotent, keyed on `pip_size <>` expected; template in git history / trivial to reconstruct — factor = old_pip/new_pip on `approach_velocity, pre_hit_mae, mfe_pips, mae_pips`). Snapshot table `signal_excursions_backup_20260712` holds pre-backfill values.
- Canonical pip sizes: forex 0.0001 (JPY pairs 0.01), XAU/GC 0.01, XAG 0.001, BTC 1.0, other crypto 0.1, indices 1.0, oil 0.01, stocks 0.01.

`signal_volume_samples`: per-minute price/volume/ATR for ≤90 min per signal — for volume-turns-against-us exit research (still unanswered).

`trailing_simulations`: shadow what-if trailing at three distances (tight = 0.5× TP value, medium = 1.0×, loose = 2.0×), armed at the auto-TP price. As of 2026-07-12: n=54, all levels profitable, medium marginally best but not statistically separable; real execution data says trailing exits beat fixed TP (+0.253 vs +0.127 avg R). **Open question: which trailing distance is best — needs a few hundred more sims.** TM Bot surface metrics (`!report`, embeds) intentionally stay fixed-TP; the execution bot defaults to trailing.

## 6. tp_outcomes (execution bot; real-world truth)

Unit = (signal × account × stage). `stage='final'` is the source of truth (one per signal per account, SQLite-guarded); `stage='trigger'` = state at TP fire, deduped per fill depth from v1.6.0. Rows before 2026-06-15 don't exist; rows before v1.6.0 lack the new columns.

Era-2 additions (v1.6.0+): `symbol_normalized` (broker suffixes stripped — group by this, not `symbol`), `account_equity`/`account_balance` (weight users, detect glitched accounts), `entry_slippage_points`/`exit_slippage_points` (adverse-positive, broker points; NULL exit slippage = broker-side SL/trailing exit, not zero), `notes` jsonb on finals = resolved exit config (threshold, trailing distance, partial-close %, lot mode) so trailing-vs-fixed comparisons are no longer confounded by which users enabled what.

Cleaning rules that mattered on era-1 data (keep applying):
- Screen |r_multiple| > 5 or < −2 (user glitches: closed laptops, manual MT5 interference).
- One signal can appear under 30+ accounts — per-signal aggregation first.
- `signal_type` is stamped at placement; `risky` exists here but rarely in TM analysis sets.

## 7. Access + egress etiquette

- Read-only analysis role: `analyst_ro` / password in the owner's records (created 2026-07-12; SELECT on all public tables incl. future ones). Owner DSN lives in this repo's `.env` (line `SUPABASE_DB_URL`); asyncpg needs `statement_cache_size=0` only on pooler port 6543, not the direct 5432 connection.
- **Supabase egress is near its limit.** For analysis: pull tables once into local pandas/pickle, don't re-query in loops. Never add bot code that polls new tables or fattens the per-cycle fetch materially — the exec bot's per-cycle Supabase reads are the main egress driver.

## 8. Open questions for the next analysis (in priority order)

1. **True expectancy on era-2 data** — first time every entered signal has an exit price. Does the toll/gold edge hold? Is `standard` still negative? (Era-1 gave toll +0.167 avg R, standard −0.024.)
2. **Trailing distance choice** — trailing_simulations + `post_exit_mfe_pips` + tp_outcomes `notes` (now records each user's trailing config). Decide tight/medium/loose; consider migrating TM auto-TP to trailing if data supports (user is open to it).
3. **Break-even-move rules** — now answerable with `mae_before_mfe` + MFE/MAE magnitudes.
4. **SL re-sim at ATR multiples** — was blocked on n (33 winners with MAE); check `signal_excursions` count first, need ~200+ entered signals.
5. **manual_tp_price vs tp_price gap** — if manual overrides consistently sit below tp_price, TP thresholds are too far; per type/asset.
6. **Slippage cost** — entry/exit slippage vs the modeled edge (~0.08R avg — slippage could eat a big share).
7. **6+ limit skip validation** — exec bots now skip them by default (`skip_limits_at=6`); compare filled depth distributions before/after v1.6.0.
8. **Session/day effects** — era-1 found 8–9 AM ET negative and Thursday negative; re-test on era 2 before acting.
9. **NM-cancel counterfactuals** — still NOT instrumented (deferred); the 387+ near-miss cancels remain unevaluated. Era-2 adds 53 more. The excursion row for a near-miss closes at `approach` phase with no exit price (nothing was entered), so nothing records whether price went on to reach limit 1 after the cancel. Answering this needs new instrumentation: a bounded post-cancel watch on non-entered closes, mirroring `post_exit_mfe_pips`.

## 9. Schema quick-reference (analysis-relevant deltas from CLAUDE.md)

- `signals`: + `close_bid, close_ask, close_feed, tp_threshold_used, tp_threshold_unit, minutes_to_news, data_version`
- `signal_excursions`: + `mae_before_mfe, post_exit_mfe_pips, post_exit_end_time`
- `config_history`: new (family, key, old/new value, set_by, changed_at)
- `tp_outcomes`: + `symbol_normalized, account_equity, account_balance, entry_slippage_points, exit_slippage_points`; finals carry config snapshot in `notes`
- `performance_metrics`: **dropped** (was never written)
- Table merges were considered and rejected: `signal_excursions` stays 1:1-joined to `signals` on `signal_id` (JOIN is trivial; merging would bloat the hot `signals` table the exec bot polls — egress).
