# TM Bot — Codebase Orientation

## Summary

The bot monitors trading-signal messages in designated Discord channels, parses them into structured position data (instrument, direction, entry limits, stop-loss), stores them in PostgreSQL on Supabase, and streams real-time prices from four feeds (ICMarkets/MT5, OANDA, Binance, Exness/MT5) to fire approaching/hit/stop-loss/auto-TP/near-miss alerts. All events edit a single persistent Discord embed per signal to keep channels tidy. A command suite (`!cancel`, `!setstatus`, `!tp`, `!news`, etc.) manages signal lifecycle and configuration at runtime without restarts.

---

## How To Work In This Repo

- **Commit by default** — after completing changes, commit and push to the current branch without asking. Only ask before committing if the change is destructive or you are unsure about correctness.
- **Commit style** — one short line, plain English, imperative mood ("Fix X", not "Fixed X"), no version labels ("Stage 19", "V2"), no decision refs, no parenthetical explanations. State *what* changed in as few words as possible. Good: `"Fix SL offset on offset instruments"`. Bad: `"Stage 19 fix: SL offset (decision 12) — adj_sl now includes offset for SPX/NAS/BTC/ETH"`.

---

## Technology Stack

- **Language**: Python 3.9+ (Windows required for MetaTrader5)
- **Discord**: discord.py 2.3.0+, `commands.Bot` with cog extensions
- **Database**: PostgreSQL via Supabase; `asyncpg` pool (min 2, max 10 connections, 30 s timeout)
- **Price Feeds**: ICMarkets/MT5 (polling 100 ms/symbol), OANDA (REST streaming), Binance (WebSocket bookTicker), Exness/MT5 (child-process polling for oil)
- **Models**: Pydantic v2 for domain objects (`SignalData`, `LimitData`, `BotSettings`); attribute access only (the transitional dict-protocol was removed in stage 20)
- **AI Fallback Parser**: OpenAI `gpt-4o-mini`; disabled by default (`enable_openai_fallback: false` in `settings.json`)
- **Config**: `.env` for secrets; JSON files in `config/` for runtime settings; Pydantic validation on load

---

## Project Structure

```
main.py                         Entry point; loads .env, creates TradingBot, runs asyncio.run(main())

models/
  __init__.py                   Re-exports SignalData, LimitData, BotSettings, enums
  enums.py                      SignalStatus, LimitStatus, ChangeType, Direction (str enums)
  signal.py                     SignalData, LimitData (Pydantic); computed pending_limits/hit_limits;
                                  from_db_row() handles id→signal_id normalization
  config.py                     BotSettings, SpreadBufferConfig, ChannelSettings, ThresholdEntry (Pydantic)

core/
  bot.py                        TradingBot(commands.Bot); wires all subsystems in setup_hook();
                                  creates all price_feeds subsystems and injects into StreamingPriceMonitor;
                                  admin_ids + health_alert_admin_id read from settings.json; ServiceRegistry populated here
  services.py                   ServiceRegistry — typed container for subsystem references;
                                  replaces bot.monitor.X.Y reach-through coupling
  expiry_manager.py             @tasks.loop(5min) — expires ACTIVE/HIT signals past expiry_time;
                                  updates embeds; uses react_to_original_signal() from streaming_monitor
  news_manager.py               Tracks active news windows; persists to data/news_events.json;
                                  cleanup polls every 30 s; parse_news_command() parses !news args
  channel_cleaner.py            @tasks.loop(1min) — every Friday 18:00 local time, bulk-deletes
                                  the past 7 days of messages from every alert channel AND every
                                  monitored channel (except `price-action-trades`); preserves
                                  messages tied to ACTIVE/HIT signals. Idempotent via _last_purge_date
  parser/
    __init__.py                 parse_signal(message, channel_name) entry point;
                                  ParsedSignal / RejectedSignal types; lazy sub-parser init
    pattern_parsers.py          CorePatternParser / StockPatternParser / CryptoPatternParser;
                                  parse_instant_signal() — market-entry signals (instrument + direction
                                  + labelled sl/tp, no limits) for instant-entry channels;
                                  CHANNEL_TYPE_MAP + get_signal_type() (channel → standard/scalp/swing/toll/pa/1-1);
                                  get_gold_tolls_sl_offset() (settings.json, 30 s cache)
    validators.py               is_potential_signal(), should_exclude(), validate_signal(),
                                  detect_channel_type()
    ai_fallback.py              AIFallbackParser (gpt-4o-mini); only runs if pattern fails + flag enabled

database/
  __init__.py                   Loads .env; exposes global db (DatabaseManager) and initialize_signal_db()
  connection.py                 asyncpg pool; execute/fetch helpers; params as $1,$2,... positional args
  manager.py                    DatabaseManager — extends connection with core signal/limit ops:
                                  update_signal_status (validates transitions), mark_limit_hit,
                                  get_active_signals_for_tracking (returns List[SignalData]),
                                  get_hit_limits_for_signal, expiry ops, bot_mode_status
  schema.py                     DDL for all tables + indexes + idempotent migrations
  report_queries.py             Read-only reporting/presentation queries (!active rows, !report
                                  period queries, statistics) — returns plain dicts, not models
  signal_ops.py                 SignalDatabase — CRUD + lifecycle operations;
                                  get_signal_with_limits() returns Optional[SignalData];
                                  save_signal, cancel, reactivate, manually_set_signal_status,
                                  process_limit_hit, expire_old_signals;
                                  add_instant_entry() — appends a second already-filled market entry
                                    to an instant signal (MAX_INSTANT_ENTRIES, row-locked);
                                  get_overlapping_signals() — range-intersection query used on save;
                                  check_reactivation_guard() — compares cancelled limits vs live price;
                                  _get_live_price() — reads bid/ask from live_prices table
  utils.py                      calculate_expiry() (day_end → 4:45 PM EST), _parse_dt()

price_feeds/
  feeds/                        Feed clients + stream coordination
    price_stream_manager.py     Coordinates all four feeds; calculates spread if missing (ask − bid);
                                  stamps price_data["updated_at"] (UTC) from broker tick time (ICMarkets/Exness)
                                  or wall-clock (OANDA/Binance) before calling subscribers;
                                  routes symbols to feeds via SymbolMapper; notifies all subscribers
    icmarkets_stream.py         MT5 polling 100 ms/symbol — Windows only;
                                  tick "timestamp" is UTC-aware (datetime.fromtimestamp(tick.time, tz=utc));
                                  on_poll callback fires on every successful tick fetch (price-change-agnostic)
                                  so the health monitor's last_seen timer doesn't age during quiet markets
    oanda_stream.py             OANDA REST stream; live/practice via OANDA_PRACTICE env var
    binance_stream.py           WebSocket bookTicker; US (binance.us) or international via BINANCE_USE_INTERNATIONAL
    exness_stream.py            Spawns exness_worker.py as child process; reads JSON-line stdout;
                                  same duck-typed interface as other feeds (connect/subscribe/stream_prices)
    exness_worker.py            Standalone child process; holds its own MT5 connection to Exness terminal;
                                  polls symbol_info_tick() at 100 ms; writes JSON price lines to stdout;
                                  reads subscribe/unsubscribe/shutdown commands from stdin
  alerting/                     Persistent embed orchestration + archiving
    alert_system.py             Persistent embed orchestrator; 4 data dicts + _live_embeds;
                                  5-channel routing; bounded sequential live-refresh passes;
                                  hydrate_from_db / recover_pending_archives / recover_finished_embeds
                                  re-attach existing embeds on restart; retract_approaching_embed
                                  drops the embed when price drifts back past the alert window;
                                  delegates embed building and archiving
    embed_builders.py           Pure functions: _build_signal_embed(), _build_profit_archive_embed(),
                                  _set_archive_footer() + formatting helpers
    archive_manager.py          ArchiveManager — schedule_end_state_move(), cancel_pending_move(),
                                  delayed move/delete for finished signals
    info_embeds.py              InfoEmbedManager — static per-channel info/risk embeds;
                                  message IDs persisted to data/info_embeds.json
  monitors/                     Per-tick evaluation, health, and market guards
    streaming_monitor.py        Per-tick signal evaluation; receives all deps via constructor;
                                  react_to_original_signal() as module-level function;
                                  _MAX_TICK_AGE_SECONDS = 5 — drops stale ticks before signal evaluation;
                                  refresh_signal_in_memory(signal_id) — re-fetches a single signal
                                  from DB and syncs active_signals + alert_system._live_embeds
                                  (called from edit / reactivate paths to avoid 30 s drift);
                                  check order documented below
    tp_monitor.py               AutoTPMonitor — checks HIT-status signals per tick; per-signal limit cache
    nm_monitor.py               NearMissMonitor — in-memory NMTrackingState; _nm_immune set;
                                  mark_immune(signal_id) called on every reactivation;
                                  signal.type == "swing" short-circuits NM (swing is not near-missable)
    trailing_monitor.py         TrailingStopMonitor — trailing-stop evaluation for HIT signals
    excursion_monitor.py        ExcursionMonitor — MFE/MAE tracking + post-exit follow-through sampling;
                                  run_reconciler() closes rows the live paths missed (15 min)
    feed_health_monitor.py      Stale threshold 300 s; 3 max reconnect attempts; 120 s startup grace;
                                  15 min alert cooldown; DMs health_alert_admin_id from settings.json;
                                  spread hour (17–18 ET) treated as market-closed for forex/metals/indices/oil;
                                  first_stale_time tracks real stall start for accurate recovery downtime;
                                  reconnects only the stale feed (reconnect_feed), never all feeds;
                                  price-flow watchdog force-restarts the bot when a subscribed market is
                                  open but no feed has ticked for WATCHDOG_SILENCE_SECONDS;
                                  all knobs are module-level constants (no config file)
    vol_guard.py                VolatilityGuard — per-instrument volatility flag across forex/gold/indices/
                                  crypto/oil/stocks; reconciles vol_guard bot_mode_status + one live embed
    risky_window.py             RiskyWindowAnnouncer — risky-window gating + is_risky_trading_disabled()
    market_context.py           MarketContextProvider — market session / open-state context
    live_price_writer.py        Writes bid/ask/feed to live_prices table every 5 s for every signal-bearing
                                  symbol (ICMarkets/OANDA/Binance/Exness); the EX bot derives its own broker
                                  offset from its MT5 feed vs the stored price, so no IC reference price is written
  config/                       Threshold configs + symbol mapping
    _base_config.py             BaseThresholdConfig — shared JSON load/save/validate/override management;
                                  shared SymbolMapper singleton across all config types
    alert_config.py             AlertDistanceConfig(BaseThresholdConfig) — approaching distance thresholds
    tp_config.py                TPConfig(BaseThresholdConfig) — per-signal-type TP thresholds (type_defaults / type_overrides);
                                  calculate_pnl(); accepts signal_type kwarg (or legacy scalp bool)
    nm_config.py                NMConfig(BaseThresholdConfig) — max_proximity + base_bounce per asset class
    trailing_config.py          TrailingStopConfig(BaseThresholdConfig) — trailing-stop LEVELS per asset class
    symbol_mapper.py            Internal ↔ feed-specific symbol translation; always returns UPPERCASE

discord_handlers/
  message_handler.py            Handles new/edited/deleted messages; dispatches reply commands for
                                  both alert embeds + pings (any user) and signal messages
                                  (author/admin only); bot reply messages and the user's trigger
                                  reply are auto-deleted after _REPLY_DELETE_AFTER (15 s) to keep
                                  monitored / alert channels tidy;
                                  _handle_overlap_prompt() — 30 s reaction prompt when new signal overlaps
                                  an existing one (✅ cancel old / ❌ keep both / timeout = cancel old);
                                  _reply_breakeven_stop() — `set be` / `unset be` arms or clears a
                                  signal's breakeven stop (protection, not a status change);
                                  _reply_add_entry() — `add` averages a second market entry into an
                                  instant-entry signal (see Instant-entry channels)

commands/
  __init__.py
  _base.py                      BaseCog: is_admin(), is_command_channel(), get_channel_name();
                                  stores self.services = bot.services for cog access
  views.py                      Discord UI views (confirmation buttons, etc.)
  admin/
    __init__.py                 Cog setup: BotManagementCog + LicenseCog
    bot_management.py           !help, !health, !admin, !cleanalerts, !goldtollssl
    license.py                  !activate, !setkeys, !grantkey, !revoke, !licenses + member listeners
  signals/
    __init__.py                 Cog setup: LifecycleCog + ReportsCog + NewsCog
    lifecycle.py                !active (default sort: distance), !info, !setstatus [--force],
                                  !profit, !hit, !stoploss/!sl, !cancel/!nm (+ bulk), !setexpiry
                                  (breakeven only via !setstatus <id> breakeven — no shortcut)
    reports.py                  !report (performance statistics generator; partitions into
                                  Regular / PA / Legends by channel name)
    news.py                     !news, !newslist, !newsclear
  config/
    __init__.py                 Cog setup: ThresholdsCog
    thresholds.py               !tp (per-type targeting: `!tp set <type> <asset_class> <value>`,
                                  `!tp set <symbol> <value> [--type=X]`, `!tp config <type|symbol>`,
                                  `!tp remove <symbol> [--type=X]`); !alertdist, !nmconfig

utils/
  logger.py                     Non-blocking queued console/file logging; rotating bot.log
                                (10 MB×5) + errors.log (5 MB×3); LOG_LEVEL env; UTF-8
  discord_http_trace.py         Under DEBUG, safely logs Discord rate-limit headers for each 429
  config_loader.py              ConfigLoader class + load_settings() → BotSettings, save_settings(),
                                  load_channels_config()
  formatting.py                 Price/pip/distance formatting; get_channel_name(); get_status_emoji();
                                  format_signal_type() (SIGNAL_TYPE_LABELS map)

config/                         Runtime JSON configs (see Configuration Files section)
data/                           Auto-generated runtime data (news_events.json)
```

---

## Data Flow: Signal Lifecycle

```
Discord message posted in monitored channel
  │
  ▼
bot.on_message() → message_handler.handle_new_message()
  │
  ▼
parse_signal(text, channel_name)
  ├─ pattern parsers (CorePatternParser / StockPatternParser / CryptoPatternParser)
  └─ AI fallback if pattern fails AND enable_openai_fallback=true
  │  Returns ParsedSignal or falsy (RejectedSignal / None)
  │
  ▼
signal_db.save_signal(parsed_signal, message_id, channel_id)
  ├─ Atomic: INSERT signals row (status=active) + INSERT limits rows
  └─ If same message_id exists and is cancelled → reactivate instead
  │  Returns (success, signal_id)
  │
  ▼
Bot adds ✅ reaction to original message
  │
  ▼
Signal added to streaming_monitor active set (refreshed every 30 s + immediate on insert)
  │
  ├─ APPROACHING: price enters alert threshold
  │    → alert_system.send_approaching_alert()
  │    → creates persistent embed; sets approaching_alert_sent=True; NM tracking begins
  │
  ├─ HIT: price crosses limit (spread buffer applied if enabled)
  │    → mark_limit_hit() → signal: active→hit on first limit
  │    → alert_system.send_limit_hit_alert() → edits embed; sends ping
  │    → AutoTPMonitor begins checking this signal per tick
  │
  ├─ STOP LOSS: price crosses SL (no spread buffer)
  │    → alert_system.send_stop_loss_alert() → edits embed; signal→stop_loss
  │
  ├─ AUTO-TP: last hit limit P&L ≥ threshold AND earlier limits sum ≥ 0
  │    → alert_system.send_auto_tp_alert() → edits embed; signal→profit
  │
  ├─ NEAR-MISS: price approached limit but bounced without hitting
  │    → nm_monitor triggers cancel → alert_system.send_near_miss_cancel_alert()
  │    → edits embed to cancelled
  │
  ├─ MANUAL COMMANDS (!profit, !cancel, reply to embed/message)
  │    → DB status change → update_embed_for_signal_id() or update_signal_message()
  │
  └─ EXPIRY (every 5 min): signal past expiry_time
       → expiry_manager → cancel → update_embed_for_signal_id()
```

---

## Data Flow: Price Tick

Every incoming price update calls `streaming_monitor._on_price_update()` → `_check_signal()` for each ACTIVE/HIT signal on that symbol. Exact check order:

```
0. Tick-staleness gate (in _on_price_update, before per-signal checks)
   price_data["updated_at"] set by price_stream_manager (UTC broker timestamp for ICMarkets/Exness,
   wall-clock for OANDA/Binance). Ticks older than _MAX_TICK_AGE_SECONDS (5 s) are dropped
   silently at DEBUG level. Spread-hour transition tracking still runs on stale ticks.

1. Spread-hour gate
   _is_spread_hour(): America/New_York, 17:00–18:00, weekdays only
   └─ Signal would trigger → send_spread_hour_cancel_alert()
      Edits embed if one exists; no standalone if embed already present.

2. News-mode gate / client-only dry run
   news_manager reconciles every active category into bot_mode_status.news_mode
   for client bots. Normal events also guard alert-bot signals; events created
   with the `dryrun` option pause clients while alert-bot processing continues.

3. Approaching check
   First pending limit only (lowest sequence_number, approaching_alert_sent=False)
   Distance ≤ alert_config threshold → send_approaching_alert()
   Spread buffer: affects display price for approaching + hit (NOT stop-loss).

4. Hit check
   Price at or past limit (long: price ≤ limit; short: price ≥ limit)
   Spread buffer optionally widens comparison window.
   → mark_limit_hit() → send_limit_hit_alert()
   Signal: active→hit on first limit hit.

5. Stop-loss check
   No spread buffer — exact price comparison only.
   → send_stop_loss_alert() → signal→stop_loss

6. Near-miss check
   Only if approaching_alert_sent=True and signal not NM-immune.
   nm_monitor.update(signal, current_price) → True if bounce confirmed.
   → cancel signal → send_near_miss_cancel_alert()

7. Breakeven-stop check (HIT-status signals only, and only once armed)
   Runs after the stop loss so a tick gapping through both books the real SL.
   _check_breakeven_stop(signal, bid, is_spread_hour) — on the bid, no spread buffer
   Skipped for non-crypto during spread hour, like the SL — re-evaluated after.
   → send_breakeven_stop_alert() → signal→breakeven

8. Auto-TP check (HIT-status signals only)
   tp_monitor.check_signal(signal, bid) — no spread buffer; evaluated on the bid
   last_hit_limit_pnl ≥ threshold AND sum(earlier_limits_pnl) ≥ 0 (ε=1e-9)
   → send_auto_tp_alert() → signal→profit
```

---

## Database Schema

All PKs: `BIGINT GENERATED ALWAYS AS IDENTITY`. Timestamps: `TIMESTAMPTZ`. RLS: deny-all for anon/authenticated; full access for service_role. Use Supabase **Session Pooler** (port 5432).

### signals
| Column | Notes |
|--------|-------|
| id | PK — normalized to `signal_id` by `SignalData.from_db_row()` |
| message_id | TEXT UNIQUE; `manual_xxx` for manually entered signals |
| channel_id | TEXT |
| instrument | TEXT (e.g. GBPUSD, XAUUSD, BTCUSDT) |
| direction | TEXT (`long` / `short`) |
| stop_loss | DOUBLE PRECISION NOT NULL |
| expiry_type | TEXT (`day_end` / `week_end` / `month_end` / `no_expiry`) |
| expiry_time | TIMESTAMPTZ; `day_end` resolves to 4:45 PM EST |
| status | TEXT; CHECK (active, hit, profit, breakeven, stop_loss, cancelled) |
| type | TEXT DEFAULT 'standard'; CHECK (standard, scalp, swing, toll, pa, 1-1, risky) |
| first_limit_hit_time | TIMESTAMPTZ |
| closed_at / closed_reason | TIMESTAMPTZ / TEXT (`automatic` / `manual` / `expiry`) |
| be_stop_armed_at | TIMESTAMPTZ (nullable); when a breakeven stop was armed via the `set be` reply. NULL = not armed. A timestamp rather than a flag so analysis can see how far into a trade protection went on. |
| take_profit | DOUBLE PRECISION (nullable); the sender's fixed TP price, set only by instant-entry channels. When present it *replaces* the TPConfig threshold as the exit condition (`tp_monitor._fixed_tp_reached`, evaluated on the bid like every other TP). |
| tp_price | DOUBLE PRECISION; market close price recorded on profit (for automatic, the bid at auto-TP trigger — see "Auto-TP is evaluated on the bid"; for manual profit, the live bid/ask at command time — bid if long, ask if short). NULL for SL / cancel / breakeven / other closures, and for manual profit when `live_prices` has no row for the instrument. |
| manual_tp_price | DOUBLE PRECISION (nullable); retrospective manual TP price override set via `!profit <id> <tp_price>`. Kept separate from `tp_price` so the original recorded close is preserved. Both the `!report` P&L and the profit-archive embed (per-limit P&L + the "TP Price" field) use `manual_tp_price` when present, else `tp_price`. |
| alert_message_id | BIGINT (nullable); Discord message ID of the active-channel alert embed. Persisted so restarts can reuse the existing embed instead of orphaning it. Cleared on archive move, live-update NotFound, and approaching-alert retraction. |
| alert_channel_id | BIGINT (nullable); Discord channel ID where the alert embed lives. Used at hydration to pin the fetch to the same channel even if `channels.json` was reconfigured. Falls back to `_get_alert_channel(signal)` if missing. |
| ping_message_id | BIGINT (nullable); Discord message ID of the most recent ping reply to the embed. Refetched on restart so the next event can delete it cleanly before sending a new one. Also added to `alert_messages` on hydration so users can reply to the ping itself, not just the embed. |
| finished_message_id | BIGINT (nullable); Discord message ID of the archived embed in `finished_signals` / `profit_channel`. Persisted so reply commands against archived embeds (e.g. `reactivate`) still resolve to a signal after restart. Cleared on `reactivate_embed` (which deletes the finished message). |
| finished_channel_id | BIGINT (nullable); Discord channel ID where the archived embed lives. Used by `recover_finished_embeds` at startup to build a `PartialMessage` reference without an extra API fetch. |
| total_limits / limits_hit | INTEGER |
| close_bid / close_ask / close_feed | DOUBLE PRECISION ×2 / TEXT (nullable); live price snapshot taken by `_snapshot_close_prices` whenever a signal with ≥1 hit limit reaches ANY terminal status (auto-TP, SL, NM, news/spread, manual, expiry, message-delete). Gives every entered signal a mark-to-market exit even when `tp_price` is NULL. |
| tp_threshold_used / tp_threshold_unit | DOUBLE PRECISION / TEXT (nullable); the TP threshold resolved via TPConfig at save time — config-at-time for analysis, immune to later `!tp set` drift. |
| minutes_to_news | INTEGER (nullable); minutes until the next news event affecting this instrument at save time. |
| data_version | SMALLINT NOT NULL DEFAULT 2; data-era marker. 1 = pre-2026-07-12 (dirty era), 2 = clean instrumentation. Primary analysis filters `data_version >= 2` (see DATA_ANALYSIS.md). |

### limits
| Column | Notes |
|--------|-------|
| id | PK — `LimitData` normalizes `limit_id` → `id` via model validator |
| signal_id | FK → signals(id) CASCADE DELETE |
| price_level | DOUBLE PRECISION (entry price) |
| sequence_number | INTEGER; UNIQUE with signal_id |
| status | TEXT (`pending` / `hit` / `cancelled`) |
| hit_time / hit_price | TIMESTAMPTZ / DOUBLE PRECISION |
| approaching_alert_sent | BOOLEAN; gates NM tracking start |
| hit_alert_sent | BOOLEAN; deduplication flag |

### status_changes
Audit trail: signal_id FK, old_status, new_status, change_type (`automatic`/`manual`), reason, changed_at.

### config_history
Audit log of runtime config changes: `changed_at`, `config_family` (`tp` / `alertdist` / `nm` / `settings`), `key` (e.g. `toll/metals`, `XAUUSD`, `gold_tolls_sl_offset`), `old_value` / `new_value` (JSON text), `set_by`. Appended by the config cogs via `database/config_history_ops.py::log_config_change` (best-effort — a failed write never breaks the command).

### live_prices
`symbol TEXT PK`, bid, ask, feed, updated_at. Written by `LivePriceWriter` for every signal-bearing symbol, sourced from whichever feed serves it (icmarkets/oanda/binance/exness). Flushes every 5 s but skips unchanged prices; an unchanged price is still re-written every 30 s (heartbeat) so the EX bot's 120 s staleness gate never trips on a quiet market, and the heartbeat stops when ticks stop so a dead feed ages out honestly. The EX bot reads these prices and computes its broker offset against its own MT5 feed, so no ICMarkets reference price is stored.

### feed_health
`feed TEXT PRIMARY KEY` (`icmarkets` / `oanda` / `binance` / `exness`), `status TEXT` (`idle` / `healthy` / `down`), `stale_seconds INTEGER`, `last_seen TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`. Upserted by `FeedHealthMonitor._write_feed_health()` on status transitions (unchanged rows are refreshed at most every 10 min). Read by the EX bot each cycle to skip placement on stale feeds. `down` only fires when **every** subscribed symbol on a feed has stalled — a single quiet symbol no longer poisons unrelated signals.

### bot_mode_status
Singleton row (id=1, enforced by CHECK). `news_mode TEXT` (nullable — comma-separated active news categories like `EUR, GOLD` or `ALL`; NULL when no news), `vol_guard TEXT` (nullable — comma-separated volatile DB instruments like `EURUSD, NAS100USD, BTCUSDT`, or `ALL` for gold; NULL when calm), `spread_hour BOOLEAN`. `news_mode` is reconciled by `NewsManager.reconcile_news_mode()` (startup, every news command, and the 30 s cleanup loop); `vol_guard` is reconciled by `VolatilityGuard._reconcile_vol_guard_mode()` once per 5 s eval cycle from the active guard keys; `spread_hour` is updated in real-time by streaming_monitor on spread-hour state transitions. Consumers (including the EX bot) read `news_mode` / `vol_guard` for truthiness, so they are NULL — never the string `'FALSE'` — when inactive. `signals_rev BIGINT` is a write watermark bumped by statement-level triggers (`signals_rev_bump` / `limits_rev_bump` → `bump_signals_rev()`) on every `signals`/`limits` INSERT/UPDATE/DELETE: the EX bot polls this row once per sync cycle and refetches its heavy signal-set/status queries only when the rev moves, which is what keeps shared-pooler egress flat. The bump function swallows its own failures so it can never roll back the signal write it rides on.

### licenses / license_allowances
License management for the Signal Subscriber role. Managed via `!activate`, `!grantkey`, `!revoke`, `!licenses` commands. Not involved in signal processing.

---

## Signal Status State Machine

```
ACTIVE ──────────────────────────────────────┐
  │ first limit hit                           │ SL hit / manual cancel
  ▼                                           ▼
 HIT                                      STOP_LOSS ─┐
  │ profit / breakeven / SL / cancel                  │
  ▼                                                   │ admin correction
PROFIT ─────────────────────────────────────────────► CANCELLED
BREAKEVEN ──────────────────────────────────────────► CANCELLED
STOP_LOSS ──────────────────────────────────────────► CANCELLED
                                                          │
CANCELLED ◄──────────────────────────────────────────────┘
  │ reactivate (→ active or hit depending on limits_hit count)
  └─► ACTIVE / HIT
```

Valid transitions from `models/enums.py::StatusTransitions.VALID_TRANSITIONS`:
```python
'active':    ['hit', 'cancelled', 'stop_loss']
'hit':       ['profit', 'breakeven', 'stop_loss', 'cancelled']
'cancelled': ['hit', 'active']          # reactivation
'profit':    ['cancelled']              # admin correction only
'breakeven': ['cancelled']
'stop_loss': ['cancelled']
```

`manager.update_signal_status()` validates transitions and writes audit record.
`signal_ops.manually_set_signal_status()` bypasses validation — used by admin commands.

---

## Alert System

One persistent Discord embed per signal. Created on first approaching or hit event; all later events edit the same message. Each event also sends a new reply ping (old ping deleted first) to trigger Discord member notifications.

### Data structures (`AlertSystem`)
| Dict | Key → Value |
|------|-------------|
| `signal_messages` | signal_id → persistent embed `discord.Message` |
| `signal_ping_messages` | signal_id → latest ping reply `discord.Message` |
| `signal_finished_messages` | signal_id → archived copy in finished-signals/profit channel (`PartialMessage` after `recover_finished_embeds`) |
| `alert_messages` | message_id_str → signal_id (bounded at 1000; for reply-handler lookup). Holds BOTH embed IDs AND ping IDs so users can reply to either. Tracked on send + hydration; untracked when a message is deleted (retraction, archive move, old-ping replacement). |
| `_live_embeds` | signal_id → `{"signal": dict, "event": str, "spread_buffer_enabled": bool}`; drives the bounded sequential live-refresh pass. Caller must keep `signal["limits"]` in sync; otherwise the refresh re-renders stale data |
| `auto_purge_channel_ids` | Set of channel_id strings whose original signal messages are deleted on end-state. Built from `monitored_channels` minus `AUTO_PURGE_EXEMPT_NAMES = {"price-action-trades"}`. |

### Embed edit vs standalone
| Event | Behavior |
|-------|----------|
| Approaching, hit, stop-loss, auto-TP, near-miss cancel | Always edit persistent embed |
| Spread-hour cancel | Edit embed if exists; **silent** if no embed yet |
| News-mode cancel | Edit embed if exists; standalone message only if no embed yet |

### Channel routing (`_get_alert_channel`)
Priority order:
1. General-toll or oil-toll signal → `general_toll_alert_channel`
2. Regular toll signal → `toll_alert_channel`
3. PA signal → `pa_alert_channel`
4. Legends signal → `legends_alert_channel`
5. Default → `alert_channel`

### Live-update task
`start_live_updates()` waits **30 seconds after a completed pass**, then takes one snapshot of the active embeds and drains it sequentially. A pass is never refilled while running; failed cosmetic edits are dropped until the next pass, and each edit has an 8 s total timeout. If a status/event edit begins, remaining cosmetic work is discarded so the critical event gets the next HTTP slot. Critical attempts have a 20 s timeout and continue through a deduplicated exponential-backoff retry task. An INFO summary is emitted after every refresh pass. Stopped when a signal closes or is cancelled.

### Key public methods
- `send_approaching_alert(signal, limit, current_price, distance_formatted, spread, spread_buffer_enabled)` — creates embed; registers for live updates
- `send_limit_hit_alert(signal, limit, current_price, spread, spread_buffer_enabled)` — edits embed; sends ping
- `send_stop_loss_alert(signal, current_price)` — edits embed; sends ping; unregisters live updates
- `send_auto_tp_alert(signal, hit_limits, last_pnl, tp_config, cumulative_pnl, limit_pnl_map)` — edits embed; sends ping
- `send_near_miss_cancel_alert(signal, nm_state)` — edits embed; sends ping
- `update_signal_message(signal, event, limits, current_price, ping_text)` — generic editor
- `update_embed_for_signal_id(signal_id, event, ping_text)` — fetches signal, calls `update_signal_message`; safe to call from anywhere
- `reactivate_embed(signal, ping_text)` — rebuilds embed for reactivated signals with live price/distance; cancels the pending archive move so embed + original signal message survive the 15-min window
- `retract_approaching_embed(signal_id)` — deletes the embed + ping for a signal whose price has drifted back past `_APPROACHING_RETRACTION_MULTIPLIER × alert_distance`; resets the limit's `approaching_alert_sent` so a future re-approach fires fresh
- `hydrate_from_db(signals)` — startup recovery; per-signal: reuse Discord embed if found, otherwise rebuild (HIT) or reset `approaching_alert_sent` (ACTIVE)
- `recover_pending_archives()` — re-schedules `schedule_end_state_move` for end-state signals whose 15-min countdown was interrupted by restart
- `recover_finished_embeds()` — re-registers finished-channel embeds via `PartialMessage` so reply commands (e.g. `reactivate`) survive a restart
- `track_alert_message(message_id, signal_id)` / `get_signal_from_alert(message_id)` — reply-handler lookup

---

## Dependency Injection Pattern

`TradingBot.initialize_price_monitor()` in `core/bot.py` creates all subsystems and injects them into `StreamingPriceMonitor` via constructor kwargs:

```python
monitor = StreamingPriceMonitor(
    bot, signal_db, db,
    alert_system=alert_system, stream_manager=stream_manager,
    alert_config=alert_config, tp_config=tp_config, tp_monitor=tp_monitor,
    nm_config=nm_config, nm_monitor=nm_monitor,
    live_price_writer=live_price_writer, health_monitor=health_monitor,
)
```

`ServiceRegistry` (`core/services.py`) is a typed container holding references to all subsystems. Populated in `setup_hook()` and injected into cogs via `BaseCog.__init__`:
```python
self.services = bot.services
self.services.alert_system   # instead of bot.monitor.alert_system
self.services.tp_config      # instead of bot.monitor.tp_config
```

---

## Critical Conventions / Gotchas

### Pydantic models use attribute access only
`SignalData` and `LimitData` in `models/signal.py` are plain Pydantic models — the transitional dict-protocol (`signal["instrument"]`) was removed in stage 20. All model access is attribute style (`signal.instrument`). Raw asyncpg rows and the `!active` presentation dicts (from `database/report_queries.py`) are still dicts; local variables holding them are named `row` to keep the two shapes visually distinct.

### id normalization
- DB column is `signals.id`, but the domain name is `signal_id`. `SignalData.from_db_row()` and `SignalData._normalise_id` handle this automatically.
- DB column is `limits.id`, but tracking queries return `limit_id`. `LimitData._normalise_limit_id` handles this automatically.

### asyncpg parameter passing
- Raw `conn.execute(query, val1, val2)` — positional splat, `$1`/`$2`/... placeholders
- Wrapper `db.execute(query, (val1, val2))` — takes a **tuple**, unpacks with `*params` internally
- Timestamps come back as native `datetime` objects — never pass to `datetime.fromisoformat()`. Use `_parse_dt` from `database.utils` (handles both `datetime` and ISO strings)
- `ROUND(x, 2)` in PostgreSQL requires `CAST(... AS NUMERIC)`, not `CAST(... AS FLOAT)`

### NM immunity after reactivation
`nm_monitor.mark_immune(signal_id)` is called whenever a cancelled signal is reactivated (any path: reply command, `!setstatus active`, `!reactivate`). Immune signals skip NM checks permanently for that signal's lifetime and can only close via hit, profit, SL, or manual cancel.

### In-memory refresh after edit / reactivate
`StreamingPriceMonitor.refresh_signal_in_memory(signal_id)` re-fetches a single signal from the DB and swaps the dict in both `active_signals` (read by every price tick) and `alert_system._live_embeds` (read by the 30 s live-refresh). It also handles instrument changes (re-keys `symbol_to_signals`, unsubscribes the old feed, subscribes the new) and calls `tp_monitor.refresh_hit_limits` for HIT signals. Called from `handle_message_edit` (both normal-edit and cancelled-then-re-edited branches), the `reactivate` reply handler, and `!setstatus active`. Without this call, the 30 s periodic refresh would briefly serve stale data and price ticks would evaluate pre-edit limits/SL.

### `!active` defaults to sort:distance
The default sort for `!active` is `distance` (closest pending limit first). Other choices: `recent` / `oldest` / `progress` via `sort:<method>` or `!active <SYMBOL> sort:<method>`. Signals without a usable distance fall to the end.

### Windows-only MT5
`MetaTrader5` package requires Windows. On Linux/Mac, the ICMarkets and Exness feeds will be unavailable. The bot handles this gracefully but loses those feeds.

### Dual MT5 terminals (ICMarkets + Exness)
The `MetaTrader5` Python package is process-global — `mt5.initialize()` can only connect to one terminal at a time. The ICMarkets feed runs in the main process; the Exness feed runs in a **child process** (`exness_worker.py`) spawned via `asyncio.create_subprocess_exec`. Communication is via JSON lines over stdin (commands) and stdout (prices). Both terminals must be running on the VPS before the bot starts. The `MT5_PATH` and `EXNESS_MT5_PATH` env vars should both be set to avoid auto-discovery ambiguity.

### Exness oil symbol mapping
Internal symbol `USOILSPOT` maps to `USOILm` on Exness MT5. The mapping is defined in both `symbol_mappings.json` (`symbol_mappings.exness.specific_mappings`) and the reverse direction (`reverse_mappings.exness`). Both sections are required — without the reverse mapping, prices arrive under `USOILM` instead of `USOILSPOT` and don't match signals.

### Spread/news behavior
Spread-hour and normal news cancels **edit the persistent embed** when one already
exists. News events created with `dryrun` still update `bot_mode_status.news_mode`
and post activation/ended notices for clients, but never suppress or cancel
alert-bot signals.

### News matching is per-currency; USD also covers US markets
`NewsEvent.instrument_affected` matches per currency (CHF news never touches EURUSD). A `USD` category additionally pauses US equities (`.NAS`/`.NYSE`) and US indices (`US_INDEX_KEYWORDS`: NAS100/US30/US500/SPX500/SPX/USTEC/US2000/…), and pauses gold when `affects_gold` is set (auto-fetched high-impact USD events). Auto-fetched events carry a merged `title` ("EUR — ECB Rate / Press Conf"); `NewsEvent.display_label` is used in all news alerts (activation, cancel, ended), falling back to the bare category for manual events.

### Instant-entry channels enter at market, not at a limit
`semi-swing-pa-signals` (listed in `validators._INSTANT_ENTRY_CHANNELS`) posts signals of the form `short gold sl 5001 tp 4080`: an instrument, a direction, and labelled SL/TP with **no limits**. `parse_instant_signal()` produces a `ParsedSignal` with `instant_entry=True`, `take_profit` set, and `limits=[]`; the AI fallback is skipped for these channels.

`message_handler` resolves the entry before saving: `_live_entry_price()` reads the feed via `stream_manager.get_latest_price()` (subscribing the symbol first if nothing was watching it), takes the **ask for a long / bid for a short**, and rejects prices older than `_INSTANT_ENTRY_MAX_PRICE_AGE` (15 s). `_resolve_instant_entry()` then rejects with ⚠️ plus a reply when there is no live price, or when price already sits outside the SL↔TP band — that trade would open only to close on the next tick. The resolved price becomes the signal's single limit, and `save_signal` writes it **already hit** — status `hit`, `limits_hit=1`, `first_limit_hit_time` set — in the same transaction as the signal row; `_open_instant_position()` then just opens the embed directly as HIT. Being born filled is load-bearing, not cosmetic: inserting the limit as `pending` and marking it hit on a second round-trip left a multi-second window (a pooler status write is 2–5 s) in which the row was indistinguishable from an ordinary signal resting a limit at the market price. Every released EX bot keys its placeable set on `l.status='pending'`, so during that window even versions that disable this channel would place a real limit order on a user's account. Nothing returns an instant limit to `pending` afterwards — cancels and terminal transitions only touch `pending` rows, reactivation only restores `cancelled` ones, and an edit rewrites SL/TP/expiry only. Overlap detection is skipped (the limit fills immediately, so it never competes with another signal's resting limits).

**A second entry can be averaged in** by replying `add` to the alert embed, its ping, or the original signal message. `signal_ops.add_instant_entry` appends a limit at the live market price in the same already-filled shape (`status='hit'`, `hit_time`/`hit_price` stamped), bumps `total_limits`/`limits_hit`, and the embed re-renders as 2/2 hit. Same entry gates as the original: fresh live price, and price still inside the SL↔TP band. `MAX_INSTANT_ENTRIES` (2) **and the signal's `status`** are both re-read inside the write under `SELECT … FOR UPDATE` on the signal row: two replies arriving together would otherwise each see one entry and each add one, and an auto-TP landing between the handler's status check and this write would append a fill to a closed signal and inflate `limits_hit`. Only instant signals accept it: an ordinary signal enters on limits the sender chose, and a market fill bolted onto those is a level nobody asked for. Everything downstream just sees a second filled limit — auto-TP averages it in, `breakeven_price` moves to the mean of both fills, exits are unchanged.

**An armed breakeven stop is disarmed by an add that moves the mean past the market**, and the ping says so. Averaging *down* is the main reason to add, so the new mean routinely lands on the far side of the bid — which is precisely the state `_can_arm_breakeven_stop` refuses to create at arm time, reached from the other direction; left armed, the next tick would close the trade flat. `_breakeven_disarm_note` runs the arm-time profitability test against the post-add mean (on the bid, like the stop itself) and disarms when it fails or when no live bid is available to check. The clear rides in the same locked write as the fill (`add_instant_entry(..., disarm_breakeven=True)`) — a signal that is briefly both averaged and still armed is one a tick can close on the spot. Re-arm with `set be` once the trade is back in profit against the new average.

Downstream everything is shared machinery: `type='pa'` routes the embed to the PA alert channel and the signal into the PA report section; SL, manual reply commands, archiving, trailing and excursion analytics are unchanged. An already-HIT signal rides out spread hour and the late-market hour. Normal news events cancel it (except `swing`); `dryrun` news events leave it running while pausing affected clients. Edits to an instant signal update **SL/TP and expiry only** (`signal_ops._update_instant_from_edit`); the entry limit records a real fill and is never re-derived. The EX bot ignores these signals — it keys off pending limits, and there is never one.

### Signal type taxonomy
`signals.type` ∈ `{standard, scalp, swing, toll, pa, 1-1, risky}`. Determined by `pattern_parsers.get_signal_type(text, channel_name)`:
- `CHANNEL_TYPE_MAP` wins first: `scalps` → scalp; `swing-trades`/`gold-swings` → swing; `gold-tolls-map`/`general-tolls`/`oil-tolls` → toll; `gold-pa-signals`/`price-action-trades`/`semi-swing-pa-signals` → pa; `gold-1-1-rr` → 1-1.
- Otherwise body keyword: `\bswing\b` → swing, `\bscalp\b` → scalp.
- Default: standard.

Each type has its own `tp_configuration.json` defaults under `type_defaults[<type>]` and per-symbol overrides under `type_overrides[<type>]`. Default initialization: scalp/standard kept as before; toll initialized from scalp; pa initialized from standard; swing = 3× standard; 1-1 metals = $10. The TP resolution order is: per-type symbol override → standard symbol override → per-type asset-class default → standard asset-class default → hard fallback ($5).

### Breakeven stop (`set be`)
Replying `set be` to an alert embed, its ping, or the original signal message arms a
breakeven stop on an open (HIT) position: from then on the signal closes flat when
price reverses to its entry, instead of riding down to the stop loss. `unset be`
(or `remove be`) disarms it. Distinct from the bare `be` reply, which marks the
signal breakeven *immediately* — that one is a status command, this one is protection.

- **The BE price is the mean of every filled limit** (`models.signal.breakeven_price`).
  Lots are equal per limit, so the mean is where the position's combined P&L is
  exactly zero — the fills above it lose what the fills below it gain. Note this is
  *not* the trailing sim's anchor, which is the deepest fill (the runner).
- **Evaluated on the bid in both directions**, like auto-TP, so it fires where the
  chart shows it rather than a spread away. No spread buffer.
- **Arming is refused unless the trade is currently in profit on the bid** — arming
  underwater would close the trade on the very next tick. Also refused when the
  signal is not HIT, or when no live price is available to check against.
- **The real stop loss wins a tick that gaps through both.** `_check_stop_loss` runs
  first and its `sl_alert_sent` flag stands the BE check down, so the DB records the
  loss the position actually took rather than a flat exit it never got.
- **Non-crypto signals ride out spread hour**, mirroring the SL rule: the bid blows
  out in the window and a long fires on `bid <= be_price`, so the spread alone would
  close the trade. Cross-repo this matters more than it looks — the EX bot strips its
  stop-losses over the same window to survive the spike, so a BE fired by a spread
  artifact would force-close its position at exactly the price the stripping exists
  to dodge (the EX also defers `breakeven` force-exits through the window, but the
  artifact belongs stopped here, at the source).
- **An `add` that moves the mean past the current bid disarms the stop** rather than
  firing it — see the `add` reply in Instant-entry channels.
- Take-profit, news/risky guards, expiry and every manual command are
  unchanged — this only puts a floor under the trade.
- The flag lives on `signals.be_stop_armed_at` and is selected by BOTH
  `SIGNAL_COLUMNS` and `manager.get_active_signals_for_tracking` — the tick path is
  fed by the latter, so a new per-signal column that only lands in `SIGNAL_COLUMNS`
  is invisible to price ticks after a restart.
- Arming calls `refresh_signal_in_memory` so ticks see it immediately instead of
  waiting up to 30 s for the periodic refresh.

### Swing is not near-missable
`NearMissMonitor.update` short-circuits when `signal.type == "swing"`. Swing signals can only close via hit, profit, SL, or manual cancel.

### Gold-tolls SL offset is configurable
`get_gold_tolls_sl_offset()` reads `settings.json` via `load_settings()` → `BotSettings.gold_tolls_sl_offset` (default `5.0`) with a 30 s cache. Change it via `!goldtollssl <value>` or edit `settings.json` directly.

### `stop_loss` is required in DB
`signals.stop_loss` is `NOT NULL`. Toll channels auto-calculate SL from limits; general-tolls derives SL from message numbers. Any new channel or parse path that doesn't produce a SL value will fail on insert.

### Reply command authorization
- Reply to an **alert embed OR its ping reply**: any user can execute reply commands; this includes embeds in the finished-signals channel (e.g. `reactivate` works from there). Ping IDs are tracked in `alert_messages` on send and on hydration; the entry is removed when the ping is deleted (retraction, archive move, or replaced by a newer ping for the same signal).
- Reply to the **original signal message** (has ✅ reaction): signal author **or** admins only

### Bot reply auto-delete
Bot reply messages (acknowledgements, error responses, the user's trigger reply itself) are deleted after `_REPLY_DELETE_AFTER = 15 s` in monitored / alert channels to keep them tidy. Persistent embeds, pings, and confirmations sent via `ctx.send` in command channels are not auto-deleted.

### Cancel via original signal message reply (no prior alert embed)
When `cancel` is replied to the original signal message and no approaching/hit embed exists yet:
- **Auto-purge channels** (`alert_system.auto_purge_channel_ids` — every monitored channel except `price-action-trades`): original message is deleted and a cancellation embed is posted to the finished-signals channel (also tracked in `alert_messages` so the user can reply `reactivate` to it)
- **Exempt channels** (only `price-action-trades` today): ❌ reaction is added to the original message; it is **not** deleted

### Reactivate when original message is gone
`reactivate_cancelled_signal` in `signal_ops.py` does not use its `parsed_signal` argument — it reactivates purely from DB state. The reactivate reply handler tries to fetch and re-parse the original message but falls back gracefully if it has been deleted (e.g. toll signals after archive move). Pass `None` as `parsed_signal` and reactivation proceeds.

### Message handler runs before prefix commands
`bot.on_message()` calls `message_handler.handle_new_message()` before `await self.process_commands(message)`. Reply commands are fully handled in the message handler and are not registered as bot commands.

### TP logic is per-limit, not fully cumulative
Auto-TP fires when: **last hit limit's P&L ≥ threshold** AND (if 2+ limits hit) **sum of earlier limits' P&L ≥ 0** (epsilon tolerance 1e-9). It is not a simple cumulative P&L check across all limits.

### Auto-TP is evaluated on the bid, both directions
`tp_monitor.check_signal` measures P&L from the limit's `price_level` to the **bid**, long and short alike, so a $4 threshold fires on $4 of movement as displayed on the chart — never $4 + spread. Shorts previously closed on the ask, which silently required the extra spread. The spread buffer still applies to approaching/hit checks (`spread_buffer_config`); it is deliberately absent from TP and stop-loss. The bid at trigger is what lands in `tp_price`, and `streaming_monitor` starts the trailing sim and the excursion exit from that same price so all three agree.

### The auto-TP status guard runs at trigger, not per tick
`check_signal` does a cheap in-memory `signal.status` check on every tick, and only re-reads the DB (`_closed_elsewhere`) once the threshold has actually been cleared. The DB read is what prevents double-marking a signal that a command closed within the last periodic-refresh cycle, so it must stay — but it belongs after the threshold test. Reinstating a per-tick DB round-trip here serialises into the feed dispatch loop (`price_stream_manager._process_price_update` awaits subscribers in order) and throttles tick sampling for every symbol on that feed, not just this one.

### react_to_original_signal
Module-level function in `price_feeds/monitors/streaming_monitor.py`. Called from `streaming_monitor`, `expiry_manager`, and `commands/signals/lifecycle.py` to add emoji reactions to original signal messages.

### Overlap detection on signal save
When a new signal is saved, `signal_ops.get_overlapping_signals()` queries for active/hit signals on the same instrument whose pending-limit price range (`[MIN, MAX]`) intersects the new signal's range. If any are found, `message_handler._handle_overlap_prompt()` is spawned as a background task (non-blocking): it posts a prompt in the same channel, waits up to 30 s for a ✅ (cancel old) or ❌ (keep both) reaction from the signal author or a guild admin. Timeout defaults to cancelling the old signal(s). The prompt is deleted after the outcome.

### Reactivation guard
Before reactivating a cancelled signal via reply command or `!setstatus active`, `signal_ops.check_reactivation_guard()` fetches the signal's cancelled limits and compares them against the current mid-price from `live_prices`. A limit is "past" when the hit condition would fire immediately on reactivation (`long: mid ≤ limit`, `short: mid ≥ limit`). If any limits are past, reactivation is blocked with a clear message listing which limits are stale. Returns `None` (fail-open) if price data is unavailable. Admin override: `!setstatus <id> active --force` (requires a configured bot admin or `guild_permissions.administrator`).

### Tick staleness gate
`streaming_monitor._on_price_update` drops ticks older than `_MAX_TICK_AGE_SECONDS` (5 s) before any signal evaluation. The timestamp comes from `price_data["updated_at"]`, which is stamped by `price_stream_manager._process_price_update`: UTC broker tick time for ICMarkets and Exness (from `tick.time`), current wall-clock for OANDA/Binance. Spread-hour transition tracking still runs on stale ticks — only per-signal checks are skipped.

### HIT signals roll over at expiry
`expire_old_signals` in `signal_ops.py` branches on status. **ACTIVE signals** are cancelled (existing behaviour). **HIT signals** are rolled over: `expiry_time` is advanced to the next occurrence of the same `expiry_type` (via `calculate_expiry`) and a `status_changes` row is inserted with `change_type='automatic', reason='rollover'`. The signal status and limits are untouched. This repeats each expiry window until the position closes naturally.

### `save_signal` is TOCTOU-safe
Uses `INSERT … ON CONFLICT (message_id) DO NOTHING RETURNING id`. If no id is returned (duplicate parse race), the existing row is inspected on the same connection: reactivate if cancelled, reject otherwise. The pre-check `SELECT` is gone.

### `mark_limit_hit` is atomic
All updates in `manager.mark_limit_hit` (limit row, signal counter, status→HIT, audit row) run inside `async with conn.transaction()`. A mid-flight disconnect cannot leave the row half-updated.

### Hit limits loaded on restart
`streaming_monitor._load_and_subscribe_signals` fetches hit limits for every HIT-status signal (via `get_hit_limits_for_signal`) and appends them as `LimitData(status="hit")` to `signal.limits`. After restart, `signal.hit_limits` is non-empty so embed builders see the complete limit history without waiting for the next event.

### live_prices is written for every feed (no IC reference columns)
`LivePriceWriter.TRACKED_FEEDS` includes `icmarkets`, so every signal-bearing symbol gets a `live_prices` row sourced from its serving feed — including IC-primary instruments (forex, stocks, GCZ26_CFD metals, XTIUSD oil) that previously had no row. The old `ic_bid`/`ic_ask` columns were dropped (migration `ALTER TABLE live_prices DROP COLUMN IF EXISTS`), and the IC reference-only symbol machinery that fed them (`subscribe_reference`, 15-min slow polling) was removed with them: the EX bot now derives its broker offset from its own MT5 feed against the stored price at the same timestamp.

### Price-flow watchdog
`FeedHealthMonitor._check_price_flow_watchdog` force-restarts the bot (graceful `bot.close()` → `main.py` supervisor relaunch) only when ALL hold: past `WATCHDOG_GRACE_SECONDS`, at least one subscribed symbol whose market is open now (via `is_market_open`, which already excludes weekends/holidays/spread hour), and zero ticks across every feed for `WATCHDOG_SILENCE_SECONDS` (180 s). Fires at most once (`_watchdog_fired`). Runs the shutdown in a separate task so it doesn't await its own monitor task.

### Discord connection watchdog
`TradingBot.heartbeat` checks gateway readiness every 30 s and runs an authenticated REST probe every 60 s. A gateway that remains unready for 120 s, three consecutive REST probe failures/timeouts (30 s per probe), or two consecutive bounded message-operation timeouts closes the bot from a separate task so `main.py` can relaunch it. Cosmetic edits time out after 8 s and critical attempts after 20 s; this catches a wedged message route even when the separate user-fetch probe remains healthy. The client also caps any single non-global Retry-After at 60 s; ordinary short Discord Retry-After responses are still honoured.

### Per-feed reconnect (no cascade)
`FeedHealthMonitor.attempt_reconnection` calls `PriceStreamManager.reconnect_feed(name)` to reconnect only the stale feed. It must never call `reconnect_all()` from the health path — that tore down healthy feeds (and the MT5 terminal) whenever one feed went stale. OANDA additionally self-heals via a read-timeout watchdog in `oanda_stream.stream_prices` (`_STREAM_READ_TIMEOUT` = 15 s; OANDA heartbeats every ~5 s), so a silently half-dead stream reconnects without health-monitor involvement.

### Alert embed recovery on restart
The alert embed message reference is persisted on `signals` (`alert_message_id`, `alert_channel_id`, `ping_message_id`) every time `_upsert_signal_message` creates a new embed or sends a new ping. The archived embed location is persisted on `finished_message_id` / `finished_channel_id` when `archive_manager._move_after_delay` moves the embed, or when the signal-reply cancel path posts a direct cancellation embed to the finished channel.

On startup, `AlertSystem.hydrate_from_db` runs from `streaming_monitor._load_and_subscribe_signals` AFTER hit-limits are loaded and BEFORE `bulk_subscribe` — this ordering matters: if the price stream started first, the next tick could fire `send_approaching_alert` / `send_limit_hit_alert` and post a duplicate embed alongside the orphaned one. Per-signal decision for active/hit signals:
- **Persisted ID + Discord fetch succeeds** → re-populate `signal_messages` / `signal_ping_messages` / `alert_messages` (both embed and ping IDs) and register for live updates. Same embed continues through the bounded sequential refresh pass.
- **Persisted ID + NotFound, status=ACTIVE** → clear persisted IDs, `UPDATE limits SET approaching_alert_sent = FALSE WHERE signal_id=$1 AND status='pending'`, mutate in-memory limit copies. The approaching alert re-fires on the next price tick with a fresh embed.
- **Persisted ID + NotFound, status=HIT** → clear persisted IDs and call `reactivate_embed(signal, ping_text=None)` to rebuild the embed immediately so live updates and future events have a target.
- **No persisted ID** (pre-feature signals, first deploy) → same fallback as above: ACTIVE resets `approaching_alert_sent`; HIT rebuilds. One-time cosmetic churn on first restart after deploy.

After hydration, two more recovery passes run:
- **`recover_pending_archives()`** — any end-state signal (`profit`/`stop_loss`/`cancelled`/`breakeven`) with a non-NULL `alert_message_id` had its 15-min archive countdown interrupted by the restart. The embed is refetched, registered, and `schedule_end_state_move` is re-armed so it eventually moves to finished / profit.
- **`recover_finished_embeds()`** — every signal with non-NULL `finished_message_id` and `finished_channel_id` (closed in the last 14 days) gets a `PartialMessage` reference put in `signal_finished_messages` + tracked in `alert_messages`. This is O(N) with no Discord API calls; the partial is enough for delete / reply lookup. Reply commands like `reactivate` against archived embeds work across restarts as a result.

IDs are cleared in `_clear_persisted_alert_ids` on live-update NotFound (`alert_system._refresh_one_embed`), in `archive_manager._move_after_delay` after the embed is moved out of the alert channel, and during retraction.

### Approaching alert retraction
Once `limits.approaching_alert_sent=TRUE`, the embed used to linger until hit, SL, NM, expiry, or manual cancel. A new check in `streaming_monitor._check_limit` gated on `signal.status=='active' AND limit.sequence_number==1 AND limit.approaching_alert_sent` retracts the embed when `abs(distance) > _APPROACHING_RETRACTION_MULTIPLIER × alert_distance` (default `2.0`, module-level constant in `streaming_monitor.py`). `_retract_approaching_alert` calls `alert_system.retract_approaching_embed` (deletes embed + ping, removes from all dicts, clears persisted IDs), resets `approaching_alert_sent=FALSE` on the limit, evicts `nm_monitor` tracking state. Next time price re-enters the alert distance the approaching alert fires fresh. Applies to all signal types (no swing carve-out — retraction is cosmetic, not a cancel). NM tracking starts cleanly on re-approach because `nm_monitor.update` bails when `approaching_alert_sent` is False (`nm_monitor.py:151`).

### Close-price snapshot on every terminal transition
`signal_ops._snapshot_close_prices(signal_id, instrument)` writes `close_bid/close_ask/close_feed` from `live_prices` whenever a signal with ≥1 hit limit closes. Hooked in `manually_set_signal_status` (covers auto-TP, NM, news/spread/risky cancels, SL, admin commands), `cancel_signal_by_message`, and the cancel branch of `expire_old_signals`. Best-effort: wrapped in try/except; a missing live price logs a warning and skips.

### Save-time context stamps
`message_handler._build_save_context()` stamps `tp_threshold_used/tp_threshold_unit` (resolved TPConfig threshold) and `minutes_to_news` (next affecting news event) onto every new signal via `save_signal(..., context=)`. Best-effort; reactivated signals keep their original stamps.

### config_history on every config command
`!tp set/remove`, `!alertdist set/remove`, `!nmconfig set/remove`, `!goldtollssl`, `!riskygoldsl` append old/new values to `config_history`. When adding a new runtime-config command, hook `log_config_change` the same way.

### Pip sizes are canonical in `BaseThresholdConfig.get_pip_size`
Stocks (.NAS/.NYSE) 0.01, forex 0.0001 (JPY 0.01), XAU/GC futures 0.01, XAG 0.001, BTC 1.0, other crypto 0.1, indices 1.0, oil 0.01. The stock branch must stay ABOVE the index-keyword branch (".NAS" contains "NAS"). Unknown symbols fall back to 0.0001 — when adding a new asset class, add a branch here FIRST or `signal_excursions` pips will be wrong (this exact bug corrupted era-1 excursion rows; backfilled 2026-07-12).

### Excursion ratchet writes are flushed on an interval, never per tick
`pre_hit_mae` / MFE / MAE ratchet in memory on every tick; the DB write is deferred to a background flush at most every `_RATCHET_FLUSH_INTERVAL_SECONDS` (5 s), plus one awaited flush at the top of `finalize`. During a fast run nearly every tick sets a new extreme, so writing per ratchet put a DB round-trip in the feed dispatch loop precisely when price was moving fastest. Accuracy is unaffected — the extremes are in-memory truth, `finalize` re-asserts them, and the DB keeps whichever is larger. Only the flush carries the ATR multiples, which is why `finalize` flushes before writing. Keep new per-tick excursion fields on the same dirty-flag path.

### Excursion ordering flag + post-exit window
`ExcursionMonitor` (now constructed with `tp_config`) decides `mae_before_mfe` when either excursion first exceeds 25% of the TP threshold (`_ORDERING_BAR_FRACTION`). After close, entered signals move to a `_post_exit` dict sampled by the same 60 s loop: favorable follow-through beyond the exit price from M1 bars ratchets `post_exit_mfe_pips` for 60 min (`_POST_EXIT_WINDOW_SECONDS`), then `post_exit_end_time` is stamped. Post-exit state does not survive a restart.

### Every terminal close must finalize the analytics trackers
`monitor.finalize_trailing_on_manual_close(signal_id)` is the single hook for closes that have no tick price on hand — reply commands, `!setstatus`/`!profit`/bulk cancel, expiry (`expiry_manager`), and message-deletion cancel (`message_handler.handle_message_delete`). It reads the last `live_prices` row and finalizes both the trailing sim and the excursion row. **When adding a new close path, call it.** Two supports make a miss non-fatal:
- `_closing_instrument` falls back to a DB lookup when the signal has already left `active_signals` — a pooler status write can take seconds, long enough for the periodic refresh to drop it first.
- `ExcursionMonitor.finalize` writes to the DB regardless of in-memory state (the UPDATE is scoped to still-open rows), and `run_reconciler` sweeps every 15 min for rows whose signal is already final, deriving the exit from `tp_price` / the close snapshot and suffixing `exit_reason` with `:reconciled`.

Losing in-memory excursion state mid-trade also freezes `mfe_pips`/`mae_pips` at their last value, so a missed hook costs more than a NULL exit — see DATA_ANALYSIS.md §5.

### Data-era marker
`signals.data_version` = 1 for rows created before 2026-07-12, 2 for the clean-instrumentation era. Analysis conventions live in **DATA_ANALYSIS.md** — keep that file current when changing analytics-relevant schema or semantics.

---

## Configuration Files

| File | Location | Contents | Runtime-editable |
|------|----------|----------|-----------------|
| `channels.json` | `config/` | 19 monitored channel IDs; alert/command/profit/finished channel IDs; per-channel defaults | No (restart needed) |
| `settings.json` | `config/` | `admin_ids`, `health_alert_admin_id`, `spread_buffer_enabled`, `spread_buffer_config`, `license_role_name`, `gold_tolls_sl_offset`, `us_market_holidays` | Yes (30 s cache for some); loaded as `BotSettings` Pydantic model |
| `tp_configuration.json` | `config/` | Per-type TP thresholds (`type_defaults`/`type_overrides`, keyed by signal type → asset class / symbol). Legacy `defaults`/`scalp_defaults`/`overrides`/`scalp_overrides` shape is auto-migrated on first load. | Yes (`!tp set`) |
| `alert_distances.json` | `config/` | Approaching alert distances per asset class + overrides | Yes (`!alertdist set`) |
| `nm_configuration.json` | `config/` | NM max_proximity + base_bounce per asset class + overrides | Yes (`!nmconfig set`) |
| `symbol_mappings.json` | `config/` | Feed-specific symbol name translations | No |
| `news_events.json` | `data/` | Active/upcoming news windows; persisted across restarts | Yes (written by `!news`) |

---

## Environment Variables (`.env`)

```
DISCORD_BOT_TOKEN=...
SUPABASE_DB_URL=postgresql://postgres.[ref]:[pw]@aws-0-[region].pooler.supabase.com:5432/postgres
OANDA_API_KEY=...
OANDA_ACCOUNT_ID=...
OANDA_PRACTICE=false
OPENAI_API_KEY=...              # optional; AI fallback off by default
BINANCE_USE_INTERNATIONAL=false # set true if binance.us is blocked
EXNESS_MT5_PATH=...             # path to Exness terminal64.exe; enables oil feed
EXNESS_MT5_LOGIN=...            # Exness MT5 account login ID
EXNESS_MT5_PASSWORD=...         # Exness MT5 account password
EXNESS_MT5_SERVER=...           # Exness MT5 server (e.g. Exness-MT5Trial11)
LOG_LEVEL=INFO                  # DEBUG enables app + discord.py debug in console and bot.log
```

`database/__init__.py` calls `load_dotenv()` with an absolute path derived from `__file__` before instantiating `DatabaseManager`, so `.env` is found regardless of working directory. `main.py` does the same.

---

## Coding Standards

The codebase aims to read like a clean production application: minimal, direct, professional. These rules exist because they tend to break when an LLM edits without guidance — they're guardrails, not aesthetics.

### Naming
Names describe what something *is* or *does* in the present tense, not its history or how it came to exist. `SignalParser`, not `EnhancedSignalParser` or `SignalParserV2`. If you find a name with `enhanced_`, `improved_`, `_v2`, `new_`, or `_fixed`, that's a smell from an earlier edit — drop the adjective unless removing it creates a real collision.

### Functions
One function, one job. If you need "and" to describe what it does ("parses the message and updates the DB and sends the alert"), it should be split. Keep functions short enough to fit on a screen; if early returns get you there, prefer them over deep nesting. Don't introduce parameters that only one caller passes — inline the value or split the function.

### Error handling
Catch what you can actually recover from. A bare `except: pass` is hiding a bug, not preventing one. A try/except around code that can't fail (e.g. guarding a method call on an attribute the constructor always sets) is noise — delete it. When you do catch, log enough to debug from.

### Imports
At the top of the file, grouped (stdlib / third-party / local), no inline imports unless there's a real circular-import reason — and if there is one, that's usually a sign the module split is wrong. Drop unused imports.

### Comments and docstrings
Comments explain *why*, not *what*. `# Loop through signals` above `for signal in signals` is noise. Docstrings describe the contract (what it does, what it returns, what raises) — they don't restate the signature. Strip refactor-history preambles from docstrings (`"REDESIGNED:"`, `"Stage 2 Enhanced:"`, `"FIXED:"`) — that belongs in git, not the file.

### Constants and magic numbers
Numbers and strings that mean something — timeouts, thresholds, role IDs, channel keys — go in named constants at module top or in a config file. Inline `1000`, `"approaching"`, `<@&123456>` scattered through code is harder to change safely.

### Defensive programming
`hasattr` guards, `if x is not None` checks, and try/excepts have a real cost — they obscure the happy path and tell future readers "this might fail" when it can't. Use them at genuine boundaries (external I/O, user input, optional config) and not for attributes the constructor always sets. When in doubt: would a sensible caller ever hit this case? If no, delete the guard.

### Logging
Log at the level that matches the severity (debug for tracing, info for normal events, warning for recoverable problems, error for failures). Don't sprinkle emojis or status indicators into log messages — `logger.info("Signal saved")` not `logger.info("✅ Signal saved successfully!")`. Logging is for operators, not for users.

### Classes vs functions
Prefer functions unless you actually need state. A class with only a constructor and one method is a function in disguise. A class that holds three sub-managers and forwards every method to them is plumbing — flatten it or delete it.

### Formatting
Mechanical formatting (line length, quote style, import sort, trailing commas) is handled by `ruff` — see `pyproject.toml`. This will be done manually by the user. No need to run these commands yourself.
