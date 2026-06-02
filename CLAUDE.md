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
- **Models**: Pydantic v2 for domain objects (`SignalData`, `LimitData`, `BotSettings`) with dict-protocol methods for backward compat
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
                                  dict-protocol methods (__getitem__, get, etc.) for transition;
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
  channel_cleaner.py            @tasks.loop(1min) — bulk-deletes alert-channel messages every
                                  Friday 18:00 local time (7-day window)
  parser/
    __init__.py                 parse_signal(message, channel_name) entry point;
                                  ParsedSignal / RejectedSignal types; lazy sub-parser init
    pattern_parsers.py          CorePatternParser / StockPatternParser / CryptoPatternParser;
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
  schema.py                     DDL for all 7 tables + indexes + idempotent migrations
  signal_ops.py                 SignalDatabase — flattened CRUD + Lifecycle + Analytics in one file;
                                  get_signal_with_limits() returns Optional[SignalData];
                                  save_signal, cancel, reactivate, manually_set_signal_status,
                                  process_limit_hit, expire_old_signals, get_statistics;
                                  get_overlapping_signals() — range-intersection query used on save;
                                  check_reactivation_guard() — compares cancelled limits vs live price;
                                  _get_live_price() — reads bid/ask from live_prices table
  utils.py                      calculate_expiry() (day_end → 4:45 PM EST), _parse_dt(),
                                  calculate_sl_pnl()

price_feeds/
  price_stream_manager.py       Coordinates all four feeds; calculates spread if missing (ask − bid);
                                  stamps price_data["updated_at"] (UTC) from broker tick time (ICMarkets/Exness)
                                  or wall-clock (OANDA/Binance) before calling subscribers;
                                  routes symbols to feeds via SymbolMapper; notifies all subscribers
  streaming_monitor.py          Per-tick signal evaluation; receives all deps via constructor;
                                  react_to_original_signal() as module-level function;
                                  _MAX_TICK_AGE_SECONDS = 5 — drops stale ticks before signal evaluation;
                                  check order documented below
  alert_system.py               Persistent embed orchestrator; 4 data dicts; 5-channel routing;
                                  15 s live-refresh background task; delegates embed building and archiving
  embed_builders.py             Pure functions: _build_signal_embed(), _build_profit_archive_embed(),
                                  _set_archive_footer() + formatting helpers
  archive_manager.py            ArchiveManager — schedule_end_state_move(), cancel_pending_move(),
                                  delayed move/delete for finished signals
  _base_config.py               BaseThresholdConfig — shared JSON load/save/validate/override management;
                                  shared SymbolMapper singleton across all config types
  alert_config.py               AlertDistanceConfig(BaseThresholdConfig) — approaching distance thresholds
  tp_config.py                  TPConfig(BaseThresholdConfig) — per-signal-type TP thresholds (type_defaults / type_overrides);
                                  calculate_pnl(); accepts signal_type kwarg (or legacy scalp bool)
  tp_monitor.py                 AutoTPMonitor — checks HIT-status signals per tick; per-signal limit cache
  nm_config.py                  NMConfig(BaseThresholdConfig) — max_proximity + base_bounce per asset class
  nm_monitor.py                 NearMissMonitor — in-memory NMTrackingState; _nm_immune set;
                                  mark_immune(signal_id) called on every reactivation;
                                  signal.type == "swing" short-circuits NM (swing is not near-missable)
  feed_health_monitor.py        Stale threshold 300 s; 3 max reconnect attempts; 120 s startup grace;
                                  15 min alert cooldown; DMs health_alert_admin_id from settings.json;
                                  spread hour (17–18 ET) treated as market-closed for forex/metals/indices/oil;
                                  first_stale_time tracks real stall start for accurate recovery downtime;
                                  all knobs are module-level constants (no config file)
  live_price_writer.py          Writes bid/ask/feed to live_prices table every 5 s (OANDA/Binance/Exness);
                                  also reads ICMarkets last_prices at flush time and writes ic_bid/ic_ask
                                  in the same UPSERT row (used by EX offset calculator)
  symbol_mapper.py              Internal ↔ feed-specific symbol translation; always returns UPPERCASE
  feeds/
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

discord_handlers/
  message_handler.py            Handles new/edited/deleted messages; dispatches reply commands for
                                  both alert embeds (any user) and signal messages (author/admin only);
                                  _handle_overlap_prompt() — 30 s reaction prompt when new signal overlaps
                                  an existing one (✅ cancel old / ❌ keep both / timeout = cancel old)

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
    lifecycle.py                !active, !info, !setstatus [--force], !profit, !hit,
                                  !stoploss, !cancel (+ bulk), !setexpiry, !breakeven
    reports.py                  !report (performance statistics generator; partitions into
                                  Regular / PA / Legends by channel name)
    news.py                     !news, !newslist, !newsclear
  config/
    __init__.py                 Cog setup: ThresholdsCog
    thresholds.py               !tp (per-type targeting: `!tp set <type> <asset_class> <value>`,
                                  `!tp set <symbol> <value> [--type=X]`, `!tp config <type|symbol>`,
                                  `!tp remove <symbol> [--type=X]`); !alertdist, !nmconfig

utils/
  logger.py                     Rotating logs: bot.log (10 MB×5) + errors.log (5 MB×3); UTF-8; LOG_LEVEL env
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

2. News-mode gate
   news_manager.is_news_active_for(instrument)
   └─ News active AND would trigger → send_news_cancel_alert()
      Edits embed if one exists; standalone message only if no embed yet.

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

7. Auto-TP check (HIT-status signals only)
   tp_monitor.check_signal(signal, bid, ask)
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
| type | TEXT DEFAULT 'standard'; CHECK (standard, scalp, swing, toll, pa, 1-1) |
| first_limit_hit_time | TIMESTAMPTZ |
| closed_at / closed_reason | TIMESTAMPTZ / TEXT (`automatic` / `manual` / `expiry`) |
| tp_price | DOUBLE PRECISION; market price at which auto-TP fired; NULL for manual profit / SL / other closures |
| total_limits / limits_hit | INTEGER |

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

### performance_metrics
Daily aggregates per instrument (total, profitable, breakeven, stop_loss, cancelled, win_rate). UNIQUE(date, instrument).

### live_prices
`symbol TEXT PK`, bid, ask, feed, updated_at. `ic_bid DOUBLE PRECISION` / `ic_ask DOUBLE PRECISION` (nullable) — ICMarkets prices written at the same flush as the OANDA/Binance row so the EX offset calculator sees both prices from the same timestamp (no inter-fetch drift). Written every 5 s by `LivePriceWriter`.

### feed_health
`feed TEXT PRIMARY KEY` (`icmarkets` / `oanda` / `binance` / `exness`), `status TEXT` (`idle` / `healthy` / `degraded` / `down`), `stale_seconds INTEGER`, `last_seen TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`. Upserted by `FeedHealthMonitor._write_feed_health()` on every status transition. Read by the EX bot each cycle to skip placement on stale feeds.

### bot_mode_status
Singleton row (id=1, enforced by CHECK). `news_mode BOOLEAN`, `spread_hour BOOLEAN`. Updated in real-time by streaming_monitor on spread-hour state transitions.

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
| `signal_finished_messages` | signal_id → archived copy in finished-signals/profit channel |
| `alert_messages` | message_id_str → signal_id (bounded; for reply-handler lookup) |

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
`start_live_updates()` refreshes all active embeds every **15 seconds** with current price and distance. Stopped when a signal closes or is cancelled.

### Key public methods
- `send_approaching_alert(signal, limit, current_price, distance_formatted, spread, spread_buffer_enabled)` — creates embed; registers for live updates
- `send_limit_hit_alert(signal, limit, current_price, spread, spread_buffer_enabled)` — edits embed; sends ping
- `send_stop_loss_alert(signal, current_price)` — edits embed; sends ping; unregisters live updates
- `send_auto_tp_alert(signal, hit_limits, last_pnl, tp_config, cumulative_pnl, limit_pnl_map)` — edits embed; sends ping
- `send_near_miss_cancel_alert(signal, nm_state)` — edits embed; sends ping
- `update_signal_message(signal, event, limits, current_price, ping_text)` — generic editor
- `update_embed_for_signal_id(signal_id, event, ping_text)` — fetches signal, calls `update_signal_message`; safe to call from anywhere
- `reactivate_embed(signal, ping_text)` — rebuilds embed for reactivated signals with live price/distance
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

### Pydantic models with dict-protocol
`SignalData` and `LimitData` in `models/signal.py` have `__getitem__`, `__setitem__`, `get`, `__contains__`, and `pop` methods so existing dict-style access (`signal["instrument"]`) continues to work alongside attribute access (`signal.instrument`). New code should prefer attribute access.

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

### Windows-only MT5
`MetaTrader5` package requires Windows. On Linux/Mac, the ICMarkets and Exness feeds will be unavailable. The bot handles this gracefully but loses those feeds.

### Dual MT5 terminals (ICMarkets + Exness)
The `MetaTrader5` Python package is process-global — `mt5.initialize()` can only connect to one terminal at a time. The ICMarkets feed runs in the main process; the Exness feed runs in a **child process** (`exness_worker.py`) spawned via `asyncio.create_subprocess_exec`. Communication is via JSON lines over stdin (commands) and stdout (prices). Both terminals must be running on the VPS before the bot starts. The `MT5_PATH` and `EXNESS_MT5_PATH` env vars should both be set to avoid auto-discovery ambiguity.

### Exness oil symbol mapping
Internal symbol `USOILSPOT` maps to `USOILm` on Exness MT5. The mapping is defined in both `symbol_mappings.json` (`symbol_mappings.exness.specific_mappings`) and the reverse direction (`reverse_mappings.exness`). Both sections are required — without the reverse mapping, prices arrive under `USOILM` instead of `USOILSPOT` and don't match signals.

### Spread/news cancel behavior
Spread-hour and news cancels **edit the persistent embed** when one already exists. They only fall back to standalone messages if no embed has been created yet for that signal.

### Signal type taxonomy
`signals.type` ∈ `{standard, scalp, swing, toll, pa, 1-1}`. Determined by `pattern_parsers.get_signal_type(text, channel_name)`:
- `CHANNEL_TYPE_MAP` wins first: `scalps` → scalp; `swing-trades` → swing; `gold-tolls-map`/`general-tolls`/`oil-tolls` → toll; `gold-pa-signals`/`price-action-trades` → pa; `gold-1-1-rr` → 1-1.
- Otherwise body keyword: `\bswing\b` → swing, `\bscalp\b` → scalp.
- Default: standard.

Each type has its own `tp_configuration.json` defaults under `type_defaults[<type>]` and per-symbol overrides under `type_overrides[<type>]`. Default initialization: scalp/standard kept as before; toll initialized from scalp; pa initialized from standard; swing = 3× standard; 1-1 metals = $10. The TP resolution order is: per-type symbol override → standard symbol override → per-type asset-class default → standard asset-class default → hard fallback ($5).

### Swing is not near-missable
`NearMissMonitor.update` short-circuits when `signal.type == "swing"`. Swing signals can only close via hit, profit, SL, or manual cancel.

### Gold-tolls SL offset is configurable
`get_gold_tolls_sl_offset()` reads `settings.json` via `load_settings()` → `BotSettings.gold_tolls_sl_offset` (default `5.0`) with a 30 s cache. Change it via `!goldtollssl <value>` or edit `settings.json` directly.

### `stop_loss` is required in DB
`signals.stop_loss` is `NOT NULL`. Toll channels auto-calculate SL from limits; general-tolls derives SL from message numbers. Any new channel or parse path that doesn't produce a SL value will fail on insert.

### Reply command authorization
- Reply to an **alert embed**: any user can execute reply commands; this includes embeds in the finished-signals channel (e.g. `reactivate` works from there)
- Reply to the **original signal message** (has ✅ reaction): signal author **or** admins only

### Cancel via original signal message reply (no prior alert embed)
When `cancel` is replied to the original signal message and no approaching/hit embed exists yet:
- **Gold-toll channels** (`alert_system.toll_channel_ids`): original message is deleted and a cancellation embed is posted to the finished-signals channel
- **All other channels**: ❌ reaction is added to the original message; it is **not** deleted

### Reactivate when original message is gone
`reactivate_cancelled_signal` in `signal_ops.py` does not use its `parsed_signal` argument — it reactivates purely from DB state. The reactivate reply handler tries to fetch and re-parse the original message but falls back gracefully if it has been deleted (e.g. toll signals after archive move). Pass `None` as `parsed_signal` and reactivation proceeds.

### Message handler runs before prefix commands
`bot.on_message()` calls `message_handler.handle_new_message()` before `await self.process_commands(message)`. Reply commands are fully handled in the message handler and are not registered as bot commands.

### TP logic is per-limit, not fully cumulative
Auto-TP fires when: **last hit limit's P&L ≥ threshold** AND (if 2+ limits hit) **sum of earlier limits' P&L ≥ 0** (epsilon tolerance 1e-9). It is not a simple cumulative P&L check across all limits.

### react_to_original_signal
Module-level function in `price_feeds/streaming_monitor.py`. Called from `streaming_monitor`, `expiry_manager`, and `commands/signals/lifecycle.py` to add emoji reactions to original signal messages.

### Overlap detection on signal save
When a new signal is saved, `signal_ops.get_overlapping_signals()` queries for active/hit signals on the same instrument whose pending-limit price range (`[MIN, MAX]`) intersects the new signal's range. If any are found, `message_handler._handle_overlap_prompt()` is spawned as a background task (non-blocking): it posts a prompt in the same channel, waits up to 30 s for a ✅ (cancel old) or ❌ (keep both) reaction from the signal author or a guild admin. Timeout defaults to cancelling the old signal(s). The prompt is deleted after the outcome.

### Reactivation guard
Before reactivating a cancelled signal via reply command or `!setstatus active`, `signal_ops.check_reactivation_guard()` fetches the signal's cancelled limits and compares them against the current mid-price from `live_prices`. A limit is "past" when the hit condition would fire immediately on reactivation (`long: mid ≤ limit`, `short: mid ≥ limit`). If any limits are past, reactivation is blocked with a clear message listing which limits are stale. Returns `None` (fail-open) if price data is unavailable. Admin override: `!setstatus <id> active --force` (requires `guild_permissions.administrator`).

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

### live_prices ic_bid / ic_ask columns
`live_prices` has two nullable columns `ic_bid` and `ic_ask`. `LivePriceWriter` reads the ICMarkets `last_prices` cache at flush time and writes them in the same UPSERT row as the OANDA/Binance/Exness `bid`/`ask`. Both prices are therefore from the same flush window — the EX offset calculator reads `ic_mid − feed_mid` without a separate MT5 tick fetch, eliminating the 5-second inter-fetch drift. When `ic_bid`/`ic_ask` are NULL (rolling-deploy gap), EX falls back to a live MT5 tick and logs once.

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
LOG_LEVEL=INFO                  # optional
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
