# TM Bot — Codebase Orientation

## Summary

The bot monitors trading-signal messages in designated Discord channels, parses them into structured position data (instrument, direction, entry limits, stop-loss), stores them in PostgreSQL on Supabase, and streams real-time prices from three feeds (ICMarkets/MT5, OANDA, Binance) to fire approaching/hit/stop-loss/auto-TP/near-miss alerts. All events edit a single persistent Discord embed per signal to keep channels tidy. A command suite (`!cancel`, `!setstatus`, `!tp`, `!news`, etc.) manages signal lifecycle and configuration at runtime without restarts.

---

## Technology Stack

- **Language**: Python 3.9+ (Windows required for MetaTrader5)
- **Discord**: discord.py 2.3.0+, `commands.Bot` with cog extensions
- **Database**: PostgreSQL via Supabase; `asyncpg` pool (min 2, max 10 connections, 30 s timeout)
- **Price Feeds**: ICMarkets/MT5 (polling 100 ms/symbol), OANDA (REST streaming), Binance (WebSocket bookTicker)
- **AI Fallback Parser**: OpenAI `gpt-4o-mini`; disabled by default (`enable_openai_fallback: false` in `settings.json`)
- **Config**: `.env` for secrets; JSON files in `config/` for runtime settings

---

## Project Structure

```
main.py                         Entry point; loads .env, creates TradingBot, runs asyncio.run(main())

core/
  bot.py                        TradingBot(commands.Bot); wires all subsystems in setup_hook();
                                  on_message() routes to handler before process_commands();
                                  admin_ids hardcoded here; close() stops all background tasks
  expiry_manager.py             @tasks.loop(5min) — expires ACTIVE/HIT signals past expiry_time;
                                  updates embeds; deletes original messages for toll-channel no-embed signals
  news_manager.py               Tracks active news windows; persists to news_events.json;
                                  cleanup polls every 30 s; parse_news_command() parses !news args
  channel_cleaner.py            @tasks.loop(1min) — bulk-deletes alert-channel messages every
                                  Friday 18:00 local time (7-day window); stop() called in bot.close()
  parser/
    __init__.py                 parse_signal(message, channel_name) entry point;
                                  ParsedSignal / RejectedSignal types; lazy sub-parser init
    pattern_parsers.py          CorePatternParser / StockPatternParser / CryptoPatternParser;
                                  SCALP_CHANNELS; get_gold_tolls_sl_offset() (settings.json, 30 s cache)
    validators.py               is_potential_signal(), should_exclude(), validate_signal(),
                                  detect_channel_type()
    ai_fallback.py              AIFallbackParser (gpt-4o-mini); only runs if pattern fails + flag enabled

database/
  __init__.py                   Loads .env; exposes global db (DatabaseManager) and initialize_signal_db()
  connection.py                 asyncpg pool; execute/fetch helpers; params as $1,$2,... positional args
  database_manager.py           Extends connection; delegates to BaseOperations; re-exports status constants
  base_operations.py            Core CRUD: insert_signal, insert_limits, update_signal_status
                                  (validates transitions), mark_limit_hit, get_active_signals_for_tracking
  models.py                     SignalStatus, LimitStatus, ChangeType, Direction, StatusTransitions
  schema.py                     DDL for all 7 tables + indexes + idempotent migrations
  signal_operations/
    __init__.py                 SignalDatabase coordinator; delegates to crud/lifecycle/analytics
    crud.py                     get_signal_with_limits() returns key 'id' (not 'signal_id');
                                  save_signal(), get_active_signals_detailed_sorted() (sort: recent/oldest/progress)
    lifecycle.py                cancel, reactivate, manually_set_signal_status (bypasses validation),
                                  process_limit_hit, expire_old_signals
    analytics.py                get_statistics(), performance by period
    utils.py                    calculate_expiry() (day_end → 4:45 PM EST), pip helpers, format_time_remaining()

price_feeds/
  price_stream_manager.py       Coordinates all three feeds; calculates spread if missing (ask − bid);
                                  routes symbols to feeds via SymbolMapper; notifies all subscribers
  streaming_monitor.py          Per-tick signal evaluation; check order documented below;
                                  updates bot_mode_status table on spread-hour transitions
  alert_system.py               Persistent embed manager; 4 data dicts; 5-channel routing;
                                  15 s live-refresh background task for active embeds
  alert_config.py               AlertDistanceConfig — approaching distance thresholds; runtime-editable
  tp_config.py                  TPConfig — TP thresholds + calculate_pnl(); runtime-editable
  tp_monitor.py                 AutoTPMonitor — checks HIT-status signals per tick; per-signal limit cache
  nm_config.py                  NMConfig — max_proximity + base_bounce per asset class; runtime-editable
  nm_monitor.py                 NearMissMonitor — in-memory NMTrackingState; _nm_immune set;
                                  mark_immune(signal_id) called on every reactivation
  feed_health_monitor.py        Stale threshold 300 s; 3 max reconnect attempts; 120 s startup grace;
                                  15 min alert cooldown; DMs admin_user_id from health_config.json
  symbol_mapper.py              Internal ↔ feed-specific symbol translation; always returns UPPERCASE
  feeds/
    icmarkets_stream.py         MT5 polling 100 ms/symbol — Windows only
    oanda_stream.py             OANDA REST stream; live/practice via OANDA_PRACTICE env var
    binance_stream.py           WebSocket bookTicker; US (binance.us) or international via BINANCE_USE_INTERNATIONAL

discord_handlers/
  message_handler.py            Handles new/edited/deleted messages; dispatches reply commands for
                                  both alert embeds (any user) and signal messages (author/admin only)

commands/
  base_command.py               BaseCog: is_admin(), is_command_channel()
  bot_commands.py               !ping, !help, !price, !feeds, !health, !clear, !reload, !shutdown,
                                  !cleanalerts, !goldtollssl; license management commands
  trading_commands.py           Signal lifecycle (!setstatus, !cancel, !profit, !hit, !stoploss, !active,
                                  !info, !delete, !signal, !setexpiry, !report); bulk cancels; TP/alertdist/
                                  NM/news config subcommands

utils/
  logger.py                     Rotating logs: bot.log (10 MB×5) + errors.log (5 MB×3); UTF-8; LOG_LEVEL env
  config_loader.py              JSON config read/write helpers
  embed_factory.py              Standardized Discord embed builder
  formatting.py                 Price/pip/distance formatting helpers

config/                         (see Configuration Files section)
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
| id | PK — returned as `signal['id']` from all DB queries |
| message_id | TEXT UNIQUE; `manual_xxx` for manually entered signals |
| channel_id | TEXT |
| instrument | TEXT (e.g. GBPUSD, XAUUSD, BTCUSDT) |
| direction | TEXT (`long` / `short`) |
| stop_loss | DOUBLE PRECISION NOT NULL |
| expiry_type | TEXT (`day_end` / `week_end` / `month_end` / `no_expiry`) |
| expiry_time | TIMESTAMPTZ; `day_end` resolves to 4:45 PM EST |
| status | TEXT; CHECK (active, hit, profit, breakeven, stop_loss, cancelled) |
| scalp | BOOLEAN DEFAULT FALSE |
| first_limit_hit_time | TIMESTAMPTZ |
| closed_at / closed_reason | TIMESTAMPTZ / TEXT (`automatic` / `manual` / `expiry`) |
| result_pips | DOUBLE PRECISION |
| total_limits / limits_hit | INTEGER |

### limits
| Column | Notes |
|--------|-------|
| id | PK |
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
`symbol TEXT PK`, bid, ask, feed, updated_at. Written on every price tick by streaming_monitor.

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

Valid transitions from `models.py::StatusTransitions.VALID_TRANSITIONS`:
```python
'active':    ['hit', 'cancelled', 'stop_loss']
'hit':       ['profit', 'breakeven', 'stop_loss', 'cancelled']
'cancelled': ['hit', 'active']          # reactivation
'profit':    ['cancelled']              # admin correction only
'breakeven': ['cancelled']
'stop_loss': ['cancelled']
```

`base_operations.update_signal_status()` validates transitions and writes audit record.
`lifecycle.manually_set_signal_status()` bypasses validation — used by admin commands.

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
- `update_signal_message(signal, event, limits, current_price, ping_text)` — generic editor; **requires `signal['signal_id']` key**
- `update_embed_for_signal_id(signal_id, event, ping_text)` — fetches signal, normalizes key, calls `update_signal_message`; safe to call from anywhere
- `reactivate_embed(signal, ping_text)` — rebuilds embed for reactivated signals with live price/distance
- `track_alert_message(message_id, signal_id)` / `get_signal_from_alert(message_id)` — reply-handler lookup

---

## Critical Conventions / Gotchas

### `signal['id']` vs `signal['signal_id']`
Every DB query (`get_signal_with_limits`, `get_signal_by_message_id`, etc.) returns a dict with key `'id'`. `AlertSystem.update_signal_message()` and `_build_signal_embed()` expect `'signal_id'`. Always normalize before calling:
```python
sig = dict(signal)
if 'signal_id' not in sig:
    sig['signal_id'] = sig.get('id')
await alert_system.update_signal_message(signal=sig, event=..., ping_text=...)
```
Or use `update_embed_for_signal_id(signal_id, event)` which handles this automatically.

### asyncpg parameter passing
- Raw `conn.execute(query, val1, val2)` — positional splat, `$1`/`$2`/... placeholders
- Wrapper `db.execute(query, (val1, val2))` — takes a **tuple**, unpacks with `*params` internally
- Timestamps come back as native `datetime` objects — never pass to `datetime.fromisoformat()`. Use `_parse_dt()` helpers (handle both `datetime` and ISO strings)
- `ROUND(x, 2)` in PostgreSQL requires `CAST(... AS NUMERIC)`, not `CAST(... AS FLOAT)`

### NM immunity after reactivation
`nm_monitor.mark_immune(signal_id)` is called whenever a cancelled signal is reactivated (any path: reply command, `!setstatus active`, `!reactivate`). Immune signals skip NM checks permanently for that signal's lifetime and can only close via hit, profit, SL, or manual cancel.

### Windows-only MT5
`MetaTrader5` package requires Windows. On Linux/Mac, the ICMarkets feed will be unavailable. The bot handles this gracefully but loses that feed.

### Spread/news cancel behavior
Spread-hour and news cancels **edit the persistent embed** when one already exists. They only fall back to standalone messages if no embed has been created yet for that signal. The pattern "send standalone, not embed edit" in older docs is incorrect.

### `SCALP_CHANNELS` has 5 members
`pattern_parsers.SCALP_CHANNELS = {'scalps', 'gold-pa-signals', 'gold-tolls-map', 'general-tolls', 'oil-tolls'}`. Any code that lists only 4 channels is missing `'oil-tolls'`.

### Gold-tolls SL offset is configurable
`get_gold_tolls_sl_offset()` reads `settings.json['gold_tolls_sl_offset']` (default `5.0`) with a 30 s cache. It is **not** hardcoded. Change it via `!goldtollssl <value>` or edit `settings.json` directly (takes effect within 30 s).

### `stop_loss` is required in DB
`signals.stop_loss` is `NOT NULL`. Toll channels auto-calculate SL from limits; general-tolls derives SL from message numbers. Any new channel or parse path that doesn't produce a SL value will fail on insert.

### Reply command authorization
- Reply to an **alert embed**: any user can execute reply commands
- Reply to the **original signal message** (has ✅ reaction): signal author **or** admins only

### Message handler runs before prefix commands
`bot.on_message()` calls `message_handler.handle_new_message()` before `await self.process_commands(message)`. Reply commands are fully handled in the message handler and are not registered as bot commands.

### TP logic is per-limit, not fully cumulative
Auto-TP fires when: **last hit limit's P&L ≥ threshold** AND (if 2+ limits hit) **sum of earlier limits' P&L ≥ 0** (epsilon tolerance 1e-9). It is not a simple cumulative P&L check across all limits.

---

## Configuration Files

| File | Contents | Runtime-editable |
|------|----------|-----------------|
| `channels.json` | 18 monitored channel IDs; alert/command/profit/finished channel IDs; per-channel defaults (instrument, expiry type) | No (restart needed for new channels) |
| `settings.json` | `bot_prefix`, `spread_buffer_enabled`, `enable_openai_fallback`, `gold_tolls_sl_offset`, `debug_mode`, `license_role_name` | Yes (30 s cache for some) |
| `tp_configuration.json` | TP thresholds per asset class + per-symbol overrides; `scalp_defaults` | Yes (`!tp set`) |
| `alert_distances.json` | Approaching alert distances per asset class + overrides | Yes (`!alertdist set`) |
| `nm_configuration.json` | NM max_proximity + base_bounce per asset class + overrides | Yes (`!nmconfig set`) |
| `news_events.json` | Active/upcoming news windows; persisted across restarts | Yes (written by `!news`) |
| `expiry_config.json` | Expiry type definitions and daily close times | No |
| `health_config.json` | Feed health thresholds; `admin_user_id` for DM alerts; market hours per asset class; US holidays | No |
| `symbol_mappings.json` | Feed-specific symbol name translations | No |
| `tracking_config.json` | Update interval thresholds per distance bucket (critical 1 s, near 5 s, medium 30 s, far 60 s) | No |

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
LOG_LEVEL=INFO                  # optional
```

`database/__init__.py` calls `load_dotenv()` with an absolute path derived from `__file__` before instantiating `DatabaseManager`, so `.env` is found regardless of working directory. `main.py` does the same.
