# Discord Trading Alert Bot — Project Overview

## Status: Production (fully operational)

The bot monitors Discord trading channels for signal messages, parses them into structured data, stores them in PostgreSQL on Supabase, streams real-time prices from three feeds, fires approaching/hit/stop-loss/auto-TP/near-miss alerts, manages news and spread-hour windows, and provides a full command suite for signal management and performance reporting.

---

## Technology Stack

- **Language**: Python 3.9+ (Windows — required for MetaTrader5)
- **Discord Library**: discord.py 2.3.0+
- **Database**: PostgreSQL via Supabase (`asyncpg` connection pool)
- **Price Feeds**: ICMarkets/MT5 (streaming), OANDA (streaming), Binance (WebSocket)
- **AI Parsing Fallback**: OpenAI API (disabled by default via `settings.json`)
- **Environment**: `.env` file in project root, loaded with `python-dotenv`

---

## Project Structure

```
main.py                         — Entry point; loads .env, starts bot
core/
  bot.py                        — TradingBot class; init DB, load cogs, start feeds,
                                  connect alert system to message handler;
                                  close() stops expiry_manager, news_manager, monitor, DB
  expiry_manager.py             — Background task (every 5 min): expires stale signals
                                  and updates their persistent embeds
  news_manager.py               — NewsManager: tracks active news windows, auto-cleanup,
                                  instrument-affected logic, parse_news_command(),
                                  stop_cleanup_task() for clean shutdown
  parser/
    __init__.py                 — parse_signal() entry point; ParsedSignal / RejectedSignal
    pattern_parsers.py          — Regex parsing; tolls/general-tolls/scalp logic;
                                  is_scalp(); SCALP_CHANNELS set
    validators.py               — Signal validation
    ai_fallback.py              — OpenAI fallback parser

database/
  __init__.py                   — Loads .env; instantiates global db
  connection.py                 — DatabaseManager base: asyncpg pool, execute/fetch helpers
  database_manager.py           — Extends connection; delegates to BaseOperations
  base_operations.py            — Core CRUD: insert_signal, insert_limits, update_status,
                                  mark_limit_hit, get_performance_stats
  schema.py                     — PostgreSQL DDL (CREATE TABLE / INDEX)
  models.py                     — SignalStatus, LimitStatus, StatusTransitions enums
  signal_operations/
    __init__.py                 — SignalDatabase coordinator class
    crud.py                     — get_signal_with_limits (returns key "id", NOT "signal_id"),
                                  get_signal_by_message_id, update_signal_from_edit,
                                  get_active_signals_detailed, get_hit_limits_for_signal
    lifecycle.py                — cancel_signal_by_message, reactivate_cancelled_signal,
                                  manually_set_signal_status, manually_set_signal_to_hit,
                                  process_limit_hit, expire_old_signals, set_signal_expiry
    analytics.py                — get_dashboard_stats, get_performance_for_period,
                                  get_period_signals_with_results, get_trading_period_range
    utils.py                    — calculate_expiry(), format_time_remaining(), pip helpers

commands/
  base_command.py               — BaseCog: is_admin(), is_command_channel()
  bot_commands.py               — !ping, !help, !price, !feeds, !health, !clear,
                                  !reload, !shutdown
  trading_commands.py           — All signal commands (see Commands section)

discord_handlers/
  message_handler.py            — Parses new messages; handles edits/deletes;
                                  reply commands on signal messages and alert embeds

price_feeds/
  price_stream_manager.py       — Manages all three feeds; routes prices to monitor
  streaming_monitor.py          — Compares prices vs active signal limits; fires alerts;
                                  handles spread-hour and news-mode auto-cancels;
                                  integrates NearMissMonitor on every price tick
  alert_system.py               — Persistent embed system (see Alert System section)
  alert_config.py               — Per-asset alert distance thresholds and overrides
  tp_config.py                  — TPConfig: per-asset TP thresholds (normal + scalp);
                                  calculate_pnl(); set_override(); set_default()
  tp_monitor.py                 — AutoTPMonitor: checks hit limits against TP threshold,
                                  triggers auto-profit
  nm_config.py                  — NMConfig: per-asset near-miss thresholds (linear bounce
                                  model); max_proximity + base_bounce per asset class;
                                  per-symbol overrides; editable at runtime via !nmconfig
  nm_monitor.py                 — NearMissMonitor: tracks approaching signals tick-by-tick;
                                  fires auto-cancel when price bounces without hitting;
                                  state is in-memory, rebuilt on restart;
                                  NM-immune set prevents re-fire after reactivation
  feed_health_monitor.py        — Stale-data detection, auto-reconnect, admin DM alerts
  symbol_mapper.py              — Translates instrument names across feed formats
  feeds/
    icmarkets_stream.py         — MT5 polling feed (Windows only)
    oanda_stream.py
    binance_stream.py

utils/
  logger.py                     — Rotating file logger (bot.log + errors.log, UTF-8)
  config_loader.py              — JSON config reader
  embed_factory.py              — Standardised Discord embed builder
  formatting.py                 — Price/pip formatting helpers

config/
  channels.json                 — Monitored channel IDs, alert/command/profit channel IDs,
                                  per-channel defaults (instrument, expiry type)
  settings.json                 — Global bot settings (prefix, spread_buffer_enabled, etc.)
  tp_configuration.json         — TP thresholds per asset class (normal + scalp defaults,
                                  per-symbol overrides); edited at runtime via !tp commands
  alert_distances.json          — Alert distance thresholds; edited via !alertdist commands
  nm_configuration.json         — Near-miss thresholds (max_proximity, base_bounce per
                                  asset class + overrides); edited via !nmconfig commands
  news_events.json              — Persisted active news events (written at runtime)
  expiry_config.json            — Expiry type definitions
  health_config.json            — Feed health monitor settings; admin_user_id for DM alerts;
                                  market hours per asset class; US market holidays
  symbol_mappings.json          — Feed-specific symbol translations
  tracking_config.json          — Internal tracking settings
.env                            — DISCORD_BOT_TOKEN, SUPABASE_DB_URL, OANDA_API_KEY,
                                  OANDA_ACCOUNT_ID, OANDA_PRACTICE, OPENAI_API_KEY
```

---

## Database Schema (PostgreSQL / Supabase)

All PKs use `BIGINT GENERATED ALWAYS AS IDENTITY`. Timestamps are `TIMESTAMPTZ`. Row Level Security: deny-all for `anon`/`authenticated`, full access for `service_role`. Use Supabase **Session Pooler** URL (port 5432).

### signals
| Column | Type | Notes |
|---|---|---|
| id | BIGINT IDENTITY PK | Referenced as `signal['id']` from DB queries |
| message_id | TEXT UNIQUE | Discord message ID or `manual_xxx` |
| channel_id | TEXT | Source channel |
| instrument | TEXT | e.g. GBPUSD, XAUUSD, BTCUSDT |
| direction | TEXT | `long` / `short` |
| stop_loss | DOUBLE PRECISION | Auto-set for toll channels |
| expiry_type | TEXT | `day_end` / `week_end` / `month_end` / `no_expiry` |
| expiry_time | TIMESTAMPTZ | Calculated from expiry_type |
| status | TEXT | See lifecycle below |
| scalp | BOOLEAN | True if signal is from a scalp channel or message |
| first_limit_hit_time | TIMESTAMPTZ | |
| closed_at | TIMESTAMPTZ | |
| closed_reason | TEXT | `automatic` / `manual` |
| result_pips | DOUBLE PRECISION | |
| total_limits | INTEGER | |
| limits_hit | INTEGER | |
| created_at / updated_at | TIMESTAMPTZ | |

### limits
| Column | Type | Notes |
|---|---|---|
| id | BIGINT IDENTITY PK | |
| signal_id | BIGINT FK → signals | CASCADE delete |
| price_level | DOUBLE PRECISION | Entry price |
| sequence_number | INTEGER | Order of limits (1, 2, 3…) |
| status | TEXT | `pending` / `hit` / `cancelled` |
| hit_time / hit_price | TIMESTAMPTZ / DOUBLE PRECISION | |
| approaching_alert_sent | BOOLEAN | Deduplication flag; also gates NM tracking |
| hit_alert_sent | BOOLEAN | Deduplication flag |

### status_changes (audit trail)
| Column | Type |
|---|---|
| id | BIGINT IDENTITY PK |
| signal_id | BIGINT FK → signals |
| old_status / new_status | TEXT |
| change_type | TEXT (`automatic` / `manual`) |
| reason | TEXT |
| changed_at | TIMESTAMPTZ |

### performance_metrics
Aggregated daily stats per instrument: total, profitable, breakeven, stop_loss, cancelled, win_rate. Unique constraint on `(date, instrument)`.

---

## Signal Lifecycle

```
ACTIVE → HIT → PROFIT
               BREAKEVEN
               STOP_LOSS
       → CANCELLED (reversible → ACTIVE or HIT via reactivate)
```

- **ACTIVE**: Created, no limits hit yet
- **HIT**: First limit triggered (auto-TP monitoring begins)
- **PROFIT / BREAKEVEN / STOP_LOSS**: Final closed states
- **CANCELLED**: Voided or expired; reactivatable

Status transitions are validated in `models.py`. `lifecycle.py` uses direct `conn.execute` for manual admin overrides that bypass validation.

---

## Alert System — Persistent Embeds

**One persistent Discord message (embed) per signal.** All events edit this same message in-place rather than sending new messages, keeping channels tidy.

### Key data structures (in `AlertSystem`)
- `signal_messages: Dict[int, discord.Message]` — `signal_id → persistent embed message`
- `signal_ping_messages: Dict[int, discord.Message]` — `signal_id → most recent ping message`
- `alert_messages: Dict[str, int]` — `message_id_str → signal_id` (bounded to 1000 entries; for reply handler lookup)

### Event flow for each alert
1. A **new ping** is sent as a reply to the persistent embed (old ping deleted first)
2. The **persistent embed is edited** to reflect the new state
3. If no embed exists yet, one is created (first event for that signal)

### Embed events — colors and labels
| Event string | Color | Label |
|---|---|---|
| `approaching` | Orange | 🟡 Approaching |
| `hit` | Green | 🎯 Limit Hit |
| `stop_loss` | Red | 🛑 Stop Loss |
| `auto_tp` | Green | 💰 Auto Take-Profit |
| `profit` | Green | 💰 Profit |
| `breakeven` | Grey | ➖ Breakeven |
| `cancelled` | Grey | ❌ Cancelled |
| `expired` | Grey | ⌛ Expired |
| `spread_hour_cancelled` | Orange | 🕔 Spread Hour — Cancelled |
| `reactivated` | Blue | ♻️ Reactivated |
| `edited` | Blue | 📝 Updated |

### Critical key normalisation
`get_signal_with_limits()` (and most DB queries) returns `signal['id']`, **not** `signal['signal_id']`. The `AlertSystem` methods (`_build_signal_embed`, `_upsert_signal_message`, `update_signal_message`) all expect `signal['signal_id']`. **Always normalise before calling:**
```python
_signal_for_update = dict(signal)
if 'signal_id' not in _signal_for_update:
    _signal_for_update['signal_id'] = _signal_for_update.get('id')
await alert_system.update_signal_message(signal=_signal_for_update, event=embed_event, ping_text=...)
```
Or use the convenience method `update_embed_for_signal_id(signal_id, event, ping_text=None)` which handles fetching and normalisation internally.

### Public methods
- `update_signal_message(signal, event, limits=None, current_price=None, ping_text=None)` — update embed; no-op if no embed exists yet
- `update_embed_for_signal_id(signal_id, event, ping_text=None)` — fetches signal from DB, normalises key, calls `update_signal_message`; safe to call from anywhere
- `send_approaching_alert(...)` / `send_limit_hit_alert(...)` / `send_stop_loss_alert(...)` — called by `StreamingPriceMonitor`
- `send_auto_tp_alert(...)` — called by `AutoTPMonitor`
- `send_near_miss_cancel_alert(signal, nm_state=None)` — edits persistent embed to `cancelled` with near-miss details; called by `StreamingPriceMonitor` via `NearMissMonitor`
- `send_spread_hour_cancel_alert(...)` / `send_news_cancel_alert(...)` — standalone messages (not persistent embed edits)
- `track_alert_message(message_id, signal_id)` — registers a message for reply-handler lookup
- `get_signal_from_alert(message_id)` — reverse lookup for reply handler

### Channel routing
Signals are routed to the correct alert channel based on their source channel:
- `general-tolls` → `general-tolls-alert`
- Any other toll channel → `toll-alert-channel`
- PA channels (name contains "pa" or "price-action") → `pa-alert-channel`
- Everything else → `alert_channel`

---

## Message Handler — Reply Commands

### Replying to the **persistent alert embed**
Any user can reply to an alert embed with a command. The handler:
1. Looks up `signal_id` via `alert_system.get_signal_from_alert(referenced.id)`
2. Executes the DB operation
3. **Deletes the user's reply message** (keeps channel clean)
4. Calls `update_signal_message` with a `ping_text` like `💰 GBPUSD LONG — manually marked as profit (by Username)`

### Replying to the **original signal message** (has ✅ bot reaction)
Only the signal author or admins can reply. Same flow as above but:
1. Signal looked up via `get_signal_by_message_id(referenced.id)` — returns `signal['id']`, not `signal['signal_id']`
2. After DB op, **deletes reply**, normalises key, calls `update_signal_message` with ping

### Supported reply commands (both paths)
| Reply text | Action |
|---|---|
| `cancel`, `nm`, `cancelled` | Cancel signal |
| `profit`, `win`, `tp` | Mark as profit |
| `breakeven`, `be` | Mark as breakeven |
| `sl`, `stop`, `stoploss`, `stop loss` | Mark as stop loss |
| `reactivate`, `reopen`, `active` | Reactivate cancelled signal (also marks NM-immune) |
| `hit` | Mark as hit (starts auto-TP) |

### Signal message edits
When the original sender edits a signal message:
- Re-parses the message
- Updates DB via `update_signal_from_edit`
- Adds ✅ 📝 reactions
- Calls `update_signal_message` with `event="edited"` and a ping saying "signal updated by sender"

### Message deletions
When a signal message is deleted, the signal is cancelled in DB and `update_embed_for_signal_id` is called to update the embed.

---

## Monitored Channels

All defined in `config/channels.json` under `monitored_channels`:

| Channel Name | Notes |
|---|---|
| daily-trade-setup | Default expiry: day_end |
| scalps | Scalp channel (auto-scalp flag), day_end |
| forex-exotics | week_end |
| gold-trades | Instrument: XAUUSD, week_end |
| oil-trades | Instrument: USOILSPOT, week_end |
| indices-trades | week_end |
| crypto-trades | week_end |
| stocks-trades | month_end |
| swing-trades | week_end |
| ot-trade-calls | day_end |
| proper-calls | week_end |
| crypto-alt-trades | month_end |
| price-action-trades | PA channel, day_end |
| gold-pa-signals | PA channel + scalp, Instrument: XAUUSD, day_end |
| gold-tolls-map | **Toll channel** + scalp, XAUUSD, day_end |
| general-tolls | **General-tolls** channel, day_end |

---

## Toll / General-Tolls Channel Special Behaviour

Two distinct modes for channels with "toll" in the name:

### `gold-tolls-map` (and other toll channels, excluding general-tolls)
- All parsed numbers treated as **limits** (no explicit stop loss in message)
- SL **auto-calculated** as ±$5 from outermost limit: long → `min(limits) - 5.0`, short → `max(limits) + 5.0`
- Single-number messages valid (one limit, no SL required)
- Treated as scalp signals

### `general-tolls`
- Standard parsing: SL is the last number (long) or first number (short), limits are the rest
- Does **not** auto-calculate SL
- Treated as scalp signals
- Routed to its own alert channel (`general-tolls-alert`)

Logic lives in `pattern_parsers.py` → `determine_limits_and_stop()`.

---

## Scalp Detection

A signal is flagged as scalp (`signal['scalp'] = True`) if:
1. It comes from a channel in `SCALP_CHANNELS = {'scalps', 'gold-pa-signals', 'gold-tolls-map', 'general-tolls'}`, **or**
2. The message text contains the word `scalp`

Scalp status affects TP thresholds (lower values defined in `scalp_defaults` in `tp_configuration.json`).

---

## Auto Take-Profit (AutoTPMonitor)

Once a signal reaches `HIT` status, `AutoTPMonitor` (`tp_monitor.py`) monitors it on every price tick:
- Fetches hit limits for the signal
- Checks if cumulative P&L across all hit limits meets or exceeds the TP threshold for the instrument (from `TPConfig`)
- If threshold met → marks signal as profit, calls `alert_system.send_auto_tp_alert()`
- TP thresholds are configurable per asset class and per symbol, with separate scalp defaults
- P&L calculated by `TPConfig.calculate_pnl(instrument, direction, entry_price, current_price, scalp)`

---

## Near-Miss Auto-Cancel (NearMissMonitor)

`NearMissMonitor` (`nm_monitor.py`) detects when price approaches a limit closely but then reverses without triggering it — a "near-miss" — and auto-cancels the signal.

### Linear bounce model
Two parameters per asset class / symbol (configured in `nm_configuration.json`):
- **`max_proximity`** — outer gate; price must come within this distance to be tracked at all
- **`base_bounce`** — minimum bounce required regardless of how close price got

```
required_bounce = closest_distance + base_bounce
```

The closer price approached, the less additional bounce is required to confirm the near-miss, but `base_bounce` is always the floor.

### Tracking flow
1. Only signals with `approaching_alert_sent = True` on their first limit are tracked (bot already has an embed)
2. Once price enters the `max_proximity` zone, `closest_distance` is recorded and updated each tick
3. Near-miss confirmed when `current_distance − closest_distance >= required_bounce`
4. On confirmation → signal cancelled in DB, `alert_system.send_near_miss_cancel_alert()` called, embed updated to `cancelled`

### NM immunity
When a cancelled signal is reactivated (via reply or `!setstatus`), it is added to `_nm_immune`. Immune signals are never auto-cancelled by NM again — they can only close via a real hit, profit, stop-loss, or manual cancel.

### State
Entirely in-memory. `NMTrackingState` objects are evicted when a signal closes (any path). State is rebuilt naturally on restart as approaching alerts re-trigger.

---

## News Mode

`NewsManager` (`core/news_manager.py`) tracks scheduled news events that pause signal monitoring for affected instruments.

- Events stored in `config/news_events.json` (persisted across restarts)
- Each event has: `currency` (or special category like `GOLD`, `CRYPTO`), `start_time`, `duration_minutes`, `timezone`
- `is_news_active_for(instrument)` — returns the active `NewsEvent` if one affects the instrument, else `None`
- When a signal triggers during an active news event → auto-cancelled, standalone alert sent via `send_news_cancel_alert()`
- Cleanup task auto-purges expired events, sends alert via `alert_system` when a news window ends
- `stop_cleanup_task()` cancels the background task on bot shutdown

---

## Spread Hour

`StreamingPriceMonitor._is_spread_hour()` detects the daily spread-widening window (17:00–18:00 EST for forex/metals/indices). When active:
- Approaching and hit alerts are suppressed
- Signals that would trigger are auto-cancelled with standalone `send_spread_hour_cancel_alert()`
- Spread buffer: when enabled (`settings.json: spread_buffer_enabled`), the bid/ask spread is added to the display price for approaching/hit events (not stop-loss). `_reload_spread_buffer_setting()` is called on each price update (cached 30 s) so config changes take effect without restart.

---

## Price Feed Architecture

```
ICMarkets (MT5) ─┐
OANDA            ├─→ PriceStreamManager → StreamingPriceMonitor → AlertSystem → Discord
Binance         ─┘         ↓
                      FeedHealthMonitor (checks every 60 s, DMs admin on failure)
```

- **ICMarkets**: Forex and metals via MT5 polling (100 ms per symbol); **Windows only**
- **OANDA**: Forex; `OANDA_PRACTICE=true/false` env var toggles practice/live
- **Binance**: Crypto via WebSocket; US API by default, international via `BINANCE_USE_INTERNATIONAL=true`
- `SymbolMapper` translates between internal format and each feed's format
- `AlertDistanceConfig` (`alert_config.py`): per-asset-class approaching alert thresholds, per-symbol overrides; editable at runtime via `!alertdist`
- `FeedHealthMonitor`: stale threshold 300 s, up to 3 auto-reconnect attempts per feed, 120 s startup grace period; sends DM to `admin_user_id` from `health_config.json`

---

## Commands Reference

All commands use prefix `!`. Commands only processed in the designated command channel (and monitored/alert channels for reply commands).

### General (`bot_commands.py`)
| Command | Description |
|---|---|
| `!ping` | Check bot latency |
| `!help [topic]` | Command list or help for specific topic |
| `!price <instrument>` | Current price from feeds (aliases: `!cp`, `!checkprice`) |
| `!feeds` | Feed connection status (alias: `!feedstatus`) |
| `!health` | Database and bot health stats |
| `!clear` | *(admin)* Clear all signals from DB |
| `!reload` | *(admin)* Reload config from files |
| `!shutdown` | *(admin)* Stop the bot |

### Signal Management (`trading_commands.py`)
| Command | Description |
|---|---|
| `!signal <text>` | Manual signal entry |
| `!active [instrument] [sort:method]` | Active signals (paginated); sort: `recent`/`oldest`/`distance`/`progress` |
| `!info <id>` | Detailed signal info |
| `!delete <id>` | Delete a signal from DB |
| `!setstatus <id> <status>` | Override status (`active`, `hit`, `profit`, `breakeven`, `stop_loss`, `cancelled`); reactivation also marks NM-immune |
| `!profit <id>` | Mark as profit |
| `!hit <id>` | Mark as hit (starts auto-TP) |
| `!stoploss <id>` | Mark as stop loss (alias: `!sl`) |
| `!cancel <id>` | Cancel a signal (alias: `!nm`) |
| `!cancel all <PAIR/CURRENCY>` | Bulk cancel all active signals for a pair or currency |
| `!cancel gold <longs\|shorts\|both> <setups\|pa\|tolls\|everything>` | Bulk cancel gold signals by direction and type |
| `!setexpiry <id> <type>` | Set expiry type (`day_end` / `week_end` / `month_end` / `no_expiry`) |
| `!report [day\|week\|month] [profit\|stoploss]` | Performance report, optionally filtered |

### TP Configuration (`!tp`)
| Subcommand | Description |
|---|---|
| `!tp show [symbol]` | Show current TP thresholds (all or for one symbol) |
| `!tp set <asset_class\|symbol> <value> [pips\|dollars]` | Set TP threshold |
| `!tp remove <symbol>` | Remove per-symbol override |

### Alert Distance Configuration (`!alertdist` / `!adist` / `!alertdistance`)
| Subcommand | Description |
|---|---|
| `!alertdist show [symbol]` | Show alert distance thresholds |
| `!alertdist set <asset_class\|symbol> <value> [pips\|dollars\|percent]` | Set threshold |
| `!alertdist remove <symbol>` | Remove override |

### Near-Miss Configuration (`!nmconfig` / `!nmc` / `!nm_config`)
| Subcommand | Description |
|---|---|
| `!nmconfig show [symbol]` | Show NM thresholds (all or for one symbol) |
| `!nmconfig set <asset_class\|symbol> <max_proximity> <base_bounce> [pips\|dollars]` | Set thresholds |
| `!nmconfig remove <symbol>` | Remove per-symbol override |

### News Mode (`!news`, `!newslist`, `!newsclear`)
| Command | Description |
|---|---|
| `!news <currency> <time> <duration> [timezone]` | Schedule a news event (e.g. `!news USD 14:30 30 EST`) |
| `!newslist` | List all active/upcoming news events (aliases: `!newsstatus`, `!newsmode`) |
| `!newsclear [id]` | Remove a news event by ID, or clear all (aliases: `!newsdel`, `!newsremove`) |

---

## Embed Update Coverage — All Status-Change Paths

Every path that changes signal status also updates the persistent embed:

| Trigger | Embed updated via |
|---|---|
| Reply "profit/sl/cancel/etc" to alert embed | `update_signal_message` (with ping) |
| Reply "profit/sl/cancel/etc" to signal message | `update_signal_message` (with ping) |
| `!setstatus`, `!profit`, `!stoploss`, `!cancel <id>` | `update_embed_for_signal_id` |
| `!hit` command | `update_embed_for_signal_id` |
| `!cancel gold ...` / `!cancel all <pair>` (bulk) | `update_embed_for_signal_id` per signal |
| Signal message deleted by sender | `update_embed_for_signal_id` |
| Auto-expiry (5-min loop) | `update_embed_for_signal_id` per expired signal |
| Limit hit (price feed) | `send_limit_hit_alert` → `_upsert_signal_message` |
| Stop loss hit (price feed) | `send_stop_loss_alert` → `_upsert_signal_message` |
| Auto Take-Profit triggered | `send_auto_tp_alert` → `_upsert_signal_message` |
| Approaching alert (price feed) | `send_approaching_alert` → `_upsert_signal_message` |
| Near-miss auto-cancel (price feed) | `send_near_miss_cancel_alert` → `_upsert_signal_message` (event=`cancelled`) |
| Signal edited by sender | `update_signal_message` with `event="edited"` (with ping) |
| News-mode auto-cancel | `send_news_cancel_alert` (standalone message, not embed edit) |
| Spread-hour auto-cancel | `send_spread_hour_cancel_alert` (standalone message, not embed edit) |

> **Note:** Spread-hour and news cancels send standalone messages rather than editing the persistent embed, as these are system-level events rather than trade outcomes. Near-miss cancels **do** edit the persistent embed (mirrors auto-TP behaviour).

---

## asyncpg Conventions

- `conn.execute()` / `fetch*()` take **unpacked positional args**: `conn.execute(query, $1_val, $2_val)`
- The `db.execute()` / `db.fetch_one()` / `db.fetch_all()` wrappers take a **tuple** and unpack internally with `*params`
- Timestamp columns return native Python `datetime` objects — never pass them to `datetime.fromisoformat()`. Use `_parse_dt()` helpers which handle both datetime objects and ISO strings
- `ROUND(x, 2)` in PostgreSQL requires `NUMERIC` — use `CAST(... AS NUMERIC)` not `CAST(... AS FLOAT)`
- Date/time query parameters must be `datetime` objects, never `.isoformat()` strings

---

## Environment Variables (`.env`)

```
DISCORD_BOT_TOKEN=...
SUPABASE_DB_URL=postgresql://postgres.[ref]:[password]@aws-0-[region].pooler.supabase.com:5432/postgres
OANDA_API_KEY=...
OANDA_ACCOUNT_ID=...
OANDA_PRACTICE=false
OPENAI_API_KEY=...              # optional; AI fallback parser disabled by default in settings.json
BINANCE_USE_INTERNATIONAL=false # optional; set true if binance.us is blocked
```

---

## Key Implementation Notes

- **`database/__init__.py`** calls `load_dotenv()` with an absolute path derived from `__file__` before instantiating `DatabaseManager`, so `.env` is found regardless of working directory. `main.py` does the same.
- **Timeout protection**: All manual status operations in `discord_handlers` wrap DB calls with `asyncio.wait_for(..., timeout=5.0)`.
- **`bot.monitor`** is the `StreamingPriceMonitor` instance. Access alert system via `bot.monitor.alert_system`. The message handler connects to it in `setup_hook` and re-verifies in `on_ready`.
- **`signal['id']` vs `signal['signal_id']`**: DB queries return `id`. The `AlertSystem` needs `signal_id`. Always normalise before calling alert system methods — or use `update_embed_for_signal_id(signal_id, event)` which handles it automatically.
- **Scalp flag**: stored in DB as `signals.scalp` (boolean). Affects TP and NM thresholds. Auto-set at parse time based on channel name or message content.
- **Spread buffer**: controlled by `settings.json → spread_buffer_enabled`. When enabled, the bid/ask spread is added to approaching/hit display prices (not stop-loss). `_reload_spread_buffer_setting()` is called on each price update (cached 30 s) so config changes take effect without restart.
- **News events** persist across restarts via `config/news_events.json`. The cleanup task re-attaches to the alert system after it's initialised in `bot.py`. `stop_cleanup_task()` is called in `bot.close()`.
- **Expiry manager** `stop()` is called in `bot.close()` to cancel the background loop cleanly.
- **Admin IDs**: `bot.py → self.admin_ids` (hardcoded). Guild admins (via `guild_permissions.administrator`) are also treated as admins for signal management.
- **MT5 / ICMarkets**: Windows only. The `MetaTrader5` package cannot be installed on Linux. Deploy on Windows or accept that the ICMarkets feed will be unavailable.
- **NM immunity**: After a signal is reactivated (any path — reply, `!setstatus active`, etc.), `nm_monitor.mark_immune(signal_id)` is called so the near-miss monitor will never auto-cancel it again.