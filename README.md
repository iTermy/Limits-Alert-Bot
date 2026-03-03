# Limits Alert Bot

A Discord bot for monitoring trading signal channels. It parses signal messages, tracks entry limits against live price feeds, and fires approaching/hit/stop-loss/take-profit alerts in real time.

---

## What it does

The bot watches a set of configured Discord channels for trading signal messages. When a signal is posted, it extracts the instrument, direction, entry limits, and stop loss, stores them in a PostgreSQL database, and begins monitoring live prices from three feeds simultaneously. When price reaches a configured threshold near a limit, or crosses it entirely, the bot posts an alert embed in the designated alert channel and keeps it updated in-place as the trade progresses.

Key behaviours:

- **Real-time price monitoring** via ICMarkets (MT5), OANDA, and Binance WebSocket feeds
- **Persistent alert embeds** — one embed per signal, edited in-place on every status change rather than flooding the channel with new messages
- **Auto take-profit** — once a signal is hit, P&L is tracked on every price tick and the signal is automatically closed when the configured TP threshold is reached
- **News mode** — scheduled news windows suppress alerts and auto-cancel signals for affected instruments during the event
- **Spread hour handling** — approaching and hit alerts are suppressed during the daily spread-widening window; signals that would trigger are auto-cancelled with a notification
- **Full audit trail** — every status change is recorded in a `status_changes` table with timestamp, reason, and whether it was automatic or manual
- **Feed health monitoring** — stale feed detection with automatic reconnection attempts and admin DM alerts on failure

---

## Signal lifecycle

```
ACTIVE → HIT → PROFIT
                BREAKEVEN
                STOP_LOSS
       → CANCELLED  (reversible → ACTIVE or HIT)
```

Signals expire automatically based on their configured expiry type (`day_end`, `week_end`, `month_end`, or `no_expiry`). Expired signals are cancelled with `closed_reason = automatic`.

---

## Price feeds

| Feed | Instruments |
|---|---|
| ICMarkets (MT5) | Forex, metals |
| OANDA | Forex |
| Binance | Crypto |

MT5 requires a Windows environment with the MetaTrader 5 terminal installed and running. OANDA supports both practice and live modes via the `OANDA_PRACTICE` environment variable.

---

## Channel types

The bot distinguishes several channel categories, each with different parsing and routing behaviour:

- **Standard channels** — full signal parsing with explicit stop loss
- **Scalp channels** — lower TP thresholds; scalp flag stored on the signal
- **PA (price action) channels** — routed to a separate alert channel
- **Toll channels** — limits-only parsing; stop loss auto-calculated from outermost limit
- **General-tolls** — standard SL parsing but treated as scalp; routed to its own alert channel

---

## Commands

All commands use the `!` prefix and are only processed in the designated command channel, with the exception of reply commands which work in monitored and alert channels.

### General

| Command | Description |
|---|---|
| `!ping` | Check bot latency |
| `!help [topic]` | Command list or help for a specific topic |
| `!price <instrument>` | Current price from feeds |
| `!feeds` | Feed connection status |
| `!health` | Database and bot health stats |
| `!reload` | *(admin)* Reload config from files |
| `!shutdown` | *(admin)* Stop the bot |
| `!clear` | *(admin)* Clear all signals from the database |

### Signal management

| Command | Description |
|---|---|
| `!signal <text>` | Manually enter a signal |
| `!active [instrument] [sort:method]` | List active signals (paginated); sort by `recent`, `oldest`, `distance`, or `progress` |
| `!info <id>` | Detailed signal info |
| `!delete <id>` | Delete a signal |
| `!setstatus <id> <status>` | Override signal status |
| `!profit <id>` | Mark as profit |
| `!hit <id>` | Mark as hit (starts auto-TP monitoring) |
| `!stoploss <id>` | Mark as stop loss |
| `!cancel <id>` | Cancel a signal |
| `!cancel all <PAIR/CURRENCY>` | Bulk cancel all active signals for a pair or currency |
| `!cancel gold <longs\|shorts\|both> <setups\|pa\|tolls\|everything>` | Bulk cancel gold signals by direction and type |
| `!setexpiry <id> <type>` | Set expiry type |
| `!report [day\|week\|month] [profit\|stoploss]` | Performance report |

### TP configuration

| Command | Description |
|---|---|
| `!tp show [symbol]` | Show current TP thresholds |
| `!tp set <asset_class\|symbol> <value> [pips\|dollars]` | Set a TP threshold |
| `!tp remove <symbol>` | Remove a per-symbol override |

### Alert distance configuration

| Command | Description |
|---|---|
| `!alertdist show [symbol]` | Show alert distance thresholds |
| `!alertdist set <asset_class\|symbol> <value> [pips\|dollars\|percent]` | Set a threshold |
| `!alertdist remove <symbol>` | Remove a per-symbol override |

### News mode

| Command | Description |
|---|---|
| `!news <currency> <time> <duration> [timezone]` | Schedule a news event (e.g. `!news USD 14:30 30 EST`) |
| `!newslist` | List all active/upcoming news events |
| `!newsclear [id]` | Remove a news event by ID, or clear all |

### Reply commands

Replying to an alert embed or the original signal message (bot-reacted with ✅) with any of the following triggers the corresponding action without needing a command channel:

`profit` / `win` / `tp` — `sl` / `stop` / `stoploss` — `cancel` / `nm` — `breakeven` / `be` — `hit` — `reactivate` / `reopen` / `active`

---

## Configuration files

All configuration lives in the `config/` directory and is loaded at startup. Most files can be reloaded at runtime with `!reload`; TP and alert distance configs are also editable via their respective commands.

| File | Purpose |
|---|---|
| `channels.json` | Monitored channel IDs, alert/command/profit channel IDs, per-channel defaults |
| `settings.json` | Global bot settings (prefix, spread buffer, debug mode, etc.) |
| `tp_configuration.json` | TP thresholds per asset class, with per-symbol overrides |
| `alert_distances.json` | Approaching alert distance thresholds |
| `expiry_config.json` | Expiry type definitions |
| `symbol_mappings.json` | Feed-specific symbol name translations |
| `health_config.json` | Feed health monitor settings and market hours |
| `news_events.json` | Active news events (written at runtime, persists across restarts) |
| `nm_configuration.json` | Near-miss monitor thresholds per asset class |

---

## Tech stack

- Python 3.9+
- discord.py 2.3.0+
- PostgreSQL via Supabase (`asyncpg` connection pool)
- aiohttp (OANDA and Binance streaming)
- MetaTrader5 (ICMarkets feed, Windows only)
- OpenAI API (optional AI parsing fallback, disabled by default)
