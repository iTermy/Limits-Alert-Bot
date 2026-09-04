# TM Bot — Trading Signal Monitor

A Discord bot for monitoring trading-signal channels. It parses signal messages
posted in monitored channels, tracks entry limits against live price feeds, and
fires approaching / hit / stop-loss / take-profit alerts in real time — editing a
single persistent embed per signal.

A companion per-user execution bot (separate repository) reads the same database
and mirrors these signals onto MetaTrader 5 broker accounts.

---

## What it does

The bot watches a set of configured Discord channels for signal messages. When a
signal is posted, it extracts the instrument, direction, entry limits, and stop
loss, stores them in PostgreSQL, and begins monitoring live prices from four feeds.
As price approaches or crosses a limit, the bot posts an alert embed in the
designated channel and edits it in place as the trade moves through its lifecycle.

Key behaviours:

- **Four price feeds** — ICMarkets (MT5), OANDA, Binance, and Exness (MT5, run in
  an isolated child process since the MetaTrader5 package is process-global). A
  symbol mapper routes each instrument to the right feed and translates symbol
  formats in both directions.
- **Channel-aware parsing** — pattern parsers per channel family (core forex /
  metals / indices, stocks via MT5 symbol lookup, crypto with auto-`USDT` tickers),
  per-channel defaults, typo detection via limit-ordering validation, and an
  optional AI fallback parser.
- **Persistent alert embeds** — one embed per signal, edited in place on every
  event. Live-price refreshes run as one sequential, best-effort snapshot pass,
  followed by a 30 s cooldown. Slow passes are never refilled, and individual
  cosmetic edits time out after 8 s so they cannot monopolize Discord's shared
  channel bucket. Each refresh also takes a slot from a sliding-window budget of
  what the bot has recently sent to that channel, so passes stay under Discord's
  per-channel limit rather than being throttled by it; event alerts record their
  spend against the same window but never wait on it. Embed references are stored in the database so restarts
  re-attach to existing embeds instead of orphaning them.
- **Auto take-profit** — once a signal is hit, P&L is tracked on every tick and the
  signal closes automatically when the configured TP threshold is reached.
- **News and spread-hour handling** — scheduled news windows and the daily
  spread-widening window suppress alerts and auto-cancel affected signals. Add
  `dryrun` to a news command to pause clients while the alert bot continues normally.
- **Connection health monitoring** — stale-feed detection with targeted reconnects,
  a price-flow watchdog, and a Discord gateway/REST watchdog that restarts the bot
  after a prolonged network failure.
- **Runtime configuration** — TP thresholds, alert distances, and near-miss rules
  are adjustable through commands without a restart; every change is appended to a
  `config_history` audit table.
- **Full audit trail** — every status change is recorded in a `status_changes`
  table with timestamp, reason, and whether it was automatic or manual.

---

## Signal lifecycle

```
ACTIVE → HIT → PROFIT
                BREAKEVEN
                STOP_LOSS
       → CANCELLED  (reversible → ACTIVE or HIT)
```

Signals expire automatically based on their configured expiry type (`day_end`,
`week_end`, `month_end`, or `no_expiry`). A hit signal rolls its expiry forward to
the next window instead of being cancelled, so an open position is never dropped.

The full per-tick check order, state machine, and restart-recovery semantics are
documented in [CLAUDE.md](CLAUDE.md), which serves as the maintainer reference.

---

## Tech stack

- Python 3.9+ (Windows required for the MetaTrader5 feeds)
- discord.py 2.3+, `commands.Bot` with cog extensions
- PostgreSQL (Supabase) via `asyncpg`
- Pydantic v2 for domain models (`SignalData`, `LimitData`, `BotSettings`)
- OpenAI API (optional AI parsing fallback, disabled by default)
- ruff and pytest (CI via GitHub Actions)

---

## Project layout

```
main.py                  Entry point + in-process restart supervisor
core/                    Bot wiring, signal parser, news manager, expiry manager
  parser/                Channel-aware pattern parsers + validators + AI fallback
database/                asyncpg pool, schema/migrations, signal CRUD + lifecycle,
                         reporting queries, audit ops
price_feeds/             Grouped into feeds/ (feed clients + stream coordination),
                         alerting/ (embeds + archiving), monitors/ (streaming
                         evaluation, TP/near-miss/excursion/trailing, health, guards),
                         and config/ (threshold configs + symbol mapper)
discord_handlers/        Message intake: parsing, edits/deletes, reply commands
commands/                Cogs: signal lifecycle, reports, news, config, admin
models/                  Pydantic domain models + status enums
config/                  Runtime JSON configuration
tests/                   Pure-logic test suite (parser, TP math, state machine, …)
```

---

## Setup

1. Python 3.9+ on Windows. Both MT5 terminals (ICMarkets and Exness) must be
   installed and logged in for their feeds; the bot degrades gracefully without them.
2. `pip install -r requirements.txt` (or `-r requirements-dev.txt` for tests).
3. Create `.env`:

```
DISCORD_BOT_TOKEN=...
SUPABASE_DB_URL=postgresql://postgres.[ref]:[pw]@...pooler.supabase.com:5432/postgres
OANDA_API_KEY=...
OANDA_ACCOUNT_ID=...
OANDA_PRACTICE=false
OPENAI_API_KEY=...              # optional; AI fallback is off by default
BINANCE_USE_INTERNATIONAL=false
EXNESS_MT5_PATH=...             # path to the Exness terminal64.exe
EXNESS_MT5_LOGIN=...
EXNESS_MT5_PASSWORD=...
EXNESS_MT5_SERVER=...
LOG_LEVEL=INFO                    # DEBUG also enables discord.py diagnostics in console + bot.log
```

`LOG_LEVEL=DEBUG` is intentionally verbose and should be used temporarily while
diagnosing Gateway or REST/rate-limit behaviour. For every Discord 429 it records
the available limit, remaining, reset, reset-after, scope, retry-after, global, and
bucket values. Authentication headers and webhook/interaction tokens are never
logged. Console and file writes run outside the asyncio thread so a paused console
cannot freeze Discord heartbeats or commands. Restart the bot after changing it.

4. Fill in `config/channels.json` with the monitored / alert channel IDs and
   `config/settings.json` with admin IDs.
5. `python main.py`

---

## Testing

```
pytest tests/ -q      # pure-logic suite: parser, TP math, status machine, pip sizes
ruff check .          # lint (clean baseline enforced in CI)
```

---

## Shared database contract (TM bot ↔ execution bot)

The execution bot is a read-mostly consumer of this bot's database.

**Ownership** — TM bot owns and writes `signals`, `limits`, `status_changes`,
`live_prices`, `feed_health`, `bot_mode_status`, `config_history`, and the analytics
tables. The execution bot reads those and writes only `tp_outcomes` (append-only) and
its own `users` snapshot via a `SECURITY DEFINER` function; its database role has no
direct access to `licenses` or `users`.

**Vocabularies**

| Field | Values |
|---|---|
| `signals.status` | `active`, `hit`, `profit`, `breakeven`, `stop_loss`, `cancelled` |
| `signals.type` | `standard`, `scalp`, `swing`, `toll`, `pa`, `1-1`, `risky` |
| `signals.closed_reason` | `automatic`, `manual`, `expiry`, `near_miss`, `news:<CAT>`, `spread_hour`, `late_market`, `risky_window`, `real_sl` |
| `feed_health.status` | `idle`, `healthy`, `down` |
| `bot_mode_status.news_mode` / `vol_guard` | comma-separated tokens or `ALL`; `NULL` when inactive. `news_mode` uses currency/asset tokens (e.g. `EUR, GOLD`); `vol_guard` uses whole DB instruments (e.g. `EURUSD, NAS100USD, BTCUSDT`) plus `ALL` for gold |

**Invariants the execution bot depends on**

- `limits.id` is stable across signal edits — unchanged price levels keep their row.
- Cancel paths update `limits` to `cancelled` *before* the signal row transitions, so
  a pending-limit query never sees a half-cancelled signal.
- `live_prices.updated_at` advances at least every 30 s while a feed is ticking
  (heartbeat), and is allowed to age when a feed goes silent so staleness gates work.
- `feed_health.status` changes are written immediately; unchanged rows are refreshed
  at most every 10 minutes.
