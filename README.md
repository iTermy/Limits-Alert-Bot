# TM Bot — Trading Signal Monitor

A Discord bot that turns free-form trading-signal messages into tracked, alerted, and
audited positions. It parses signals posted in designated channels, stores them in
PostgreSQL, streams real-time prices from four independent feeds, and drives each
signal through its full lifecycle — approaching, entry hit, take-profit, stop-loss,
near-miss cancellation — by live-editing a single persistent Discord embed per signal.

A companion per-user execution bot (separate repository) reads the same database and
mirrors these signals onto MetaTrader 5 broker accounts.

## Highlights

- **Four-feed price streaming** — ICMarkets (MT5 polling), OANDA (REST streaming),
  Binance (WebSocket), and Exness (MT5 in an isolated child process, since the
  MetaTrader5 package is process-global). A symbol mapper routes each instrument to
  its best feed and translates symbol formats in both directions.
- **Channel-aware signal parsing** — pattern parsers per channel family (core forex /
  metals / indices, stocks via MT5 symbol lookup, crypto with auto-`USDT` tickers),
  per-channel defaults, typo detection via limit-ordering validation, and an optional
  AI fallback parser.
- **Persistent-embed alerting** — one embed per signal, edited in place for every
  lifecycle event and refreshed with live prices every 15 s. Embed references are
  persisted to the database so restarts re-attach to existing embeds instead of
  orphaning them.
- **Self-healing** — per-feed health monitoring with targeted reconnects, a
  price-flow watchdog that force-restarts the bot when open markets go silent, and an
  out-of-loop freeze watchdog that survives a wedged event loop.
- **Egress-conscious persistence** — TOCTOU-safe signal saves, dirty-checked live
  price writes with a 30 s heartbeat contract for the execution bot, and
  transition-only feed-health writes.
- **Runtime configuration** — take-profit thresholds per signal type, alert
  distances, and near-miss rules are all adjustable through commands without a
  restart, with every change appended to a `config_history` audit table.

## Tech stack

| Layer | Choice |
|---|---|
| Language | Python 3.9+ (Windows required for the MetaTrader5 feeds) |
| Discord | discord.py 2.3+, `commands.Bot` with cog extensions |
| Database | PostgreSQL (Supabase) via asyncpg |
| Models | Pydantic v2 (`SignalData`, `LimitData`, `BotSettings`) |
| Lint / tests | ruff, pytest (CI via GitHub Actions) |

## Project layout

```
main.py                  Entry point + in-process restart supervisor
core/                    Bot wiring, signal parser, news manager, expiry manager
  parser/                Channel-aware pattern parsers + validators + AI fallback
database/                asyncpg pool, schema/migrations, signal CRUD + lifecycle,
                         reporting queries, audit ops
price_feeds/             Feed clients, streaming monitor (per-tick evaluation),
                         alert system, TP/near-miss/excursion/trailing monitors,
                         threshold configs, symbol mapper, health monitor
discord_handlers/        Message intake: parsing, edits/deletes, reply commands
commands/                Cogs: signal lifecycle, reports, news, config, admin
models/                  Pydantic domain models + status enums
config/                  Runtime JSON configuration
tests/                   Pure-logic test suite (parser, TP math, state machine, …)
```

## How a signal flows

```
message → parse_signal() → save (atomic, TOCTOU-safe) → ✅ reaction
   → streaming monitor subscribes the symbol
   → per tick: staleness gate → spread-hour/news/risky gates → approaching
     → hit → stop-loss → near-miss → auto-TP
   → every event edits the signal's persistent embed + pings subscribers
   → terminal states archive the embed and snapshot close prices
```

The full check order, state machine, and recovery semantics are documented in
[CLAUDE.md](CLAUDE.md), which serves as the maintainer reference.

## Setup

1. Python 3.9+ on Windows (both MT5 terminals must be installed for the ICMarkets
   and Exness feeds; the bot degrades gracefully without them).
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
LOG_LEVEL=INFO
```

4. Fill in `config/channels.json` with the monitored / alert channel IDs and
   `config/settings.json` with admin IDs.
5. `python main.py`

## Testing

```
pytest tests/ -q      # pure-logic suite: parser, TP math, status machine, pip sizes
ruff check .          # lint (clean baseline enforced in CI)
```

## Shared database contract (TM bot ↔ execution bot)

The execution bot is a read-mostly consumer of this bot's database. The contract:

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
| `bot_mode_status.news_mode` / `vol_guard` | comma-separated tokens or `ALL`; `NULL` when inactive. `news_mode` uses currency/asset tokens (e.g. `EUR, GOLD`); `vol_guard` uses full pairs (e.g. `EURUSD`) plus `ALL` for gold |

**Invariants the execution bot depends on**

- `limits.id` is stable across signal edits — unchanged price levels keep their row.
- Cancel paths update `limits` to `cancelled` *before* the signal row transitions, so
  a pending-limit query never sees a half-cancelled signal.
- `live_prices.updated_at` advances at least every 30 s while a feed is ticking
  (heartbeat), and is allowed to age when a feed goes silent so staleness gates work.
- `feed_health.status` changes are written immediately; unchanged rows are refreshed
  at most every 10 minutes.
