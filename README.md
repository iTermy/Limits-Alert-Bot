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
  event and refreshed with live prices every 30 s. Refreshes are paced against a
  per-channel write budget the bot learns from Discord's own rate-limit headers,
  so it never generates more edits than a channel can drain — every embed still
  refreshes, the interval just stretches as embeds accumulate. Event alerts
  (hit / stop-loss / TP) hold reserved slots and never queue behind a price
  snapshot. Embed references are stored in the database so restarts re-attach to
  existing embeds instead of orphaning them. `!health` reports the live
  allowance and sweep interval per channel.
- **Auto take-profit** — once a signal is hit, P&L is tracked on every tick and the
  signal closes automatically when the configured TP threshold is reached.
- **News and spread-hour handling** — scheduled news windows and the daily
  spread-widening window suppress alerts and auto-cancel affected signals with a
  notification. Add `dryrun` to a news command to pause clients while the alert
  bot continues normally.
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

## Monitoring (Windows)

Install updated `requirements.txt`, restart the bot with `run.bat`, then run
`run_monitoring.bat`. The launcher installs Docker Desktop via winget if missing,
starts its Linux engine, and launches Prometheus and Grafana. Complete any Docker
administrator/WSL 2 prompts and reboot if requested, then rerun. Windows Server is
not supported by Docker Desktop. See [Docker prerequisites](https://docs.docker.com/desktop/setup/install/windows-install/).
The launcher does not start another bot or change its Python environment.

- Grafana: http://localhost:3003 (user `admin`, generated password in `.monitoring.env`).
  Open Dashboards / Limits Alert Bot. Password initialization only applies on first
  start; editing the file does not reset an existing Grafana account.
- Prometheus: http://localhost:9091 (Targets and Alerts pages).
- Stop: `run_monitoring.bat -Stop`. Named volumes preserve data; retention is 30 days.

The native bot exposes `/metrics` on TCP 9108 via `host.docker.internal`.
`METRICS_ENABLED=false` disables it. `METRICS_HOST` defaults to `0.0.0.0` so Docker
can reach it; restrict inbound TCP 9108 to Docker's network using Windows Firewall.
Do not publish it through a public tunnel. `METRICS_PORT` defaults to 9108; update
`ops/prometheus/prometheus.yml` too if changing it. Grafana/Prometheus bind only to
localhost, using ports distinct from the neighboring discord-bot stack.

Metrics cover Discord readiness/latency, event loop lag, database probes, tracked
signals, price monitor health, market-aware feed failures, alert queue depth,
important delivery retries, uptime, and supervisor restarts. Database probes use
SELECT 1 on the existing pool with a 3-second timeout. No credentials, signal IDs,
or prices are exported. Feed status reuses existing market-hours checks; feeds not
yet initialized are absent. Restart counts cover in-process supervisor restarts,
not watchdog process replacements. A frozen event loop causes scrapes to fail.

Rules detect bot/database outages, disconnected Discord, stopped monitoring,
feed failures, prolonged retries, and loop delays. Rules are visible in Prometheus
and Grafana; external notification delivery needs an Alertmanager destination
(not configured). Existing Discord health notifications continue independently.

Verify: confirm the bot target is UP and dashboard values populate. Stop the bot;
BotUnavailable should fire after one minute. Restart and verify recovery.

### Latency panels

Dashboard rows separate overview, MT5 queries, TP/BE reactions, Discord delivery,
price feeds, and runtime/database health. Latency panels show p50/p95/p99.
ICMarkets query latency measures each completed `symbol_info_tick` call; executor
latency includes scheduling wait and full polling sweeps, including cancellation.
Exness query latency is sampled on changed-price ticks sent by its worker.
These are local terminal API times, not a broker network ping.

Reaction timing starts when the stream manager receives a tick for dispatch.
TP/BE state timing ends after the database update is confirmed; Discord timing ends
only when critical delivery returns success, including background retry delays.
This excludes time before dispatch (market movement, feed transport, polling interval,
and Exness IPC queue). It is not order execution or fill latency. BE here means the
breakeven stop firing, not a broker stop modification. Failed/untriggered events do
not produce successful-reaction observations. Empty panels mean no qualifying events
in the selected rate window; restart the updated bot to begin collecting metrics.
