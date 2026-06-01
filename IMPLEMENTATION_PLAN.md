# Pre-Production Hardening — Phased Implementation Plan

> **Status:** Phases 1 and 2 complete (2026-05-31). Phase 3 is next.
> **Authored:** 2026-05-31 (Opus 4.7, plan mode).
> **Executor:** Sonnet, one phase at a time.

---

## 1. Handoff context

### 1.1 The two systems
- **TM Bot** (`C:\Python Stuff\TM Bot`, branch `stage19_arch_restructure`) — Discord signal parser → Supabase. See `CLAUDE.md` for full architecture. Read that first.
- **MT5 Auto-Execution Bot** (`C:\Personal Projects\MT5-Auto-Execution-Bot`) — polls Supabase, places real MT5 orders, manages TP/SL. Has its own `CLAUDE.md`.

Shorthand used in this plan: **TM** = TM Bot, **EX** = Execution bot.

### 1.2 Why this work exists
User is about to forward-test both bots in production. Execution mechanics need to be tight enough that P&L isn't moved around by race conditions, missed events, or unsynchronized state between the two systems. **Strategy is out of scope** — only execution / plumbing / integration.

### 1.3 Integration recap (one-minute version)
- **Coupling:** Supabase tables `signals`, `limits`, `live_prices`, `bot_mode_status` (shared). EX uses `execution_bot_ro` role (read-only).
- **Discovery:** EX polls `signals WHERE status IN ('active','hit') AND limits.status='pending'` on 1s active / 30s idle cycle (`bot/core/sync_cycle.py:115`). Active mode is triggered when there are active orders/positions (`bot/core/engine.py:189-198`).
- **Local state:** EX owns `orders.db` SQLite — `order_mappings` is the source of truth for MT5 placement.
- **Invisible to EX:** approaching alerts, embed edits, cancel *reasons*, NM detection, `!tp` edits (TM-local JSON only), TM in-memory state.

### 1.4 Verifications from this session (do not re-investigate)
- **C3 polling cadence:** active mode is `tp_active_interval_seconds` (~1s) when orders/positions exist (`bot/core/engine.py:189-198`). The cancel-detection window is ~1s, not 30s — narrows the C3 race but does not eliminate it.
- **M6 spread hour:** EX already has its own spread-hour gate (`bot/core/sync_cycle.py:133` via `scheduler.should_cancel_pending()`). News mode is **not** gated on the EX side — only spread hour is. The M6 fix adds news_mode parity.
- **H1 fix shape:** add `ic_bid`/`ic_ask` columns to `live_prices`; TM writes ICMarkets mid alongside OANDA/Binance at the same `updated_at`; EX computes offset from two equal-time prices in one row. `WRITE_INTERVAL` stays at 5s — no extra API load.
- **L4 type inference:** verified that `core/parser/pattern_parsers.py:690-704` already handles "post in `daily-trades-setup` with body keyword `scalp` → type=scalp" via the body-keyword fallback after `CHANNEL_TYPE_MAP`. No change needed.

### 1.5 User preferences worth knowing
- Ruff: user runs it themselves at the end. Don't run after each change.
- Refactoring project is mid-stream (Areas 4-8 pending) — don't conflate this hardening work with that.
- Catalogue review convention: items the user didn't explicitly drop are confirmed and stay in the plan.

---

## 2. Items dropped (user-confirmed not-issues)

| ID | Reason |
|---|---|
| C4 | TP configs separate by design (per-side risk customization). |
| C7 | No account-level caps to be added. |
| H2 | No ICMarkets-only symbols; skip-on-missing-mapping is the right behavior. |
| H3 | SL spread adjustment is fine. |
| H4 | MT5 retcode is reliable. |
| H6 | `manually_set_signal_status` bypassing state machine is fine. |
| H10 | One symbol → one feed (no backup feed). If any backup-feed code exists, **remove it**. |
| M4 | Trailing not retreating is intended. |
| L1, L2, L3 | Cosmetic / non-issues. |
| L4 | Already implemented correctly. No change. |

---

## 3. Phased execution plan

Each phase: concrete scope, file targets, steps, acceptance criteria. **Sonnet executes one phase at a time and stops for review before moving to the next.**

Within a phase, items are mostly independent and can be tackled in any order unless noted. Phase 4.1 is internally ordered (schema → writer → reader).

---

### Phase 1 — Foundation (schema + low-risk config) ✅ COMPLETE

Independent groundwork. Safe to land first.

#### 1.1 H1 schema: add ICMarkets columns to `live_prices` ✅
- **Files:** `TM/database/schema.py`
- Added `ic_bid DOUBLE PRECISION` and `ic_ask DOUBLE PRECISION` (nullable) to the `live_prices` CREATE TABLE DDL and two `ALTER TABLE ADD COLUMN IF NOT EXISTS` migration blocks.

#### 1.2 M3: increase Supabase pool size for EX ✅
- **Files:** `EX/bot/db/supabase.py`
- Raised `max_size` from 3 → 10.

#### 1.3 M5 writer: feed-health → Supabase table ✅
- **Files:** `TM/database/schema.py`, `TM/price_feeds/feed_health_monitor.py`, `TM/core/bot.py`
- Added `feed_health (feed TEXT PRIMARY KEY, status TEXT, stale_seconds INTEGER, last_seen TIMESTAMPTZ, updated_at TIMESTAMPTZ)` table DDL in migrations.
- Added optional `db` parameter to `FeedHealthMonitor.__init__`; added `_write_feed_health()` upsert method; wired into each status branch of `_check_feed()` (idle / healthy / degraded / down).
- `bot.py` passes `db=db` to the constructor.
- **Reader side (EX) deferred to Phase 4.4.**

#### 1.4 L6 / L7 housekeeping ✅
- **L6 (`EX/bot/core/sync_cycle.py`):** added `rejection_reason: dict[int, str]` tracking in the approval loop (symbol not in terminal / live price stale / outside proximity); per-limit INFO log in the placement loop when skipped; proximity filter upgraded from DEBUG → INFO; added WARNING for the tick/info-None case that previously had no log.
- **L7 (`TM/database/signal_ops.py`):** changed `expire_old_signals` from one all-signals transaction to per-signal transactions — each signal's three updates are atomic, but a failure on one does not roll back already-committed signals. Order also fixed as part of C3 (see Phase 2).

---

### Phase 2 — Placement integrity (kill duplicate fills & cancel-races) ✅ COMPLETE

Core "don't take unintended trades" phase.

#### 2.1 C2: order-placement orphan window ✅
- **Files:** `EX/bot/trading/order_placer.py`, `EX/bot/db/sqlite.py`, `EX/bot/db/queries.py`, `EX/bot/core/reconciler.py`, `EX/bot/core/engine.py`
- MT5 comment changed from `s{signal_id}` to `s{signal_id}_l{limit_id}`[:32].
- Added `insert_claimed_order()` (pre-write with placeholder ticket `-limit_id`) / `promote_claimed_to_pending()` / `delete_claimed_order()` / `get_claimed_orders()` / `get_claimed_by_signal_limit()` to `sqlite.py` + `queries.py`.
- `place_order()` now: writes claim → pre-send check → `order_send()` → promote on success / delete on failure.
- `reconciler.py` refactored: extracted `reconcile_orphans()` which re-links claimed rows (via `_parse_comment()`), cleans stale claims, cancels truly untracked orders with magic-number filter. `reconcile()` calls `reconcile_orphans()` for the orphan phase.
- `engine.py` new `_reconcile_loop` task: orphan sweep every 60s (C2), full reconcile every 2h (M1).

#### 2.2 C3: cancel race window ✅
- **EX side** (`order_placer.py`, `supabase.py`, `queries.py`): `fetch_signal_status(signal_id)` added; pre-send check aborts placement (and deletes claim) if status not in `{active, hit}`. `supabase` parameter added to `place_order()` and threaded through from `sync_cycle.run()`.
- **TM side** (`signal_ops.py`): all cancel paths — `cancel_signal_by_message`, `manually_set_signal_status` (final-status branch), `expire_old_signals` — now update `limits.status='cancelled'` **before** `signals.status='cancelled'`. Invariant documented inline.

#### 2.3 M1 / M2: continuous reconciliation ✅
- **M1** (`engine.py`): full reconcile runs every 2h via `_reconcile_loop`.
- **M2** (`sync_cycle.py`): new `_check_external_closes()` method — for every SQLite `filled` row whose ticket is absent from `mt5_positions`, calls `mark_closed()` and logs at INFO. Runs every cycle outside the Supabase gate so it works even when Supabase is down.

#### 2.4 M14: idempotent `update_ticket` ✅
- **Files:** `EX/bot/db/queries.py`, `EX/bot/db/sqlite.py`
- Added `AND status='filled'` to UPDATE_TICKET WHERE clause (matches the call-site pattern: `mark_filled` always runs before `update_ticket`). `update_ticket()` now returns `bool` — `True` if a row was actually updated.

---

### Phase 3 — Reactivation and overlap discipline

#### 3.1 C1: overlapping limit-zone risk
- **Files:** `TM/price_feeds/streaming_monitor.py:284-330`, `EX/bot/core/sync_cycle.py:316-325`, `EX/bot/trading/lot_calculator.py`.
- **Steps:** pick one approach with the user before coding.
  - **Option A — EX-side merge:** before placement, group all pending limits per instrument across all active signals; if two limit prices are within a small tolerance (configurable, e.g. 5 pips forex / 0.5% crypto / $1 metals), place one combined order using the more conservative SL of the two and tag the SQLite row with both `signal_id`s. Lot sizing uses the wider of the two SL distances.
  - **Option B (Recommended) — Hard cap per instrument:** sum proposed lots per instrument; cap at `max_lot_per_instrument` from config; if cap exceeded, partially fill from the most-recent signal's limits. Simpler; matches user intent of bounding risk without entangling per-signal accounting.
- **Acceptance:** two overlapping signals on GBPUSD with hardcoded limits — total exposure does not exceed `max_lot_per_instrument`; EX log shows which limits were dropped or partially placed.

#### 3.2 C6: reactivation guard (TM) + placement contract (EX)
- **TM side — alert-bot guard (explicit user request):**
  - **Files:** `TM/database/signal_ops.py:343-409` (`reactivate_cancelled_signal`) and the reply-command callers in `TM/commands/signals/lifecycle.py` and `TM/discord_handlers/message_handler.py`.
  - **Steps:**
    1. Before reactivating, fetch the current price for the signal's instrument from `live_prices` (or the latest in-memory tick if running).
    2. For each currently-pending limit, check whether price has already moved past it (long: price > limit; short: price < limit).
    3. If **any** pending limit is past, block reactivation. Return a structured rejection that includes the offending limit indexes so the reply handler can post a clear embed-reply / DM to the requester.
    4. Allow override via `!setstatus active --force` (admin only).
  - **Acceptance:** signal with hit-1/2, cancelled, then price moved past limit 3 → reactivation reply is rejected with a readable message; `--force` succeeds.
- **EX side — placement contract (verify, no behavior change expected):**
  - **Files:** `EX/bot/core/sync_cycle.py`, `EX/bot/trading/order_placer.py`.
  - **Steps:** confirm the existing "new limit detected → place" path treats reactivated limits identically. On reactivation EX should attempt all pending limits; MT5 invalid-price retcodes are acceptable and should log `limit_id`.
  - **Acceptance:** reactivated signal with limits 3-5 pending (none past) → EX places 3-5 fresh; with one past, that one logs cleanly and the others go through.

---

### Phase 4 — Price / state synchronization between bots

#### 4.1 H1 writer + offset calc (internally ordered: writer first, EX reader second)
- **TM files:** `TM/price_feeds/live_price_writer.py:102-107` (buffer + flush).
- **EX files:** `EX/bot/db/supabase.py` (`fetch_live_prices` projection), `EX/bot/trading/offset_calculator.py:12-37`.
- **Steps:**
  1. **TM writer:** when flushing a per-symbol buffered tick, also read the latest ICMarkets last-tick for that symbol (the MT5 stream polls every 100ms, so it's in-memory) and write all four columns (`bid, ask, ic_bid, ic_ask`) plus `updated_at` in the same UPSERT.
  2. **EX `fetch_live_prices`:** include `ic_bid`, `ic_ask` in the SELECT.
  3. **EX `offset_calculator`:** replace the "fetch MT5 tick now" path with "use `ic_bid/ic_ask` from the same row as `bid/ask`". Offset = `mid(ic_bid, ic_ask) - mid(bid, ask)`. Both at the same `updated_at` — no drift.
  4. Keep a sanity check: if `ic_bid/ic_ask` are NULL (rolling-deploy gap), fall back to the old path and log once.
- **Acceptance:** offsets observed over a noisy market hour are near-constant (broker-delta only). The 5s drift component disappears from logs.

#### 4.2 M6: EX honors `news_mode`
- **Files:** `EX/bot/core/sync_cycle.py:133` (current spread-hour gate).
- **Steps:** read `bot_mode_status.news_mode` once per cycle. When `True`, behave as during spread hour: `placement_active = False` and force-cancel pending placements per the spread-hour code path. Log entries should distinguish `reason='news_mode'` vs `'spread_hour'`.
- **Acceptance:** `!news add` → EX log shows news block; pending placements cancelled; no new placements until news clears.

#### 4.3 C9: tick-staleness gate
- **Files:** `TM/price_feeds/streaming_monitor.py:332-373` (gate logic), `TM/price_feeds/price_stream_manager.py` (add `updated_at` to tick struct if missing).
- **Steps:**
  1. Ensure every tick carries `updated_at` (broker-stamped where possible, otherwise local `time.time()` at receipt).
  2. In the gate check, reject ticks where `(now - tick.updated_at) > N` seconds (N configurable; default 2s).
  3. Apply specifically around the spread-hour boundary so a stale rollover print can't slip past.
- **Acceptance:** synthetic stale tick at 18:00:05 carrying a 17:59:30 timestamp does not fire a hit alert.

#### 4.4 M5 reader: EX pauses placement when feed is stale
- **Files:** `EX/bot/db/supabase.py` (new `fetch_feed_health`), `EX/bot/core/sync_cycle.py`.
- **Steps:**
  1. EX reads `feed_health` rows each cycle.
  2. For symbols on a stale feed, EX skips placement and logs `reason='feed_stale'`.
- **Acceptance:** simulate OANDA stale → EX skips placement for OANDA-fed symbols; ICMarkets-fed symbols unaffected.

#### 4.5 M11: surface excluded-symbol rejections
- **Files:** `EX/bot/core/sync_cycle.py:121-123`.
- **Steps:** when a limit is dropped because its symbol is in `excluded_symbols`, log a single line with `signal_id`, `limit_id`, `symbol`. Optionally write to a Supabase log table so TM-side embed could surface it (deferred to a later phase if scope grows).
- **Acceptance:** every excluded-symbol drop is searchable in EX log.

#### 4.6 M12
Covered by 4.1.

---

### Phase 5 — SL & TP hygiene

#### 5.1 C5: stop trailing on `!breakeven` / `!profit`
- **Files:** `TM/database/signal_ops.py:411-530` (status setters), `EX/bot/db/queries.py:88-90` (`is_trailing`), `EX/bot/tp/trailing.py`, `EX/bot/core/sync_cycle.py:534-599`.
- **Steps:**
  1. EX treats Supabase signal status as authoritative each cycle: if status is `profit` or `breakeven`, immediately set `is_trailing=False` in SQLite for all related positions and run the force-exit path.
  2. Force-exit in the same cycle (no waiting for the next trailing tick).
- **Acceptance:** `!breakeven` while trailing — within one 1s cycle trailing stops and the position closes at the breakeven price (not at a deeper trailing SL).

#### 5.2 C8: SL change must persist; alert on repeated failures
- **Files:** `EX/bot/core/sync_cycle.py:455-533` (`_sync_filled_sls`).
- **Steps:**
  1. Per-ticket SL-failure counter `{ticket: {target_sl: count}}`. Reset on success or target change.
  2. On N consecutive failures (default N=5) → DM the admin (reuse the health-alert DM channel) with ticket, requested SL, last retcode.
  3. **Do not** recompute lot or risk. Operator's SL is the target. Effective risk change is acceptable.
- **Acceptance:** mocked persistent REQUOTE → DM fires at the Nth attempt; bot keeps trying on the next state change (target SL update); a single transient failure does not DM.

#### 5.3 H9: partial close rounds-up to broker step
- **Files:** `EX/bot/tp/default_strategy.py:121-126` (V5 fix region).
- **Steps:** if `raw < volume_step`, close `max(volume_step, volume_min)` instead of trailing the whole position. Optional config knob `partial_min_close_policy ∈ {step, min, skip}` (default: `step`).
- **Acceptance:** 0.13 lots, 50% partial, step 0.1 → closes 0.1 instead of trailing 0.13.

#### 5.4 M13: force-exit attempt counter + backoff
- **Files:** `EX/bot/core/sync_cycle.py:534-599`.
- **Steps:** per-ticket retry counter for force-exit; DM admin and stop retrying that ticket after N failures (same shape as 5.2). Resume on next state change.
- **Acceptance:** mocked persistent close failure → DM at the Nth attempt; no spammy retry loop.

#### 5.5 H7: atomic `mark_filled` + ticket update (hedging mode)
- **Background:** in hedging mode MT5 issues a separate `position_ticket` for the position formed when an order fills. SQLite is keyed on `mt5_ticket` (= the original order ticket). The current flow at `EX/bot/core/sync_cycle.py:409-414` runs `mark_filled` then `update_ticket` as two separate commits. If the second fails, the row has `status='filled'` but the wrong ticket — all downstream lookups (`pos_by_ticket`, trailing resume) miss the position and it drifts unmanaged.
- **Files:** `EX/bot/db/sqlite.py` (combine), `EX/bot/core/sync_cycle.py:409-414` (call site).
- **Steps:**
  1. New helper `mark_filled_and_set_position_ticket(order_ticket, position_ticket, filled_at)` does both updates in one transaction.
  2. Caller invokes the helper unconditionally — pass `position_ticket=order_ticket` when they match.
- **Acceptance:** killing aiosqlite mid-flight cannot leave the row in the half-updated state; the row is either fully updated or fully unchanged.

#### 5.6 H8: order-placement readback (easy add)
- **Files:** `EX/bot/trading/order_placer.py` (after successful `order_send`).
- **Steps:** call `orders_get(ticket=result.ticket)`; compare `sl` and `price_open` against requested values; on mismatch, log a warning and DM admin (one-shot). No retry, no halt.
- **Acceptance:** broker silent SL adjustment is detected and surfaced.

---

### Phase 6 — Lifecycle correctness

#### 6.1 H5: HIT signals roll over instead of cancel at expiry
- **Files:** `TM/database/signal_ops.py:740-800` (`expire_old_signals`), `TM/database/utils.py` (`calculate_expiry`).
- **Steps:**
  1. In `expire_old_signals`, skip signals with status `hit`. For each, compute the next expiry (e.g. `day_end` → next day's 4:45 PM EST) and `UPDATE signals SET expiry_time = ...`.
  2. Add an audit row in `status_changes` with `change_type='automatic'` and `reason='rollover'`.
- **Acceptance:** HIT signal at 4:46 PM EST is not cancelled; its `expiry_time` advances; `active` signals still cancel as before.

#### 6.2 M7: TM reloads hit limits on restart
- **Files:** `TM/price_feeds/streaming_monitor.py:195-235` (active-set loader).
- **Steps:** when loading active signals on startup, also fetch each signal's hit limits and populate the in-memory limit-state cache so embed reconstruction is correct.
- **Acceptance:** restart with a HIT signal in-flight → embed shows correct hit-limit highlighting and next-pending-limit logic.

#### 6.3 M8: `save_signal` TOCTOU race
- **Files:** `TM/database/signal_ops.py:30-88` (`save_signal`).
- **Steps:** rewrite the duplicate-check pattern as `INSERT ... ON CONFLICT (message_id) DO NOTHING RETURNING id`. If no row returned, run the cancelled-message reactivation check in the same flow.
- **Acceptance:** simulated duplicate parse → one row inserted; reactivation path still works.

#### 6.4 M9: atomic limit-hit update
- **Files:** `TM/database/manager.py` (`mark_limit_hit`).
- **Steps:** combine the two updates (limit row + signal counters) into a single transaction over the same connection (`BEGIN ... COMMIT`).
- **Acceptance:** under a forced disconnect mid-flight, neither the limit row nor `limits_hit` is partially updated.

#### 6.5 M10: EX sees `closed_reason`
- **Files:** `EX/bot/db/queries.py:3` and the SELECT projection wherever signals are fetched.
- **Steps:** add `closed_reason` to the projection; pass through to the force-exit handler so logs distinguish `manual`, `automatic`, `expiry`.
- **Acceptance:** EX log shows the cause when a signal closes externally.

---

### Phase 7 — License teardown + per-instrument risk %

#### 7.1 L5: full teardown on license expiry
- **Files:** `EX/bot/license/validator.py`, `EX/bot/core/engine.py:128-131`, `EX/bot/core/sync_cycle.py`, `EX/bot/trading/order_placer.py` (close helpers).
- **Steps:**
  1. New engine state `shutdown_reason ∈ {None, license_expired}`.
  2. When heartbeat flips `license_valid → False`:
     - Cancel **all pending limit orders** in MT5 (iterate SQLite `status='pending'`, call MT5 cancel, mark `cancelled` locally).
     - Market-close **all filled positions** in MT5 (iterate SQLite `status='filled'`, call MT5 close, mark `closed`).
     - Halt the sync polling loop (engine sets a flag the loop respects on its next iteration).
  3. Log at `error` level; DM admin once.
  4. **Re-activation requires a bot restart** (do not auto-resume).
- **Acceptance:** revoke license while two orders pending + two positions open → all four are removed from MT5 within one cycle; sync loop stops; DM received.

#### 7.2 Extra: per-instrument `risk_percent`
- **Files:** `EX/bot/config/settings.py:18-22` (`LotSizingConfig`), `EX/bot/trading/lot_calculator.py:47-73`.
- **Steps:**
  1. Change `risk_percent: float = 1.0` to `risk_percent: float | dict[str, float] = 1.0`. Mirror the existing `fixed_lot` shape exactly.
  2. In the calculator, resolve: `risk_percent[mt5_symbol]` → `risk_percent['default']` → `1.0`. Reuse any helper already used for `fixed_lot` resolution.
  3. Document the config shape in `EX/CLAUDE.md` (or its config docs section).
- **Acceptance:** `"risk_percent": {"XAUUSD": 0.3, "default": 1.0}` → XAUUSD uses 0.3%, EURUSD uses 1.0%. Plain `1.0` still works.

---

## 4. Cross-cutting verification checklist

The full hardening pass is complete when:

1. Two overlapping signals on the same instrument do not stack risk beyond cap. *(Phase 3.1)*
2. Killing EX mid-placement never produces a duplicate or orphan after restart or after the next sweep. *(Phase 2.1)*
3. `!cancel` issued within 1s of EX's next poll aborts placement before `order_send`. *(Phase 2.2)*
4. Reactivation is blocked when price has already moved past pending limits. *(Phase 3.2)*
5. Offset between `live_prices` and ICMarkets is broker-delta-only over 100 ticks. *(Phase 4.1)*
6. `!news add` blocks EX placements. *(Phase 4.2)*
7. Stale rollover ticks at the spread-hour boundary do not fire alerts. *(Phase 4.3)*
8. `!breakeven` / `!profit` halts trailing within one cycle. *(Phase 5.1)*
9. Repeated SL-modify failures DM admin once, not on every cycle. *(Phase 5.2)*
10. HIT signals at expiry roll over instead of cancelling. *(Phase 6.1)*
11. License expiry cancels pending, closes filled, and halts polling. *(Phase 7.1)*
12. Per-instrument `risk_percent` resolves correctly. *(Phase 7.2)*

---

## 5. Handoff notes for the executor

- **One phase at a time.** After each phase, stop and report acceptance results to the user before starting the next.
- Within a phase, items are mostly independent. Only **Phase 4.1** is internally ordered (schema → writer → reader).
- Do not run `ruff` after each change — user runs it at the end.
- Keep the in-flight refactor (`stage19_arch_restructure` branch, Areas 4-8 pending) mentally separate from this hardening work.
- Coding standards in `TM/CLAUDE.md` § Coding Standards apply: no enhanced/v2/fixed naming, no defensive `hasattr` guards on constructor-owned attrs, no narrative comments, log without emojis.
- For any scope decision not pinned down here (e.g. exact N for the failure counters in 5.2 / 5.4 / M13, Option A vs B for 3.1, retention shape for `feed_health` rows), surface to the user before implementing rather than guessing.

---

## 6. Out of scope

- Strategy logic.
- Discord-only UX issues with no DB / order effect.
- Ruff / formatting.
- Deep TP partial-close math beyond H9.
- TM parser correctness — separate audit if wanted.
