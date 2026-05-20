# TM Bot — Refactoring Plan

## Executive Summary

The biggest wins are concentrated in **two places**: the message-arrival channel-gate (`bot.on_message` + `MessageHandler._get_allowed_channels`) which rebuilds the same set twice per message, and the **reply-command handler** in `message_handler.py` (~560 lines, two near-duplicate flows for "reply to alert" vs "reply to signal"). Both can be cut substantially without behavior change.

Other high-value, low-risk wins: drop dead code in `core/parser/__init__.py` (`initialize_parser`, `cleanup_parser`, `_separate_limits_and_stop`, `_extract_direction_quick`, `is_stock_channel`, `is_crypto_channel`), drop the unused `EmbedFactory` and its two dead imports, collapse the three identical `_parse_dt` / `_to_dt` helpers into one shared util, and remove the unused `check_stop_loss_hit` / `mark_hit_alert_sent` / `mark_approaching_alert_sent` DB methods (the streaming monitor has its own inline path).

The `signal['id']` vs `signal['signal_id']` key mismatch is currently papered over with `dict(sig); sig['signal_id'] = sig.get('id', signal_id)` patterns scattered across 4+ call sites — one normalisation point (in `get_signal_with_limits`) removes all of them.

Total scope is meaningful but bounded — most proposals are local cleanups, not architectural changes. I count roughly **1500–2000 lines that can be removed or consolidated** without changing behavior.

After completing an area, mark the section as finished and briefly state what you did in 3 sections: Location, Done, Adjacent fixes (optional), Notes (optional) 

---

## Area 1: Hot path — `on_message` → channel gate

### ✅ 1.1 Consolidate allowed-channels into a single bot attribute
- **Location**: `core/bot.py` (`on_message`, `load_config`, `__init__`), `discord_handlers/message_handler.py` (`_get_allowed_channels`, `is_allowed_channel`)
- **Done**: Added `bot.allowed_channel_ids: set[int]` built once at the end of `load_config()`. `on_message` now does a single set-membership check. `MessageHandler._get_allowed_channels` deleted; `is_allowed_channel` reads `bot.allowed_channel_ids` directly.
- **Adjacent fix applied**: Removed the redundant `is_allowed_channel` guard inside `handle_new_message` (caller already gates).

### ✅ 1.2 Remove the early-return defensive duplication in `on_message`
- **Location**: `core/bot.py` (`on_message`)
- **Done**: Removed outer `try/except` catch-all. Body dedented one level. Two inner try/except blocks kept unchanged.
- **Adjacent fix applied**: Removed the `if self.message_handler:` guard (always set after `setup_hook`).

---

## Area 2: Parser package (`core/parser/`)

### ✅ 2.1 Drop dead exports / dead helpers
- **Location**: `core/parser/__init__.py` and `core/parser/validators.py`
- **Done**: Deleted `initialize_parser()`, `cleanup_parser()`, `validators._separate_limits_and_stop`, `validators._extract_direction_quick`, `validators.is_stock_channel`, `validators.is_crypto_channel`. Trimmed `__all__` accordingly. Removed `Tuple` from typing imports in validators.py.

### ✅ 2.2 Collapse `_load_channel_config` redundancy
- **Location**: `core/parser/__init__.py`
- **Done**: Replaced 2-branch if/else with single flow — acquire module-level config singleton when no `config_loader` passed, then one shared try/except for the load call.

### ✅ 2.3 De-duplicate the "is general-tolls auto-SL" detection
- **Location**: `core/parser/pattern_parsers.py`
- **Done**: Added module-level `_SL_KEYWORD_RE` and `_general_tolls_auto_sl(channel_name, text) -> bool` helper. All three call sites (CorePatternParser.parse, StockPatternParser.parse, determine_limits_and_stop) now use the helper instead of local compiled regex copies.

### ✅ 2.4 Delete `CryptoPatternParser`
- **Location**: `core/parser/pattern_parsers.py`
- **Done**: Deleted `CryptoPatternParser` (34 lines). Removed `_internal_call` parameter and all 6 guards from `CorePatternParser.parse`. `SignalParser._parse_with_crypto_parser` now uses `self._core_parser` directly and sets `result.parse_method = "crypto"` after a successful parse.

### ✅ 2.5 Remove the duplicate `_remove_index_symbols` blacklist
- **Location**: `core/parser/validators.py`, `core/parser/pattern_parsers.py`
- **Done**: `INDEX_SYMBOL_BLACKLIST` defined once in `validators.py` as a module-level constant. `pattern_parsers.py` imports and uses it; inline duplicate removed.

---

## Area 3: DB layer (`database/`, `database/signal_operations/`)

### ✅ 3.1 Single shared `_parse_dt` helper
- **Location**: `database/base_operations.py`, `database/database_manager.py`, `database/signal_operations/crud.py` (was `_to_dt`), `database/signal_operations/lifecycle.py`
- **Done**: Canonical `_parse_dt` added to `database/signal_operations/utils.py`. All four local copies removed and replaced with an import from there. `database_manager.py`'s `__import__("pytz")` smell is gone. `crud.py`'s inner `_parse_dt_local` function inside `save_signal` also removed; the inline `from datetime import datetime` / `import pytz` / `from database.models import LimitStatus, SignalStatus` duplicate imports inside `save_signal` removed. `calculate_expiry` import in `crud.py` moved to module level.

### ✅ 3.2 Drop unused DB methods
- **Location**: `database/base_operations.py`, `database/database_manager.py`, `database/signal_operations/lifecycle.py`
- **Done**: Deleted `check_stop_loss_hit`, `mark_approaching_alert_sent`, `mark_hit_alert_sent` from `base_operations.py` and their delegate wrappers from `database_manager.py`. Deleted `check_and_update_stop_loss` from `lifecycle.py`. `_mark_approaching_sent` stays inline in `streaming_monitor` (single 1-line UPDATE, correct path).

### ✅ 3.3 Drop the unused `insert_signal` / `insert_limits` two-step path
- **Location**: `database/base_operations.py`, `database/database_manager.py`
- **Done**: Deleted both methods and their delegate wrappers. Active path remains `crud.save_signal` (atomic transaction).

### ✅ 3.4 Collapse `get_active_signals_detailed` and `get_active_signals_detailed_sorted`
- **Location**: `database/signal_operations/crud.py`, `database/signal_operations/__init__.py`, `commands/trading_commands.py`
- **Done**: Deleted `get_active_signals_detailed` (~80 lines) and its `SignalDatabase` delegate. Removed `first_pending_limit` from the SELECT in `get_active_signals_detailed_sorted`. Updated the one external caller in `trading_commands.py` to use `get_active_signals_detailed_sorted`. Inline `from .utils import get_status_emoji` imports inside the loop moved to module-level import.

### ✅ 3.5 Normalise `signal['id']` → `signal['signal_id']` once at the DB layer
- **Location**: `database/signal_operations/crud.py`, call sites in `discord_handlers/message_handler.py`, `core/expiry_manager.py`, `price_feeds/alert_system.py`
- **Done**: `get_signal_with_limits` and `get_signal_by_message_id` now both set `signal["signal_id"] = signal["id"]` before returning. Removed all 8 call-site normalization patches (dict copies + conditional key assignment). CLAUDE.md "gotcha" section removed.

### 3.6 Inline or rename `SignalDatabase` wrappers
- **Location**: `database/signal_operations/__init__.py` (the entire `SignalDatabase` class, ~270 lines)
- **Current state**: `SignalDatabase` is a coordinator that holds three sub-managers (`_crud`, `_lifecycle`, `_analytics`) and exposes ~25 methods, **every one** of which is a one-line `return await self._x.method(...)` delegate. The sub-managers also each take `db_manager` separately, and `lifecycle` methods often take `signal_db` and `db_manager` as separate arguments to work around the circular split.
- **Proposed change**: Either (a) flatten everything onto `SignalDatabase` directly (delete the three sub-files, keep the code organised by `# region` comments) or (b) keep the sub-modules but stop the delegate layer — callers can do `signal_db._crud.save_signal(...)`. (a) is cleaner if the file size is manageable (~600 LOC combined for crud + lifecycle).
- **Rationale**: Goal 2/3 (abstraction wrapping one caller). The delegate-only `SignalDatabase` class is pure plumbing.
- **Risk**: Medium — many call sites in commands, message_handler, expiry_manager. Touches no SQL or transactional logic, but it's a wide-blast-radius rename. Recommend doing it after the easier wins.
- **Behavior-preserving?**: Yes.

### 3.7 Lifecycle methods that take both `db_manager` and `signal_db` as args
- **Location**: `database/signal_operations/lifecycle.py:44, 110, 175, 285, 398, 427, 475`
- **Current state**: `LifecycleManager` already has `self.db = db_manager` from `__init__`, yet most methods take a second `db_manager` parameter and a `signal_db` parameter from the caller. This is the residue of an earlier split that's no longer needed — the parameters are always the same object as `self.db` / the matching coordinator.
- **Proposed change**: Drop the extra params. Use `self.db` and look up the CRUD operations via a back-reference set in `__init__`.
- **Rationale**: Goal 2 (parameters never passed differently) + Goal 4 (residue of stale refactor).
- **Risk**: Medium — same blast radius as 3.6. Best done together.
- **Behavior-preserving?**: Yes.

### ✅ 3.8 Drop unused `format_time_remaining` / `calculate_pip_difference` / `get_status_emoji` duplicates (partial)
- **Location**: `database/signal_operations/utils.py`
- **Done**: Deleted `format_time_remaining` (~44 lines) and `calculate_pip_difference` (~20 lines).
- **Skipped**: `get_status_emoji` consolidation — the two copies (`database/signal_operations/utils.py` and `utils/formatting.py`) have different emoji values for `profit` ("✅" vs "💰") and the `utils/formatting.py` version checks for `"stoploss"` which doesn't match the DB value `"stop_loss"`. Consolidating requires a deliberate choice about which emoji set to use; skipped to avoid a silent visual regression.

---

## Area 4: Streaming monitor (price-tick hot path)

### 4.1 Cache the spread-hour check on the tick path
- **Location**: `price_feeds/streaming_monitor.py:235-256` (`_is_spread_hour`), and three call sites: `396, 580, 736`
- **Current state**: `_is_spread_hour` builds `pytz.timezone('America/New_York')`, calls `datetime.now(est)`, and runs comparisons **on every price tick** (and is called up to 3 times per tick — once for transition tracking, once per limit check, once for SL check).
- **Proposed change**: Compute once at the top of `_on_price_update` (already done at line 396 for the transition check) and pass the boolean down to `_check_signal` / `_check_limit` / `_check_stop_loss`. Or cache it per tick on `self`.
- **Rationale**: Goal 1 (hot path efficiency — this is called on every tick for every symbol).
- **Risk**: Medium — touches the tick path. The change is a pure mechanical refactor (pass-down vs recompute) but still warrants careful review.
- **Behavior-preserving?**: Yes (assuming a single tick's spread-hour state is consistent).

### 4.2 Drop `test_signal_monitoring`
- **Location**: `price_feeds/streaming_monitor.py:916-934`
- **Current state**: Never called from anywhere.
- **Proposed change**: Delete.
- **Rationale**: Goal 2.
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 4.3 Inline `_reload_spread_buffer_setting` + the cache machinery
- **Location**: `price_feeds/streaming_monitor.py:207-233`
- **Current state**: `_reload_spread_buffer_setting` plus `_is_spread_buffer_enabled` plus `_last_settings_load` plus `_settings_cache_duration` (~30 lines) all to gate a single boolean from `settings.json` with a 30s cache.
- **Proposed change**: Replace with a single property that reads `load_settings()` directly (which is cheap — a 1KB JSON read), or push the cache into `load_settings()` itself in `utils/config_loader.py` so every consumer benefits. The `get_gold_tolls_sl_offset()` 30s cache is the same pattern duplicated separately.
- **Rationale**: Goal 1 + Goal 3.
- **Risk**: Medium — touches the tick path indirectly.
- **Behavior-preserving?**: Yes.

### 4.4 Cancel-on-spread-hour and cancel-on-news share a guard scaffold
- **Location**: `price_feeds/streaming_monitor.py:549-597` (news + spread-hour guards inside `_check_limit`)
- **Current state**: Two nearly identical 25-line blocks: pre-evict from `active_signals`, log, send cancel alert, react to original, call `_process_X_cancel`. The only differences are the cancel function called and the log/alert text.
- **Proposed change**: Extract a helper `_cancel_signal_during_guard(signal, current_price, reason: Literal['news','spread_hour'], extra=None)` that does the eviction + alert + react + DB update. Call it from both places.
- **Rationale**: Goal 1 (hot-path clarity) + Goal 3.
- **Risk**: High — touches concurrency-sensitive code where the pre-evict is the critical ordering. Worth doing but should be reviewed in isolation.
- **Behavior-preserving?**: Yes if the eviction-then-await ordering is preserved exactly.

### 4.5 The `signal['guild_id'] = self.bot.guilds[0].id` on every tick
- **Location**: `price_feeds/streaming_monitor.py:442-444`
- **Current state**: On every signal check on every tick, `_check_signal` looks up `bot.guilds[0].id` and patches the signal dict. Same value, every tick, ~hundreds of times per second across active signals.
- **Proposed change**: Set `signal['guild_id']` once when the signal is added to `active_signals` in `_load_and_subscribe_signals` / `_periodic_signal_refresh`. Drop the patch from `_check_signal`.
- **Rationale**: Goal 1 (tiny, but it's literal busywork on the hot path).
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

---

## Area 5: Alert system

### 5.1 Remove the dead `EmbedFactory` import + the entire `utils/embed_factory.py`
- **Location**: `utils/embed_factory.py` (371 lines); imports in `discord_handlers/message_handler.py:7` and `price_feeds/alert_system.py:14`.
- **Current state**: `EmbedFactory` is imported in both files but never referenced anywhere outside its own definition. The current embed builder is `alert_system._build_signal_embed` (a module-level function).
- **Proposed change**: Delete `utils/embed_factory.py` and the two imports.
- **Rationale**: Goal 2 — 371 lines of dead module.
- **Risk**: Low — confirmed via grep no caller references it.
- **Behavior-preserving?**: Yes.

### 5.2 Consolidate the toll-channel/PA-channel/legends-channel JSON re-reads
- **Location**: `price_feeds/alert_system.py:579-617` (`_get_finished_channel`, `_get_profit_channel_sync`), `835-887` (`_load_pa_channels`, `_load_toll_channels`), `1789-1805` (`_get_profit_channel` async variant).
- **Current state**: At least 5 different functions re-read `channels.json` from disk on every call (`_get_finished_channel`, `_get_profit_channel_sync`, the async `_get_profit_channel`, plus the PA/toll loaders during `__init__`). The two profit-channel functions return the same value via different paths.
- **Proposed change**: Read `channels.json` once at `AlertSystem.__init__` (or accept the parsed dict as a constructor param from `streaming_monitor.initialize`), keep the IDs on `self`. Delete `_get_profit_channel` (async) — only `_get_profit_channel_sync` is called. The "every call re-reads" pattern was likely added for hot-reloads but those happen via `!reload` which can call a new `reload_channels()` method instead.
- **Rationale**: Goal 1 (filesystem I/O on every embed move) + Goal 2.
- **Risk**: Low/Medium — needs a `reload_channels()` path if `!reload` is meant to refresh these.
- **Behavior-preserving?**: Yes if `!reload` is updated.

### 5.3 Hardcoded role-mention constant `<@&1334203997107650662>`
- **Location**: `price_feeds/alert_system.py:748, 878, 1064, 1086, 1589` and `discord_handlers/message_handler.py:878`
- **Current state**: Same Discord role ID hardcoded in 6+ places.
- **Proposed change**: Read from `channels.json` once into `AlertSystem.role_mention_id`. Construct the mention string in one place.
- **Rationale**: Goal 3 (magic number).
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 5.4 Two near-identical "rebuild archived embed with archive footer" blocks
- **Location**: `price_feeds/alert_system.py:692-742` (`_move_after_delay` finished-channel branch), `1607-1625` (the analogous block inside `send_news_cancel_alert._move_standalone_after_delay`).
- **Current state**: Two parallel after-delay tasks that share the same "rebuild embed, set archived footer, post to finished, delete original" recipe. The footer-cleanup pattern (`old_footer.split(" • ⏳")[0].split(" • 🗑️")[0]`) appears in three places.
- **Proposed change**: Extract `_archive_footer(embed, label="📁 Archived")` and a single `_move_to_archive_channel(signal_id, source_msg, build_archive_embed)` coroutine. Reuse from both call sites.
- **Rationale**: Goal 1/3.
- **Risk**: Medium — touches the deletion-task lifecycle. Tests with reactivation-while-archive-pending are the key edge cases.
- **Behavior-preserving?**: Yes.

### 5.5 Embed builder `_build_signal_embed` lazy-imports `TPConfig` on every call
- **Location**: `price_feeds/alert_system.py:116-121`
- **Current state**:
  ```python
  _tp_config = None
  try:
      from price_feeds.tp_config import TPConfig
      _tp_config = TPConfig()
  except Exception:
      pass
  ```
  This runs once per embed build. `TPConfig()` reads `tp_configuration.json` from disk in its constructor (verify, but likely).
- **Proposed change**: Pass `tp_config` in as an argument to `_build_signal_embed` (callers in `AlertSystem` already have one available on `self.bot.monitor.tp_config`), or import once at module level.
- **Rationale**: Goal 1 — JSON read per embed render is the kind of thing that scales badly with active signals.
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 5.6 The status_map → cancel_type → reason_text branching
- **Location**: `price_feeds/alert_system.py:189-238`
- **Current state**: A 50-line block of `if/elif` mapping `event` strings + `cancel_type` strings to display text and footer suffix. Has at least 4 different conditional branches that compute "reason_text" and "footer".
- **Proposed change**: Build a single `EVENT_DESCRIPTORS` dict mapping `(event, cancel_type_prefix)` → `(color, status_label, reason_text, footer_suffix)`. Look up once.
- **Rationale**: Goal 3 (dispatch table beats `if/elif` chain).
- **Risk**: Medium — it's the embed that members see. Behavior-preserving requires careful matching of edge cases.
- **Behavior-preserving?**: Yes if done carefully.

### 5.7 Drop the bounded-`alert_messages` cleanup as a separate concern
- **Location**: `price_feeds/alert_system.py:963-968`
- **Current state**: After every track, if `len > 1000`, evict the first N. This is fine, but it's a 4-line in-method loop where a `collections.OrderedDict.popitem(last=False)` or a `lru_cache`-style pattern would be 1 line.
- **Proposed change**: Use `collections.OrderedDict` or just leave it — it's harmless but not pretty.
- **Rationale**: Goal 3.
- **Risk**: Low.
- **Behavior-preserving?**: Yes. (Marginal value — could be skipped.)

---

## Area 6: Reply-command handler

### 6.1 Collapse `check_alert_management_reply` and `check_signal_management_reply`
- **Location**: `discord_handlers/message_handler.py:162-512` (alert reply) and `651-966` (signal reply)
- **Current state**: ~650 lines across two methods that handle the exact same set of commands (`cancel`, `profit`, `tp`, `breakeven`, `sl`, `hit`, `reactivate`). The two paths differ in: (a) authorization (alert: any user; signal: author or admin); (b) which message is the "referenced" one; (c) one extra branch for cancel-without-embed in the signal path. Everything else — the command parsing, the SL P&L calculation, the reaction updates, the embed update, the delete-user-reply — is duplicated almost verbatim.
- **Proposed change**: Build one `_handle_reply_command(message, referenced, signal, auth_check: callable)` that takes the common signal dict + an authorization predicate. The cancel-without-embed branch (lines 821-894) can be conditional inside the unified handler when called from the signal path.
- **Rationale**: Goal 1 (one path is faster to read and reason about) + Goal 2 (~250-300 lines of duplicate code) + Goal 3.
- **Risk**: High — these methods are the manual-override entry points for live signals. Both paths have intricate edge cases (re-add-to-monitor on hit when previously cancelled at lines 296-322 and 758-782, both nearly identical). Bug here cancels real money.
- **Behavior-preserving?**: Yes if done carefully. Recommend doing this as the final cleanup, after the easier wins land.

### 6.2 Extract the "compute SL result_pips from hit limits" helper
- **Location**: `discord_handlers/message_handler.py:340-358, 728-743`, `price_feeds/streaming_monitor.py:796-811`
- **Current state**: The same loop (sum P&L of all hit limits at the stop-loss price) appears three times. Each iteration uses `tp_config.calculate_pnl`, fetches `hit_limits` via `signal_db.get_hit_limits_for_signal`, and folds.
- **Proposed change**: Add `async def calculate_sl_pnl(signal, signal_db, tp_config) -> Optional[float]` in a shared util (e.g. `database/signal_operations/utils.py` or a new `price_feeds/pnl.py`). Use from all three sites.
- **Rationale**: Goal 1/3.
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 6.3 Local `import asyncio` inside the loop
- **Location**: `discord_handlers/message_handler.py:249, 694` and elsewhere
- **Current state**: `import asyncio` is done inside the method body twice (with comments "Import asyncio for timeouts"). `asyncio` is already imported at module level in the rest of the codebase.
- **Proposed change**: Move to top of file.
- **Rationale**: Goal 4.
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 6.4 The `send_profit_alert` method and `get_pip_unit_name`
- **Location**: `discord_handlers/message_handler.py:514-649`
- **Current state**: `send_profit_alert` (~85 lines) constructs an embed and sends it. Grep shows it's never called. `get_pip_unit_name` (~50 lines, with its own JSON re-read) is also never called.
- **Proposed change**: Delete both.
- **Rationale**: Goal 2.
- **Risk**: Low — confirmed unused.
- **Behavior-preserving?**: Yes.

### 6.5 The bare `except: pass` for `remove_reaction`
- **Location**: `discord_handlers/message_handler.py:151-153, 430-432, 443-446, 815-818, 904-907`
- **Current state**: Five copies of `try: await ref.remove_reaction(...); except: pass` to handle "reaction may not exist."
- **Proposed change**: Extract `_safe_remove_reaction(message, emoji)` helper.
- **Rationale**: Goal 3/4 (defensive try/except with bare `except`).
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

---

## Area 7: Commands (broad cleanup)

### 7.1 Fix the broken `load_channels_config` import
- **Location**: `commands/bot_commands.py:617-622`
- **Current state**: `from utils.config_loader import load_channels_config` — but `load_channels_config` is **not defined** in `config_loader.py`. This would crash at runtime if the `if not toll_channel_ids:` fallback branch is ever taken. Currently masked because the primary branch (using `alert_system.toll_channel_ids`) always succeeds.
- **Proposed change**: Either define `load_channels_config()` in `config_loader.py` (one line: `return config.load("channels.json")`) **or** delete the fallback branch and require `alert_system` to be initialized.
- **Rationale**: Goal 4 — dead-branch import would crash if reached.
- **Risk**: Low — fixing a latent bug.
- **Behavior-preserving?**: No (this fixes a bug — the fallback path would currently raise).

### 7.2 `commands/trading_commands.py` is 2367 lines
- **Location**: Whole file
- **Current state**: One mega-cog with command handlers, the `ActiveSignalsView` pagination class, and inline implementations for `!signal`, `!cancel`, `!setstatus`, `!tp set/show/remove`, `!alertdist set/show`, `!nmconfig`, `!news`, `!setexpiry`, `!report`, etc. This is the single largest file in the codebase.
- **Proposed change**: I am **not** recommending a full split here — that's the kind of "refactor for refactor's sake" that this task explicitly excludes. But two specific extractions are worth it:
  - Pull `ActiveSignalsView` out into `commands/views.py` (it's a self-contained pagination component).
  - Pull `!tp`, `!alertdist`, `!nmconfig` config command groups out into a single `commands/config_commands.py` file — they share the "load JSON, validate, save JSON, confirm" pattern.
- **Rationale**: Goal 3.
- **Risk**: Medium — many call sites import from `trading_commands`. Touches the load_extensions list in `bot.py:122-127`.
- **Behavior-preserving?**: Yes.
- **Note**: Unsure if the user wants this. Flagged as the lowest priority in this plan.

### 7.3 The `!goldtollssl` retroactive-update branch
- **Location**: `commands/bot_commands.py:600-731`
- **Current state**: ~130 lines that build a raw SQL `IN (...)` placeholder string by hand, walk the active toll signals, recompute SL, write back, update the in-memory monitor state, and update the embed. This is a one-shot maintenance operation that includes a custom error/skip/update tally.
- **Proposed change**: Extract `database/signal_operations/lifecycle.py::bulk_update_toll_sl(offset, channel_ids) -> {updated, skipped, errored}` helper. The command then just calls it and renders the result. The in-memory-monitor patch can stay in the command file or move into a `monitor.refresh_signal(signal_id)` method.
- **Rationale**: Goal 3 (sprawling function) + Goal 1 (the manual `placeholders = ", ".join(f"${i+1}" ...)` is a SQL-injection-adjacent pattern that should not be repeated).
- **Risk**: Medium — touches DB + embed update + in-memory state.
- **Behavior-preserving?**: Yes.

---

## Area 8: Misc / cross-cutting

### 8.1 Top-of-file docstring noise (`Stage 2 Enhanced`, `REDESIGNED`, `ENHANCED`, `FIXED`)
- **Location**: `main.py:3`, `price_feeds/streaming_monitor.py:1-7`, `price_feeds/price_stream_manager.py:1-6`, `price_feeds/alert_system.py:1-5`, and others
- **Current state**: Many file/class docstrings carry refactor-history metadata ("Stage 2", "REDESIGNED:", "ENHANCED:", "FIXED: Added OANDA practice account support"). This is git-log content, not docs.
- **Proposed change**: Strip the historical preamble lines. Keep the descriptive paragraph.
- **Rationale**: Goal 3/4 (these are AI-generated-code-smell tags).
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### ✅ 8.2 `enhanced_X` / `improved_X` / `EnhancedSignalParser` / `Enhanced DatabaseManager`
- **Location**: `core/parser/__init__.py`, `database/database_manager.py`
- **Done**: Renamed `EnhancedSignalParser` → `SignalParser` (class, type annotations, log message). Stripped "Enhanced" from both `DatabaseManager` docstrings.

### 8.3 Inline `import json` / `from pathlib import Path` inside methods
- **Location**: `price_feeds/alert_system.py:585-589, 605-608, 838-841, 855-858, 1792-1795`; `price_feeds/streaming_monitor.py:117-118, 181-186`; `discord_handlers/message_handler.py:525-528, 614-616`
- **Current state**: ~10 places import `json` and/or `Path` inside method bodies. Same imports already happen at module-load time in other files.
- **Proposed change**: Move to top of file.
- **Rationale**: Goal 4.
- **Risk**: Low.
- **Behavior-preserving?**: Yes.

### 8.4 Loosen the `if hasattr(...)` defensive walls
- **Location**: Throughout `streaming_monitor.py` (e.g. `if hasattr(self.bot, 'guilds')`, `if hasattr(self.bot, 'news_manager')`, `if hasattr(monitor, "alert_config")`, etc.)
- **Current state**: The bot wires up `news_manager`, `monitor`, `guilds`, `alert_config` at startup. The `hasattr` guards exist for "what if it isn't there" cases that can only happen if the bot is in a half-initialized state — which already has bigger problems.
- **Proposed change**: Remove `hasattr` guards for attributes that are always set after `setup_hook` completes. Keep them only at genuine boundaries (e.g. before `bot.guilds[0]` access during startup race conditions).
- **Rationale**: Goal 2 + Goal 4 (defensive checks for impossible cases).
- **Risk**: Medium — these guards exist for a reason in some places. Audit each individually rather than bulk-removing.
- **Behavior-preserving?**: Yes if audited carefully.

---

## Out of scope / not recommended

- **Adding tests across the codebase** — the prompt explicitly excludes this. Untested code stays untested.
- **Splitting `trading_commands.py` fully into N files** — Goal 3 calls out "sprawling functions that should be split," but the file's bulk is from having many commands, not from any single sprawling function. The cohesion penalty of more files would outweigh the readability gain. (Partial extraction of `ActiveSignalsView` and the config commands is on the table; full split is not.)
- **Replacing `discord.py` 2.x with anything else** — out of scope.
- **Switching from `asyncpg` raw SQL to an ORM** — out of scope; performance-critical writes in the hot path benefit from raw SQL.
- **Schema changes (e.g. renaming `signals.id` to `signals.signal_id` to avoid the gotcha entirely)** — prompt explicitly excludes DB schema changes. The Python-side normalisation (3.5) covers it.
- **Replacing `threading`-related code** — there isn't any; the codebase is asyncio-native.
- **Pre-emptive type hints across the codebase** — explicitly excluded.
- **Replacing per-tick `pytz.timezone('America/New_York')` with a module-level constant** — this is one of those changes I considered but the cost (~1 line saved, minor allocator pressure) doesn't clear the bar. `pytz.timezone(name)` is itself cached internally. Skipped.
- **Async file I/O for `channels.json` / `settings.json` reads** — currently sync `open(...)`. Each read is microseconds. Not worth the asyncio overhead.
- **Replacing the `_live_update_task` 15s polling with event-driven updates from price ticks** — this *would* be a real efficiency improvement, but it's a behaviour change (price-tick rate is much higher than 15s, would hit Discord rate limits) and requires a different rate-limiter, so it's a redesign rather than a simplification. Out of scope.