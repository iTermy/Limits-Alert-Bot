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

### ✅ 4.1 Cache the spread-hour check on the tick path
- **Location**: `price_feeds/streaming_monitor.py`
- **Done**: `now_in_spread` (already computed at top of `_on_price_update` for the transition check) is now passed as a parameter through `_check_signal` → `_check_limit` / `_check_stop_loss`. The two inner `_is_spread_hour()` calls removed.

### ✅ 4.2 Drop `test_signal_monitoring`
- **Location**: `price_feeds/streaming_monitor.py`
- **Done**: Deleted (was never called).

### ✅ 4.3 Inline `_reload_spread_buffer_setting` + the cache machinery
- **Location**: `price_feeds/streaming_monitor.py`
- **Done**: Deleted `_reload_spread_buffer_setting()` and `_is_spread_buffer_enabled()`. The 30s cache logic is now inlined at the top of `_on_price_update` (before the per-signal loop). `_settings_cache_duration` instance var removed; `_spread_buffer_enabled` and `_last_settings_load` kept for the cache. `spread_buffer_enabled` is computed once per tick and passed down through `_check_signal` → `_check_limit`.
- **Adjacent fix**: `alert_system.py` live-update path had two bugs: the stale `_reload_spread_buffer_setting` call (now removed) and a wrong attribute name (`spread_buffer_enabled` vs `_spread_buffer_enabled`) that always fell back to `False`. Both fixed.

### ✅ 4.4 Cancel-on-spread-hour and cancel-on-news share a guard scaffold
- **Location**: `price_feeds/streaming_monitor.py`
- **Done**: Extracted `_cancel_signal_during_guard(signal, current_price, reason, news_event=None)`. Eviction-before-await ordering preserved exactly. Used from both guards in `_check_limit` and the SL spread-hour guard in `_check_stop_loss`. The SL path now also benefits from the `not in active_signals` early-return guard (preventing duplicate cancels if a concurrent limit check fires first).

### ✅ 4.5 The `signal['guild_id'] = self.bot.guilds[0].id` on every tick
- **Location**: `price_feeds/streaming_monitor.py`
- **Done**: `guild_id` is set once when signals are added to `active_signals` in both `_load_and_subscribe_signals` and `_periodic_signal_refresh`. Removed the per-tick `hasattr(self.bot, "guilds")` patch from `_check_signal`.

---

## Area 5: Alert system

### ✅ 5.1 Remove the dead `EmbedFactory` import + the entire `utils/embed_factory.py`
- **Location**: `utils/embed_factory.py`
- **Done**: Deleted `utils/embed_factory.py` (371 lines). The imports had already been removed from both `message_handler.py` and `alert_system.py` in prior work.

### ✅ 5.2 Consolidate the toll-channel/PA-channel/legends-channel JSON re-reads
- **Location**: `price_feeds/alert_system.py`
- **Done**: Merged `_load_pa_channels()` + `_load_toll_channels()` into a single `_load_channels_config()` that reads `channels.json` once. Added cached `self._finished_channel_id` and `self._profit_channel_id`. Simplified `_get_finished_channel()` and `_get_profit_channel_sync()` to use cached IDs (no disk I/O). Deleted `_get_profit_channel()` async variant (never called). Added `reload_channels()` public method called by `!reload` in `bot_commands.py`.
- **Adjacent fix**: Added `alert_system.reload_channels()` call to `reload_config` command in `bot_commands.py`.

### ✅ 5.3 Hardcoded role-mention constant `<@&1334203997107650662>`
- **Location**: `price_feeds/alert_system.py`, `discord_handlers/message_handler.py`, `config/channels.json`
- **Done**: Added `"alert_role_id": "1334203997107650662"` to `channels.json`. `_load_channels_config()` reads it and sets `self.role_mention`. Replaced all 5 occurrences in `alert_system.py` with `self.role_mention` and the 1 occurrence in `message_handler.py` with `self.alert_system.role_mention`. Fallback to the hardcoded ID if key absent from config.

### ✅ 5.4 Extract archive-footer helper
- **Location**: `price_feeds/alert_system.py`, `discord_handlers/message_handler.py`
- **Done**: Extracted module-level `_set_archive_footer(embed, label="📁 Archived")` helper. Applied at all 4 call sites (two in `_move_after_delay`, one in `_move_standalone_after_delay`, one in `message_handler.py`). The full `_move_to_archive_channel` coroutine merge was not attempted (Medium risk, closure complexity).

### ✅ 5.5 Move `TPConfig`/`NMConfig` imports to module level
- **Location**: `price_feeds/alert_system.py`
- **Done**: `TPConfig` and `NMConfig` now imported at module top. Module-level `_tp_config` singleton instantiated once (no more per-call JSON read). Inline `from price_feeds.tp_config import TPConfig` removed from `_build_signal_embed` and `_build_profit_archive_embed`. Inline `from price_feeds.nm_config import NMConfig` (with stale "avoid circular imports" comment) removed from `send_near_miss_cancel_alert`.
- **Adjacent fix**: Moved inline `import json` / `from pathlib import Path` to module level in `alert_system.py` (were in 5 inline sites).

### 5.6 The status_map → cancel_type → reason_text branching
- **Skipped**: Medium risk — the `if/elif` chain covers event-specific edge cases (news currency suffix, `cancel_type == "automatic"` vs `"expiry"` distinction). A dispatch table that handles all cases correctly is larger than the original and harder to audit for regressions. Leaving as-is.

### ✅ 5.7 `alert_messages` bounded eviction
- **Location**: `price_feeds/alert_system.py`
- **Done**: Changed `self.alert_messages` from `dict` to `collections.OrderedDict`. Eviction loop replaced with `while len > 1000: popitem(last=False)`.

### Adjacent fixes (Area 5 session)
- `discord_handlers/message_handler.py`: Moved `_build_signal_embed` import to module top (was inline). Added `_set_archive_footer` to the same import. Removed redundant inline `from price_feeds.alert_system import _build_signal_embed`.
- `price_feeds/alert_system.py`: Stripped "REDESIGNED:" from module docstring (8.1).

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