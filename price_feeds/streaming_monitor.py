"""Streaming price monitor: event-driven signal evaluation from live price feeds."""

import asyncio
from datetime import datetime, timezone
from datetime import time as dtime
from time import monotonic
from typing import Dict, List, Optional

import discord
import pytz

from models.signal import LimitData
from price_feeds.risky_window import is_risky_trading_disabled
from utils.config_loader import load_settings
from utils.logger import get_logger

logger = get_logger("stream_monitor")

# Ticks older than this (seconds) are dropped before any signal evaluation.
# Guards against stale rollover prints crossing the spread-hour boundary.
_MAX_TICK_AGE_SECONDS = 5

# After an approaching alert has fired, retract the embed once price drifts
# past N × the alert distance away from the first pending limit.
_APPROACHING_RETRACTION_MULTIPLIER = 2.0

# Cache the _is_spread_hour() boolean for this many seconds to avoid
# constructing a tz-aware datetime on every price tick.
_SPREAD_HOUR_CACHE_SECONDS = 5

# Module-level pytz instance so we don't re-resolve the timezone string each call.
_EST_TZ = pytz.timezone("America/New_York")


async def react_to_original_signal(bot, signal: Dict, emoji: str):
    """
    Add a reaction to the original signal message.

    Args:
        bot: The Discord bot instance (used to fetch channels/messages).
        signal: SignalData carrying message_id and channel_id.
        emoji: The emoji to add as a reaction.
    """
    try:
        message_id = signal.message_id
        channel_id = signal.channel_id

        if not message_id or not channel_id or str(message_id).startswith("manual_"):
            return

        try:
            channel = bot.get_channel(int(channel_id))
            if not channel:
                channel = await bot.fetch_channel(int(channel_id))
            if not channel:
                logger.warning(f"Could not find channel {channel_id} for original signal")
                return
            original_message = await channel.fetch_message(int(message_id))
        except discord.NotFound:
            logger.warning(f"Original signal message {message_id} not found")
            return
        except discord.Forbidden:
            logger.warning(f"No permission to access message {message_id}")
            return
        except Exception as e:
            logger.error(f"Error fetching original message: {e}")
            return

        try:
            await original_message.add_reaction(emoji)
            logger.info(f"Added {emoji} reaction to original signal message {message_id}")
        except discord.NotFound:
            logger.warning(f"Could not add reaction to message {message_id} - message not found")
        except discord.Forbidden:
            logger.warning(f"Could not add reaction to message {message_id} - missing permissions")
        except discord.HTTPException as e:
            logger.warning(f"Could not add reaction to message {message_id} - HTTP error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error adding reaction: {e}", exc_info=False)

    except Exception as e:
        logger.error(f"Error adding reaction to original signal: {e}", exc_info=True)


class StreamingPriceMonitor:
    """
    Event-driven price monitor using streaming feeds.

    Reacts to price updates in real-time; includes spread buffer system for
    approaching and hit alerts.

    All subsystems are received via constructor — the monitor does not create
    them.  Channel setup (fetching Discord channels) is handled by the bot
    before the monitor is started.
    """

    def __init__(
        self,
        bot,
        signal_db,
        db,
        *,
        alert_system,
        stream_manager,
        alert_config,
        tp_config,
        tp_monitor,
        nm_config,
        nm_monitor,
        trailing_monitor,
        excursion_monitor,
        live_price_writer,
        health_monitor,
    ):
        self.bot = bot
        self.signal_db = signal_db
        self.db = db

        # Injected subsystems
        self.alert_system = alert_system
        self.stream_manager = stream_manager
        self.alert_config = alert_config
        self.tp_config = tp_config
        self.tp_monitor = tp_monitor
        self.nm_config = nm_config
        self.nm_monitor = nm_monitor
        self.trailing_monitor = trailing_monitor
        self.excursion_monitor = excursion_monitor
        self.live_price_writer = live_price_writer
        self.health_monitor = health_monitor

        # Monitoring state
        self.running = False
        self.active_signals: Dict[int, Dict] = {}  # signal_id -> signal_data
        self.symbol_to_signals: Dict[str, List[int]] = {}  # symbol -> [signal_ids]

        # Strong refs to fire-and-forget reaction tasks so they aren't GC'd
        # mid-flight; discarded on completion.
        self._reaction_tasks: set = set()

        # Spread buffer cache: refreshed by _refresh_spread_buffer_loop every 30 s,
        # never read from disk on the price-tick hot path.
        self._spread_buffer_enabled = True

        # Track spread hour transitions so we can update bot_mode_status exactly once
        # per transition (not on every price tick).
        self._spread_hour_active: bool = False
        self._spread_hour_cached: bool = False
        self._spread_hour_cache_expires: float = 0.0

        # Late market hour (the hour before spread hour) is cached the same way.
        self._late_market_cached: bool = False
        self._late_market_cache_expires: float = 0.0

        # Performance tracking
        self.stats = {
            "price_updates": 0,
            "signals_checked": 0,
            "limits_hit": 0,
            "stop_losses_hit": 0,
            "news_cancelled": 0,
            "errors": 0,
            "buffer_prevented_alerts": 0,
            "buffer_allowed_alerts": 0,
        }

    async def initialize(self):
        """Initialize stream manager and connect health monitoring"""
        try:
            await self.stream_manager.initialize()
            self.stream_manager.add_subscriber(self._on_price_update)
            self.stream_manager.set_health_monitor(self.health_monitor)
            await self.health_monitor.start_monitoring()
            logger.info("Streaming monitor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize monitor: {e}")
            raise

    def _react_async(self, signal: Dict, emoji: str) -> None:
        """Add an emoji reaction to the original signal off the tick hot path.

        Reactions are cosmetic and order-independent, so fetching the message
        and adding the reaction (two API round-trips) must not block per-tick
        signal evaluation or delay the embed edit.
        """
        task = asyncio.create_task(react_to_original_signal(self.bot, signal, emoji))
        self._reaction_tasks.add(task)
        task.add_done_callback(self._reaction_tasks.discard)

    def _is_spread_hour(self) -> bool:
        """
        Check whether the current time falls within the daily spread hour.
        Result cached for _SPREAD_HOUR_CACHE_SECONDS to avoid per-tick tz construction.

        Spread hour runs from 5:00 PM to 6:00 PM US/Eastern (America/New_York)
        on weekdays (Monday-Friday).  During this window broker spreads widen
        significantly, causing false limit/stop hits.
        """
        now_mono = monotonic()
        if now_mono < self._spread_hour_cache_expires:
            return self._spread_hour_cached

        now_est = datetime.now(_EST_TZ)
        if now_est.weekday() >= 5:
            result = False
        else:
            result = dtime(17, 0) <= now_est.time() < dtime(18, 0)

        # Clamp the cache so it never serves a stale value across the 17:00 /
        # 18:00 ET boundaries. Spreads widen exactly at 17:00; a flat 5 s cache
        # would let a spread-induced false hit slip through the gate for up to
        # 5 s after the boundary.
        cache_seconds = _SPREAD_HOUR_CACHE_SECONDS
        for hh in (17, 18):
            boundary = now_est.replace(hour=hh, minute=0, second=0, microsecond=0)
            if boundary > now_est:
                cache_seconds = min(cache_seconds, (boundary - now_est).total_seconds())

        self._spread_hour_cached = result
        self._spread_hour_cache_expires = now_mono + cache_seconds
        return result

    def _is_late_market_hour(self) -> bool:
        """
        Check whether the current time falls within the final market hour —
        the hour immediately before spread hour.

        Late market hour runs from 4:00 PM to 5:00 PM US/Eastern
        (America/New_York) on weekdays. Signals that get hit during this window
        historically underperform, so a fresh entry here is cancelled rather than
        tracked. Cached like _is_spread_hour, with the cache clamped to the
        16:00 / 17:00 ET boundaries so a stale value never leaks across them.
        """
        now_mono = monotonic()
        if now_mono < self._late_market_cache_expires:
            return self._late_market_cached

        now_est = datetime.now(_EST_TZ)
        if now_est.weekday() >= 5:
            result = False
        else:
            result = dtime(16, 0) <= now_est.time() < dtime(17, 0)

        cache_seconds = _SPREAD_HOUR_CACHE_SECONDS
        for hh in (16, 17):
            boundary = now_est.replace(hour=hh, minute=0, second=0, microsecond=0)
            if boundary > now_est:
                cache_seconds = min(cache_seconds, (boundary - now_est).total_seconds())

        self._late_market_cached = result
        self._late_market_cache_expires = now_mono + cache_seconds
        return result

    def _is_crypto_signal(self, signal: Dict) -> bool:
        """Crypto signals run 24/7 and are exempt from spread-hour cancellation."""
        return signal.asset_class == "crypto"

    def _annotate_asset_class(self, signal: Dict) -> None:
        """Cache asset_class on the signal so the hot path doesn't re-scan the symbol."""
        if signal.asset_class:
            return
        try:
            signal.asset_class = self.stream_manager.symbol_mapper.determine_asset_class(
                signal.instrument
            )
        except Exception:
            signal.asset_class = None

    async def start(self):
        """Start the streaming monitor"""
        if self.running:
            logger.warning("Monitor already running")
            return

        self.running = True

        self._refresh_spread_buffer_setting()
        await self._load_and_subscribe_signals()

        asyncio.create_task(self._periodic_signal_refresh())
        asyncio.create_task(self._spread_buffer_refresh_loop())
        asyncio.create_task(self.excursion_monitor.run_volume_sampler())

        self.live_price_writer.start()

        logger.info("Streaming price monitor started")

    def _refresh_spread_buffer_setting(self) -> None:
        """Reload spread_buffer_enabled from settings.json. Cheap; runs every 30 s off the hot path."""
        try:
            settings = load_settings()
            self._spread_buffer_enabled = settings.spread_buffer_enabled
        except Exception as e:
            logger.error("Error loading spread buffer setting: %s, keeping current value", e)

    async def _spread_buffer_refresh_loop(self) -> None:
        """Refresh the cached spread_buffer_enabled flag every 30 s. Decoupled from price ticks."""
        while self.running:
            await asyncio.sleep(30)
            self._refresh_spread_buffer_setting()

    async def stop(self):
        """Stop the streaming monitor"""
        self.running = False
        self.excursion_monitor.stop()

        await self.stream_manager.shutdown()
        await self.live_price_writer.stop()

        if self.health_monitor:
            await self.health_monitor.stop_monitoring()

        logger.info("Streaming price monitor stopped")

    async def _load_and_subscribe_signals(self):
        """Load active signals from database and subscribe to their symbols"""
        try:
            signals = await self.db.get_active_signals_for_tracking()

            self.active_signals.clear()
            self.symbol_to_signals.clear()

            symbols_needed = set()
            guild_id = self.bot.guilds[0].id if self.bot.guilds else None

            if not signals:
                logger.info("No active signals to monitor")

            for signal in signals:
                signal_id = signal.signal_id
                symbol = signal.instrument

                self.active_signals[signal_id] = signal
                if guild_id is not None:
                    signal.guild_id = guild_id
                self._annotate_asset_class(signal)

                if symbol not in self.symbol_to_signals:
                    self.symbol_to_signals[symbol] = []
                self.symbol_to_signals[symbol].append(signal_id)

                symbols_needed.add(symbol)

            for signal in signals:
                if signal.status == "hit":
                    signal_id = signal.signal_id
                    await self.tp_monitor.refresh_hit_limits(signal_id)
                    # Populate hit limits so embeds reconstruct with the full
                    # limit history (pending_limits alone is not enough for HIT).
                    try:
                        hit_limits = await self.signal_db.get_hit_limits_for_signal(signal_id)
                        self.active_signals[signal_id].limits.extend(hit_limits)
                    except Exception as _hit_err:
                        logger.error(
                            f"Failed to load hit limits for signal {signal_id}: {_hit_err}"
                        )

            # Re-hydrate excursion tracking (approach + in-trade) from any open
            # rows so a restart mid-signal keeps accumulating MFE/MAE instead of
            # restarting from scratch.
            for signal in self.active_signals.values():
                try:
                    await self.excursion_monitor.resume_if_tracked(signal)
                except Exception as _exc_err:
                    logger.error(
                        f"Failed to resume excursion for signal {signal.signal_id}: {_exc_err}"
                    )

            # Hydrate alert embed references BEFORE the price stream starts —
            # otherwise the first tick could fire send_*_alert and post a
            # duplicate embed alongside the orphaned one.
            await self.alert_system.hydrate_from_db(list(self.active_signals.values()))

            # End-state signals whose archive countdown was interrupted by a
            # restart need their move re-scheduled, otherwise the alert embed
            # (and the auto-purge-channel original) linger forever.
            await self.alert_system.recover_pending_archives()

            # Re-register recently-archived embeds so reply commands against
            # them (e.g. `reactivate`) survive a restart.
            await self.alert_system.recover_finished_embeds()

            # Resume trailing-stop shadow simulations that were still watching
            # price when the bot last stopped — these signals already closed
            # for real (auto-TP) so they're absent from the query above.
            for shadow_signal in await self.trailing_monitor.recover_incomplete_signals():
                signal_id = shadow_signal.signal_id
                symbol = shadow_signal.instrument
                self.active_signals[signal_id] = shadow_signal
                self.symbol_to_signals.setdefault(symbol, []).append(signal_id)
                symbols_needed.add(symbol)

            await self.stream_manager.bulk_subscribe(list(symbols_needed))

            logger.info(
                f"Loaded {len(signals)} active signals across {len(symbols_needed)} symbols"
            )

        except Exception as e:
            logger.error(f"Error loading signals: {e}")

    async def _periodic_signal_refresh(self):
        """Periodically reconcile in-memory signal state against the DB (every 30 s).

        Applies diffs only — unchanged signals retain their in-memory mutations
        (hit_alert_sent, current_spread, asset_class cache, etc.) instead of
        being rebuilt from scratch.
        """
        while self.running:
            await asyncio.sleep(30)

            try:
                signals = await self.db.get_active_signals_for_tracking()
                new_by_id = {s.signal_id: s for s in signals}
                new_ids = set(new_by_id.keys())
                old_ids = set(self.active_signals.keys())

                guild_id = self.bot.guilds[0].id if self.bot.guilds else None

                ids_removed = old_ids - new_ids
                ids_added = new_ids - old_ids

                # Drop removed signals from in-memory state. Signals flagged
                # _shadow_only (closed for real via auto-TP, but the trailing-stop
                # simulation is still watching) are excluded from the DB query
                # above and must not be evicted here — every other closure path
                # finalizes and evicts its trailing state synchronously instead
                # of relying on this 30s reconciliation.
                removed_symbols: set = set()
                for signal_id in ids_removed:
                    old = self.active_signals.get(signal_id)
                    if old is not None and old.shadow_only:
                        continue
                    old_signal = self.active_signals.pop(signal_id, None)
                    if old_signal is None:
                        continue
                    symbol = old_signal.instrument
                    if symbol and symbol in self.symbol_to_signals:
                        try:
                            self.symbol_to_signals[symbol].remove(signal_id)
                        except ValueError:
                            pass
                        if not self.symbol_to_signals[symbol]:
                            del self.symbol_to_signals[symbol]
                            removed_symbols.add(symbol)

                # Add newly-tracked signals.
                added_symbols: set = set()
                for signal_id in ids_added:
                    signal = new_by_id[signal_id]
                    symbol = signal.instrument
                    if guild_id is not None:
                        signal.guild_id = guild_id
                    self._annotate_asset_class(signal)
                    self.active_signals[signal_id] = signal
                    if symbol not in self.symbol_to_signals:
                        self.symbol_to_signals[symbol] = []
                        added_symbols.add(symbol)
                    self.symbol_to_signals[symbol].append(signal_id)
                    if signal.status == "hit":
                        await self.tp_monitor.refresh_hit_limits(signal_id)

                # Subscribe/unsubscribe only the affected symbols.
                symbols_to_unsub = removed_symbols - added_symbols
                for symbol in symbols_to_unsub:
                    await self.stream_manager.unsubscribe_symbol(symbol)
                if added_symbols:
                    await self.stream_manager.bulk_subscribe(list(added_symbols))

                if added_symbols or symbols_to_unsub:
                    logger.info(
                        f"Signal refresh: +{len(added_symbols)} -{len(symbols_to_unsub)} symbols "
                        f"(+{len(ids_added)} -{len(ids_removed)} signals)"
                    )

            except Exception as e:
                logger.error(f"Error in periodic refresh: {e}")

    async def _on_price_update(self, symbol: str, price_data: Dict):
        """Callback for price updates from stream manager."""
        self.stats["price_updates"] += 1

        # Spread-hour transition tracking
        now_in_spread = self._is_spread_hour()
        if now_in_spread != self._spread_hour_active:
            self._spread_hour_active = now_in_spread
            try:
                await self.db.set_spread_hour(now_in_spread)
                logger.info(f"Spread hour {'started' if now_in_spread else 'ended'} — DB updated")
            except Exception as _sh_err:
                logger.error(f"Failed to update spread_hour in DB: {_sh_err}")

        # Tick staleness gate: drop ticks whose broker timestamp is too old.
        # This prevents a stale ICMarkets rollover print (e.g. timestamped 17:59:30
        # but processed at 18:00:05) from firing a false hit/SL alert.
        tick_time = price_data.get("updated_at")
        if tick_time is not None:
            if tick_time.tzinfo is None:
                tick_time = tick_time.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - tick_time).total_seconds()
            if age > _MAX_TICK_AGE_SECONDS:
                logger.debug(
                    "Stale tick dropped for %s: %.1fs old (limit %ds)",
                    symbol, age, _MAX_TICK_AGE_SECONDS,
                )
                return

        signal_ids = self.symbol_to_signals.get(symbol, [])

        if not signal_ids:
            return

        spread_buffer_enabled = self._spread_buffer_enabled
        now_in_late_market = self._is_late_market_hour()

        for signal_id in signal_ids:
            signal = self.active_signals.get(signal_id)

            if not signal:
                continue

            try:
                signal.current_spread = price_data.get("spread", 0.0)
                await self._check_signal(
                    signal, price_data, now_in_spread, now_in_late_market, spread_buffer_enabled
                )
                self.stats["signals_checked"] += 1
            except Exception as e:
                logger.error(f"Error checking signal {signal_id}: {e}")
                self.stats["errors"] += 1

    async def _check_signal(
        self,
        signal: Dict,
        price_data: Dict,
        is_spread_hour: bool,
        is_late_market: bool,
        spread_buffer_enabled: bool,
    ):
        """Check a signal against current price."""
        if signal.shadow_only:
            # Real position already closed via auto-TP; only the trailing-stop
            # shadow simulation is still watching this symbol. Skip every real
            # alert/status check entirely.
            all_done = await self.trailing_monitor.update(
                signal, price_data["bid"], price_data["ask"]
            )
            if all_done:
                signal_id = signal.signal_id
                self.active_signals.pop(signal_id, None)
                await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
            return

        direction = signal.direction.lower()
        current_price = price_data["ask"] if direction == "long" else price_data["bid"]

        # An already-HIT signal sitting open when a news window opens is
        # cancelled outright — swings are exempt (they ride news out).
        if signal.status == "hit" and signal.type != "swing" and self.bot.news_manager:
            news_event = self.bot.news_manager.is_news_active_for(signal.instrument)
            if news_event is not None:
                logger.info(
                    f"News mode: cancelling open HIT signal {signal.signal_id} "
                    f"({signal.instrument}) — event: {news_event}"
                )
                await self._cancel_signal_during_guard(
                    signal, current_price, "news", news_event
                )
                return

        # An already-HIT risky signal is cancelled outright the moment a disabled
        # window is active — risky trades do not ride the window out. Active
        # signals are handled by the per-hit / per-SL guards below (cancelled only
        # if they actually trigger during the window).
        if (
            signal.status == "hit"
            and signal.type == "risky"
            and is_risky_trading_disabled()
        ):
            logger.info(
                f"Risky window: cancelling open HIT signal "
                f"{signal.signal_id} ({signal.instrument})"
            )
            await self._cancel_signal_during_guard(signal, current_price, "risky_window")
            return

        for limit in signal.pending_limits:
            await self._check_limit(
                signal, limit, current_price, direction,
                is_spread_hour, is_late_market, spread_buffer_enabled,
            )

        # Near-miss check: only for active signals (not hit) with approaching alert sent
        if signal.status in ("active", None):
            await self.excursion_monitor.update_approach(signal, current_price)
            nm_triggered = self.nm_monitor.update(signal, current_price)
            if nm_triggered:
                signal_id = signal.signal_id
                self.active_signals.pop(signal_id, None)
                self._react_async(signal, "❌")
                success = await self.nm_monitor.trigger_near_miss(signal)
                if success:
                    self._apply_status_to_signal(signal, "cancelled")
                    self.nm_monitor.evict_signal(signal_id)
                    self.tp_monitor.evict_signal(signal_id)
                    await self.excursion_monitor.finalize(signal_id, current_price, "near_miss")
                    await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
                    self.stats["nm_cancels"] = self.stats.get("nm_cancels", 0) + 1
                return

        # Check stop loss
        if signal.stop_loss:
            await self._check_stop_loss(
                signal, current_price, direction, is_spread_hour, is_late_market
            )

        # Check auto take-profit (runs for any HIT signal that has hit limits cached)
        if signal.status == "hit":
            tp_triggered = await self.tp_monitor.check_signal(
                signal,
                current_bid=price_data["bid"],
                current_ask=price_data["ask"],
            )

            signal_id = signal.signal_id

            # Excursion entry is set lazily from the first hit limit (no-op once
            # entered), then MFE/MAE advances each tick.
            await self.excursion_monitor.mark_entry(signal)
            await self.excursion_monitor.update(signal, price_data["bid"], price_data["ask"])

            if tp_triggered:
                close_price = price_data["bid"] if direction == "long" else price_data["ask"]
                # Trailing begins here, at the TP price — the what-if is whether
                # trailing the runner beats taking this fixed auto-TP.
                await self.trailing_monitor.start(signal, close_price)
                await self.trailing_monitor.update(
                    signal, price_data["bid"], price_data["ask"]
                )
                await self.excursion_monitor.finalize(signal_id, close_price, "auto_tp")
                self._apply_status_to_signal(signal, "profit")
                self._react_async(signal, "💰")
                # If the trailing-stop shadow simulation still has open levels,
                # keep this signal subscribed (shadow-only) so we can see how
                # far price runs past this auto-TP — instead of fully evicting.
                if self.trailing_monitor.has_open_levels(signal_id):
                    signal.shadow_only = True
                    self.tp_monitor.evict_signal(signal_id)
                    self.nm_monitor.evict_signal(signal_id)
                else:
                    await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)

    async def _check_limit(
        self,
        signal: Dict,
        limit: Dict,
        current_price: float,
        direction: str,
        is_spread_hour: bool,
        is_late_market: bool,
        spread_buffer_enabled: bool,
    ):
        """Check if a limit is approaching or hit; applies spread buffer."""
        limit_price = limit.price_level
        symbol = signal.instrument

        spread = signal.current_spread or 0.0

        if spread is None or spread < 0:
            logger.warning(f"Invalid spread for {symbol}: {spread}, using 0")
            spread = 0.0

        if direction == "long":
            distance = current_price - limit_price

            if spread_buffer_enabled:
                is_hit = current_price <= (limit_price + spread)
                if spread > 0 and is_hit and current_price > limit_price:
                    logger.debug(
                        "Spread buffer ALLOWED alert for %s: ask=%.5f, limit=%.5f, spread=%.5f, within buffer",
                        symbol, current_price, limit_price, spread,
                    )
                    self.stats["buffer_allowed_alerts"] += 1
            else:
                is_hit = current_price <= limit_price

        else:  # short
            distance = limit_price - current_price

            if spread_buffer_enabled:
                is_hit = current_price >= (limit_price - spread)
                if spread > 0 and is_hit and current_price < limit_price:
                    logger.debug(
                        "Spread buffer ALLOWED alert for %s: bid=%.5f, limit=%.5f, spread=%.5f, within buffer",
                        symbol, current_price, limit_price, spread,
                    )
                    self.stats["buffer_allowed_alerts"] += 1
            else:
                is_hit = current_price >= limit_price

        # Check if hit (with in-memory flag check)
        if is_hit and not limit.hit_alert_sent:
            # News mode guard
            news_event = None
            if self.bot.news_manager:
                news_event = self.bot.news_manager.is_news_active_for(signal.instrument)

            if news_event is not None:
                logger.info(
                    f"News mode: suppressing limit hit for signal "
                    f"{signal.signal_id} limit #{limit.sequence_number} "
                    f"({signal.instrument} @ {current_price:.5f}) "
                    f"— event: {news_event}"
                )
                await self._cancel_signal_during_guard(signal, current_price, "news", news_event)
                return

            # Risky-trading disabled window: an active risky entry hit during a
            # scheduled window is cancelled. Already-HIT risky signals are
            # cancelled outright in _check_signal before reaching this point.
            if (
                signal.type == "risky"
                and signal.status != "hit"
                and is_risky_trading_disabled()
            ):
                logger.info(
                    f"Risky window: suppressing limit hit for signal "
                    f"{signal.signal_id} limit #{limit.sequence_number} "
                    f"({signal.instrument} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "risky_window")
                return

            if is_spread_hour and not self._is_crypto_signal(signal):
                # An already-HIT signal rides out spread hour: this limit hit is
                # ignored for now and will be marked normally once spread hour
                # ends. Only signals with no hits yet are cancelled.
                if signal.status == "hit":
                    logger.info(
                        f"Spread hour: ignoring limit hit for already-HIT signal "
                        f"{signal.signal_id} limit #{limit.sequence_number} "
                        f"({signal.instrument} @ {current_price:.5f}) — "
                        f"will re-evaluate after spread hour"
                    )
                    return

                logger.info(
                    f"Spread hour: suppressing limit hit for signal "
                    f"{signal.signal_id} limit #{limit.sequence_number} "
                    f"({signal.instrument} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "spread_hour")
                return

            if is_late_market and not self._is_crypto_signal(signal):
                # A fresh entry during the final market hour is cancelled. An
                # already-HIT signal that rolled into the window keeps running
                # normally — this limit hit is processed as usual below.
                if signal.status != "hit":
                    logger.info(
                        f"Late market hour: suppressing limit hit for signal "
                        f"{signal.signal_id} limit #{limit.sequence_number} "
                        f"({signal.instrument} @ {current_price:.5f})"
                    )
                    await self._cancel_signal_during_guard(
                        signal, current_price, "late_market"
                    )
                    return

            # Mark the limit hit in memory first so the embed edit and any
            # concurrent live-refresh render identical state — the embed updates
            # in the same beat as the ping instead of briefly reverting.
            limit.status = "hit"
            limit.hit_alert_sent = True

            await self.alert_system.send_limit_hit_alert(
                signal,
                limit,
                current_price,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled,
            )
            self._react_async(signal, "🎯")
            await self._process_limit_hit(signal, limit, current_price)

            self.stats["limits_hit"] += 1

        # Check if approaching (first limit only)
        elif not is_hit and not limit.approaching_alert_sent:
            # Suppress approaching alerts during active news windows
            if self.bot.news_manager and self.bot.news_manager.is_news_active_for(
                signal.instrument
            ):
                return

            # Suppress approaching alerts for risky signals during a disabled window
            if signal.type == "risky" and is_risky_trading_disabled():
                return

            if limit.sequence_number == 1:
                try:
                    approaching_distance = self.alert_config.get_approaching_distance(
                        symbol, current_price=current_price
                    )
                except Exception as e:
                    logger.error(f"Error getting approaching distance for {symbol}: {e}")
                    approaching_distance = 0.0010

                if abs(distance) <= approaching_distance:
                    formatted_distance = self.alert_config.format_distance_for_display(
                        symbol, abs(distance), current_price
                    )

                    sent = await self.alert_system.send_approaching_alert(
                        signal,
                        limit,
                        current_price,
                        formatted_distance,
                        spread=spread,
                        spread_buffer_enabled=spread_buffer_enabled,
                    )

                    if sent:
                        await self._mark_approaching_sent(limit.id)
                        limit.approaching_alert_sent = True
                        await self.excursion_monitor.start_approach(signal, current_price)

        # Retraction: price has drifted past N x the alert threshold —
        # delete the embed and reset the flag so it can re-fire later.
        elif (
            not is_hit
            and limit.approaching_alert_sent
            and limit.sequence_number == 1
            and signal.status in ("active", None)
        ):
            try:
                approaching_distance = self.alert_config.get_approaching_distance(
                    symbol, current_price=current_price
                )
            except Exception as e:
                logger.error(f"Error getting approaching distance for {symbol}: {e}")
                return

            if abs(distance) > approaching_distance * _APPROACHING_RETRACTION_MULTIPLIER:
                await self._retract_approaching_alert(signal, limit, abs(distance))

    async def _retract_approaching_alert(
        self, signal: Dict, limit: Dict, distance: float
    ) -> None:
        """Drop the approaching embed + reset the flag after a long drift."""
        signal_id = signal.signal_id
        await self.alert_system.retract_approaching_embed(signal_id)
        await self._reset_approaching_sent(limit.id)
        limit.approaching_alert_sent = False
        self.nm_monitor.evict_signal(signal_id)
        # Drop approach-phase excursion state so a later re-approach measures
        # velocity/pre-hit behaviour fresh (the DB row resets on re-approach).
        self.excursion_monitor.evict_signal(signal_id)
        logger.info(
            f"Retracted approaching alert for signal {signal_id} "
            f"({signal.instrument}) — distance {distance:.5f} exceeded "
            f"{_APPROACHING_RETRACTION_MULTIPLIER}x threshold"
        )

    async def _reset_approaching_sent(self, limit_id: int) -> None:
        try:
            async with self.db.get_connection() as conn:
                await conn.execute(
                    "UPDATE limits SET approaching_alert_sent = FALSE WHERE id = $1",
                    limit_id,
                )
        except Exception as e:
            logger.error(f"Failed to reset approaching_alert_sent for limit {limit_id}: {e}")

    async def _check_stop_loss(
        self,
        signal: Dict,
        current_price: float,
        direction: str,
        is_spread_hour: bool,
        is_late_market: bool,
    ):
        """Check if stop loss is hit. Spread buffer is NOT applied."""
        stop_loss = signal.stop_loss

        if signal.sl_alert_sent:
            return

        if direction == "long":
            is_hit = current_price <= stop_loss
        else:
            is_hit = current_price >= stop_loss

        if is_hit:
            if (
                signal.type == "risky"
                and signal.status != "hit"
                and is_risky_trading_disabled()
            ):
                logger.info(
                    f"Risky window: suppressing stop loss hit for signal "
                    f"{signal.signal_id} ({signal.instrument} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "risky_window")
                return

            if is_spread_hour and not self._is_crypto_signal(signal):
                # An already-HIT signal rides out spread hour: this SL hit is
                # ignored for now and will be marked normally once spread hour
                # ends. Only signals with no hits yet are cancelled.
                if signal.status == "hit":
                    logger.info(
                        f"Spread hour: ignoring stop loss hit for already-HIT signal "
                        f"{signal.signal_id} ({signal.instrument} @ {current_price:.5f}) — "
                        f"will re-evaluate after spread hour"
                    )
                    return

                logger.info(
                    f"Spread hour: suppressing stop loss hit for signal "
                    f"{signal.signal_id} ({signal.instrument} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "spread_hour")
                return

            if is_late_market and not self._is_crypto_signal(signal):
                # Only fresh entries are cancelled in the final market hour; an
                # already-HIT signal takes its real stop loss below.
                if signal.status != "hit":
                    logger.info(
                        f"Late market hour: suppressing stop loss hit for signal "
                        f"{signal.signal_id} ({signal.instrument} @ {current_price:.5f})"
                    )
                    await self._cancel_signal_during_guard(
                        signal, current_price, "late_market"
                    )
                    return

            signal.sl_alert_sent = True

            await self.alert_system.send_stop_loss_alert(signal, current_price)
            self._react_async(signal, "🛑")
            await self._process_stop_loss_hit(signal, current_price)
            self.stats["stop_losses_hit"] += 1

    async def _cancel_signal_during_guard(
        self, signal: Dict, current_price: float, reason: str, news_event=None
    ):
        """Shared evict-alert-react-process path for news and spread-hour guards."""
        signal_id = signal.signal_id
        if signal_id not in self.active_signals:
            return
        self.active_signals.pop(signal_id, None)
        await self.trailing_monitor.finalize_with_price(signal_id, current_price, reason=reason)
        self.trailing_monitor.evict_signal(signal_id)
        await self.excursion_monitor.finalize(signal_id, current_price, reason)
        if reason == "news":
            await self.alert_system.send_news_cancel_alert(signal, current_price, news_event)
        elif reason == "late_market":
            await self.alert_system.send_late_market_cancel_alert(signal, current_price)
        elif reason == "risky_window":
            await self.alert_system.send_risky_window_cancel_alert(signal, current_price)
        else:
            await self.alert_system.send_spread_hour_cancel_alert(signal, current_price)
        self._react_async(signal, "❌")
        if reason == "news":
            await self._process_news_cancel(signal, news_event)
        elif reason == "late_market":
            await self._process_late_market_cancel(signal)
        elif reason == "risky_window":
            await self._process_risky_window_cancel(signal)
        else:
            await self._process_spread_hour_cancel(signal)

    async def _mark_approaching_sent(self, limit_id: int):
        """Mark that approaching alert has been sent"""
        try:
            query = "UPDATE limits SET approaching_alert_sent = TRUE WHERE id = $1"
            async with self.db.get_connection() as conn:
                await conn.execute(query, limit_id)
        except Exception as e:
            logger.error(f"Failed to mark approaching sent: {e}")

    async def _process_limit_hit(self, signal: Dict, limit: Dict, actual_price: float):
        """Process limit hit in database"""
        try:
            result = await self.signal_db.process_limit_hit(limit.id, actual_price)

            now = datetime.now(timezone.utc)
            limit.status = "hit"
            limit.hit_time = now
            limit.hit_price = actual_price

            await self.tp_monitor.refresh_hit_limits(signal.signal_id)
            signal.status = "hit"
            self.nm_monitor.evict_signal(signal.signal_id)

            if result.get("all_limits_hit"):
                logger.info(
                    "All limits hit for signal %s — continuing to watch for auto-TP",
                    signal.signal_id,
                )

        except Exception as e:
            logger.error(f"Failed to process limit hit: {e}")

    def _mutate_limit_hit_in_memory(
        self, signal_id: int, limit_id: int, hit_price: Optional[float] = None
    ) -> None:
        """Reflect a DB limit-hit into the in-memory signal so the live embed
        refresh doesn't need to re-query Postgres each cycle."""
        signal = self.active_signals.get(signal_id)
        if not signal:
            return
        now = datetime.now(timezone.utc)
        for limit in signal.limits:
            if limit.id == limit_id:
                limit.status = "hit"
                limit.hit_alert_sent = True
                limit.hit_time = now
                if hit_price is not None:
                    limit.hit_price = hit_price
                return

    def mark_first_pending_limit_hit_in_memory(
        self, signal_id: int, hit_price: Optional[float] = None
    ) -> None:
        """Mirror manually_set_signal_to_hit by mutating the first pending in-memory limit."""
        signal = self.active_signals.get(signal_id)
        if not signal:
            return
        pending = [l for l in signal.limits if l.status == "pending"]
        if not pending:
            return
        first = min(pending, key=lambda l: l.sequence_number)
        now = datetime.now(timezone.utc)
        first["status"] = "hit"
        first["hit_alert_sent"] = True
        first["hit_time"] = now
        if hit_price is not None:
            first["hit_price"] = hit_price
        else:
            first["hit_price"] = first.get("price_level")

    @staticmethod
    def _apply_status_to_signal(signal: Dict, new_status: str) -> None:
        """Mirror the DB-side limit-state side effects of manually_set_signal_status."""
        signal.status = new_status
        terminal = {"cancelled", "profit", "breakeven", "stop_loss"}
        for limit in signal.limits:
            current = limit.status
            if new_status in terminal and current == "pending":
                limit.status = "cancelled"
            elif new_status in ("active", "hit") and current == "cancelled":
                limit.status = "pending"

    def sync_signal_status_in_memory(self, signal_id: int, new_status: str) -> None:
        """Reflect a DB-side signal status change onto the in-memory signal +
        its limit list. See _apply_status_to_signal for limit-state rules.
        Falls back to the alert_system's live-embed signal dict so that signals
        that have already been popped from active_signals (e.g. by an evict
        path) still see their status synced for ongoing live updates.
        """
        signal = self.active_signals.get(signal_id)
        if signal is None and self.alert_system is not None:
            entry = self.alert_system._live_embeds.get(signal_id)
            if entry:
                signal = entry.get("signal")
        if signal is None:
            return
        self._apply_status_to_signal(signal, new_status)

    async def finalize_trailing_on_manual_close(self, signal_id: int) -> None:
        """Close out trailing-stop and excursion tracking after a manual status
        override (setstatus/profit/cancel/bulk-cancel) that doesn't have a
        precise tick price on hand. No-op for whichever isn't tracking this
        signal — trailing only starts at HIT, excursion starts at approach.
        """
        tracking_trailing = self.trailing_monitor.is_tracking(signal_id)
        tracking_excursion = self.excursion_monitor.is_tracking(signal_id)
        if not tracking_trailing and not tracking_excursion:
            return

        signal = self.active_signals.get(signal_id)
        instrument = signal.instrument if signal else None
        direction = (signal.direction if signal else "") or ""

        price = None
        if instrument:
            price_row = await self.db.fetch_one(
                "SELECT bid, ask FROM live_prices WHERE symbol = $1", (instrument.upper(),)
            )
            if price_row:
                price = price_row["bid"] if direction.lower() == "long" else price_row["ask"]

        if tracking_trailing and price is not None:
            await self.trailing_monitor.finalize_with_price(
                signal_id, price, reason="manual_close"
            )
        self.trailing_monitor.evict_signal(signal_id)

        if tracking_excursion:
            if price is not None:
                await self.excursion_monitor.finalize(signal_id, price, "manual_close")
            else:
                self.excursion_monitor.evict_signal(signal_id)

    async def refresh_signal_in_memory(self, signal_id: int) -> bool:
        """Re-fetch a signal from the DB and bring all in-memory caches in
        lockstep with the new row.

        The periodic refresh only diffs which signal_ids exist — it does not
        re-pull updated limits/SL/instrument for a signal_id that stays
        present. After a message edit or a reactivation the active_signals
        entry and the alert system's live-embed entry would otherwise serve
        stale data until restart (price ticks evaluate old limits; the 15s
        live refresh re-renders the embed with the pre-update dict).

        Returns True if anything was updated.
        """
        try:
            fresh = await self.signal_db.get_signal_with_limits(signal_id)
        except Exception as e:
            logger.warning(f"refresh_signal_in_memory: DB fetch failed for {signal_id}: {e}")
            return False
        if fresh is None:
            return False

        guild_id = self.bot.guilds[0].id if self.bot.guilds else None
        if guild_id is not None:
            fresh.guild_id = guild_id
        self._annotate_asset_class(fresh)

        new_symbol = fresh.instrument
        new_status = fresh.status

        old = self.active_signals.get(signal_id)
        old_symbol = old.instrument if old else None

        # Drop the old symbol bucket entry when the instrument changed or the
        # signal is no longer trackable. Unsubscribing the feed is fine here:
        # if the new symbol is the same we'll re-add and re-subscribe below.
        if old_symbol and (old_symbol != new_symbol or new_status not in ("active", "hit")):
            bucket = self.symbol_to_signals.get(old_symbol)
            if bucket:
                try:
                    bucket.remove(signal_id)
                except ValueError:
                    pass
                if not bucket:
                    del self.symbol_to_signals[old_symbol]
                    try:
                        await self.stream_manager.unsubscribe_symbol(old_symbol)
                    except Exception as e:
                        logger.warning(
                            f"refresh_signal_in_memory: unsubscribe {old_symbol} failed: {e}"
                        )

        if new_status in ("active", "hit"):
            self.active_signals[signal_id] = fresh
            if new_symbol:
                needs_subscribe = new_symbol not in self.symbol_to_signals
                bucket = self.symbol_to_signals.setdefault(new_symbol, [])
                if signal_id not in bucket:
                    bucket.append(signal_id)
                if needs_subscribe:
                    try:
                        await self.stream_manager.bulk_subscribe([new_symbol])
                    except Exception as e:
                        logger.warning(
                            f"refresh_signal_in_memory: subscribe {new_symbol} failed: {e}"
                        )
            if new_status == "hit":
                try:
                    await self.tp_monitor.refresh_hit_limits(signal_id)
                except Exception as e:
                    logger.warning(
                        f"refresh_signal_in_memory: TP refresh for {signal_id} failed: {e}"
                    )
        else:
            self.active_signals.pop(signal_id, None)

        # The live-update loop reads signal.limits from this stored entry to
        # avoid hitting the DB every 15 s. After an edit/reactivate the stored
        # signal is stale, so swap it for the fresh fetch.
        if self.alert_system is not None:
            entry = self.alert_system._live_embeds.get(signal_id)
            if entry:
                entry["signal"] = fresh

        return True

    async def _process_stop_loss_hit(self, signal: Dict, current_price: float):
        """Process stop loss hit"""
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal.signal_id,
                "stop_loss",
                closed_reason="automatic",
            )

            if success:
                self.sync_signal_status_in_memory(signal.signal_id, "stop_loss")
                logger.info(f"Signal {signal.signal_id} marked as stop loss")
                self.tp_monitor.evict_signal(signal.signal_id)
                await self.trailing_monitor.finalize_with_price(
                    signal.signal_id, current_price, reason="real_sl"
                )
                self.trailing_monitor.evict_signal(signal.signal_id)
                await self.excursion_monitor.finalize(
                    signal.signal_id, current_price, "real_sl"
                )
                await self._maybe_unsubscribe_symbol(signal.instrument, signal.signal_id)

        except Exception as e:
            logger.error(f"Failed to process stop loss: {e}")

    async def _process_spread_hour_cancel(self, signal: Dict):
        """Cancel a signal that was falsely triggered during spread hour."""
        signal_id = signal.signal_id
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal_id,
                "cancelled",
                reason="spread_hour_auto_cancel",
                closed_reason="spread_hour",
            )
            if success:
                self.sync_signal_status_in_memory(signal_id, "cancelled")
                logger.info(f"Signal {signal_id} cancelled due to spread hour hit")
                self.tp_monitor.evict_signal(signal_id)
                await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
                self.stats["spread_hour_cancels"] = self.stats.get("spread_hour_cancels", 0) + 1
            else:
                logger.error(f"Failed to cancel signal {signal_id} for spread hour")
        except Exception as e:
            logger.error(f"Error cancelling signal {signal_id} for spread hour: {e}")

    async def _process_late_market_cancel(self, signal: Dict):
        """Cancel a fresh entry that triggered during the final market hour."""
        signal_id = signal.signal_id
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal_id,
                "cancelled",
                reason="late_market_auto_cancel",
                closed_reason="late_market",
            )
            if success:
                self.sync_signal_status_in_memory(signal_id, "cancelled")
                logger.info(f"Signal {signal_id} cancelled due to late market hour hit")
                self.tp_monitor.evict_signal(signal_id)
                await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
                self.stats["late_market_cancels"] = self.stats.get("late_market_cancels", 0) + 1
            else:
                logger.error(f"Failed to cancel signal {signal_id} for late market hour")
        except Exception as e:
            logger.error(f"Error cancelling signal {signal_id} for late market hour: {e}")

    async def _process_risky_window_cancel(self, signal: Dict):
        """Cancel a risky signal that triggered during a disabled window."""
        signal_id = signal.signal_id
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal_id,
                "cancelled",
                reason="risky_window_auto_cancel",
                closed_reason="risky_window",
            )
            if success:
                self.sync_signal_status_in_memory(signal_id, "cancelled")
                logger.info(f"Signal {signal_id} cancelled due to risky disabled window")
                self.tp_monitor.evict_signal(signal_id)
                await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
                self.stats["risky_window_cancels"] = (
                    self.stats.get("risky_window_cancels", 0) + 1
                )
            else:
                logger.error(f"Failed to cancel signal {signal_id} for risky window")
        except Exception as e:
            logger.error(f"Error cancelling signal {signal_id} for risky window: {e}")

    async def _process_news_cancel(self, signal: Dict, news_event) -> None:
        """Cancel a signal that was triggered during an active news window."""
        signal_id = signal.signal_id
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal_id,
                "cancelled",
                reason=f"news_auto_cancel:{news_event.category.upper()}",
                closed_reason=f"news:{news_event.category.upper()}",
            )
            if success:
                self.sync_signal_status_in_memory(signal_id, "cancelled")
                logger.info(f"Signal {signal_id} cancelled due to news mode (event: {news_event})")
                self.tp_monitor.evict_signal(signal_id)
                await self._maybe_unsubscribe_symbol(signal.instrument, signal_id)
                self.stats["news_cancelled"] += 1
            else:
                logger.error(f"Failed to cancel signal {signal_id} for news mode")
        except Exception as e:
            logger.error(f"Error cancelling signal {signal_id} for news mode: {e}")

    async def _maybe_unsubscribe_symbol(self, symbol: str, completed_signal_id: int):
        """Unsubscribe from symbol if no other active signals need it"""
        if symbol in self.symbol_to_signals:
            if completed_signal_id in self.symbol_to_signals[symbol]:
                self.symbol_to_signals[symbol].remove(completed_signal_id)

            if not self.symbol_to_signals[symbol]:
                await self.stream_manager.unsubscribe_symbol(symbol)
                del self.symbol_to_signals[symbol]
                logger.info(f"Unsubscribed from {symbol} (no active signals)")

        self.active_signals.pop(completed_signal_id, None)
        self.tp_monitor.evict_signal(completed_signal_id)
        self.nm_monitor.evict_signal(completed_signal_id)

    def get_stats(self) -> Dict:
        """Get monitoring statistics"""
        return {
            **self.stats,
            "running": self.running,
            "active_signals": len(self.active_signals),
            "monitored_symbols": len(self.symbol_to_signals),
            "spread_buffer_enabled": self._spread_buffer_enabled,
            "nm_tracking_count": self.nm_monitor.get_tracked_count(),
            "stream_manager": self.stream_manager.get_stats(),
            "alert_stats": self.alert_system.get_stats(),
        }
