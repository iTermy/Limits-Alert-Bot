"""Streaming price monitor: event-driven signal evaluation from live price feeds."""

import asyncio
from datetime import datetime, timezone
from datetime import time as dtime
from time import monotonic
from typing import Dict, List, Optional

import discord
import pytz

from models.signal import LimitData
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
        signal: Signal dictionary containing message_id and channel_id.
        emoji: The emoji to add as a reaction.
    """
    try:
        message_id = signal.get("message_id")
        channel_id = signal.get("channel_id")

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
        self.live_price_writer = live_price_writer
        self.health_monitor = health_monitor

        # Monitoring state
        self.running = False
        self.active_signals: Dict[int, Dict] = {}  # signal_id -> signal_data
        self.symbol_to_signals: Dict[str, List[int]] = {}  # symbol -> [signal_ids]

        # Spread buffer cache: refreshed by _refresh_spread_buffer_loop every 30 s,
        # never read from disk on the price-tick hot path.
        self._spread_buffer_enabled = True

        # Track spread hour transitions so we can update bot_mode_status exactly once
        # per transition (not on every price tick).
        self._spread_hour_active: bool = False
        self._spread_hour_cached: bool = False
        self._spread_hour_cache_expires: float = 0.0

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

        self._spread_hour_cached = result
        self._spread_hour_cache_expires = now_mono + _SPREAD_HOUR_CACHE_SECONDS
        return result

    def _is_crypto_signal(self, signal: Dict) -> bool:
        """Crypto signals run 24/7 and are exempt from spread-hour cancellation."""
        return signal.get("asset_class") == "crypto"

    def _annotate_asset_class(self, signal: Dict) -> None:
        """Cache asset_class on the signal so the hot path doesn't re-scan the symbol."""
        if signal.get("asset_class"):
            return
        try:
            signal["asset_class"] = self.stream_manager.symbol_mapper.determine_asset_class(
                signal["instrument"]
            )
        except Exception:
            signal["asset_class"] = None

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

        await self.stream_manager.shutdown()
        await self.live_price_writer.stop()

        if self.health_monitor:
            await self.health_monitor.stop_monitoring()

        logger.info("Streaming price monitor stopped")

    async def _load_and_subscribe_signals(self):
        """Load active signals from database and subscribe to their symbols"""
        try:
            signals = await self.db.get_active_signals_for_tracking()

            if not signals:
                logger.info("No active signals to monitor")
                return

            self.active_signals.clear()
            self.symbol_to_signals.clear()

            symbols_needed = set()
            guild_id = self.bot.guilds[0].id if self.bot.guilds else None

            for signal in signals:
                signal_id = signal["signal_id"]
                symbol = signal["instrument"]

                self.active_signals[signal_id] = signal
                if guild_id is not None:
                    signal["guild_id"] = guild_id
                self._annotate_asset_class(signal)

                if symbol not in self.symbol_to_signals:
                    self.symbol_to_signals[symbol] = []
                self.symbol_to_signals[symbol].append(signal_id)

                symbols_needed.add(symbol)

            for signal in signals:
                if signal.get("status") == "hit":
                    signal_id = signal["signal_id"]
                    await self.tp_monitor.refresh_hit_limits(signal_id)
                    # Populate hit limits so embeds reconstruct with the full
                    # limit history (pending_limits alone is not enough for HIT).
                    try:
                        hit_limit_rows = await self.signal_db.get_hit_limits_for_signal(signal_id)
                        for row in hit_limit_rows:
                            self.active_signals[signal_id].limits.append(
                                LimitData(
                                    id=row["limit_id"],
                                    signal_id=signal_id,
                                    price_level=row["price_level"],
                                    sequence_number=row["sequence_number"],
                                    status="hit",
                                    hit_time=row.get("hit_time"),
                                    hit_price=row.get("hit_price"),
                                )
                            )
                    except Exception as _hit_err:
                        logger.error(
                            f"Failed to load hit limits for signal {signal_id}: {_hit_err}"
                        )

            # Hydrate alert embed references BEFORE the price stream starts —
            # otherwise the first tick could fire send_*_alert and post a
            # duplicate embed alongside the orphaned one.
            await self.alert_system.hydrate_from_db(list(self.active_signals.values()))

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
                new_by_id = {s["signal_id"]: s for s in signals}
                new_ids = set(new_by_id.keys())
                old_ids = set(self.active_signals.keys())

                guild_id = self.bot.guilds[0].id if self.bot.guilds else None

                ids_removed = old_ids - new_ids
                ids_added = new_ids - old_ids

                # Drop removed signals from in-memory state.
                removed_symbols: set = set()
                for signal_id in ids_removed:
                    old_signal = self.active_signals.pop(signal_id, None)
                    if old_signal is None:
                        continue
                    symbol = old_signal.get("instrument")
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
                    symbol = signal["instrument"]
                    if guild_id is not None:
                        signal["guild_id"] = guild_id
                    self._annotate_asset_class(signal)
                    self.active_signals[signal_id] = signal
                    if symbol not in self.symbol_to_signals:
                        self.symbol_to_signals[symbol] = []
                        added_symbols.add(symbol)
                    self.symbol_to_signals[symbol].append(signal_id)
                    if signal.get("status") == "hit":
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

        for signal_id in signal_ids:
            signal = self.active_signals.get(signal_id)

            if not signal:
                continue

            try:
                signal["current_spread"] = price_data.get("spread", 0.0)
                await self._check_signal(signal, price_data, now_in_spread, spread_buffer_enabled)
                self.stats["signals_checked"] += 1
            except Exception as e:
                logger.error(f"Error checking signal {signal_id}: {e}")
                self.stats["errors"] += 1

    async def _check_signal(
        self, signal: Dict, price_data: Dict, is_spread_hour: bool, spread_buffer_enabled: bool
    ):
        """Check a signal against current price."""
        direction = signal["direction"].lower()
        current_price = price_data["ask"] if direction == "long" else price_data["bid"]

        for limit in signal.get("pending_limits", []):
            await self._check_limit(
                signal, limit, current_price, direction, is_spread_hour, spread_buffer_enabled
            )

        # Near-miss check: only for active signals (not hit) with approaching alert sent
        if signal.get("status") in ("active", None):
            nm_triggered = self.nm_monitor.update(signal, current_price)
            if nm_triggered:
                signal_id = signal["signal_id"]
                self.active_signals.pop(signal_id, None)
                await react_to_original_signal(self.bot, signal, "❌")
                success = await self.nm_monitor.trigger_near_miss(signal)
                if success:
                    self._apply_status_to_signal(signal, "cancelled")
                    self.nm_monitor.evict_signal(signal_id)
                    self.tp_monitor.evict_signal(signal_id)
                    await self._maybe_unsubscribe_symbol(signal["instrument"], signal_id)
                    self.stats["nm_cancels"] = self.stats.get("nm_cancels", 0) + 1
                return

        # Check stop loss
        if signal.get("stop_loss"):
            await self._check_stop_loss(signal, current_price, direction, is_spread_hour)

        # Check auto take-profit (runs for any HIT signal that has hit limits cached)
        if signal.get("status") == "hit":
            tp_triggered = await self.tp_monitor.check_signal(
                signal,
                current_bid=price_data["bid"],
                current_ask=price_data["ask"],
            )
            if tp_triggered:
                self._apply_status_to_signal(signal, "profit")
                await react_to_original_signal(self.bot, signal, "💰")
                await self._maybe_unsubscribe_symbol(signal["instrument"], signal["signal_id"])

    async def _check_limit(
        self,
        signal: Dict,
        limit: Dict,
        current_price: float,
        direction: str,
        is_spread_hour: bool,
        spread_buffer_enabled: bool,
    ):
        """Check if a limit is approaching or hit; applies spread buffer."""
        limit_price = limit["price_level"]
        symbol = signal["instrument"]

        spread = signal.get("current_spread", 0.0)

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
        if is_hit and not limit.get("hit_alert_sent", False):
            # News mode guard
            news_event = None
            if self.bot.news_manager:
                news_event = self.bot.news_manager.is_news_active_for(signal["instrument"])

            if news_event is not None:
                logger.info(
                    f"News mode: suppressing limit hit for signal "
                    f"{signal['signal_id']} limit #{limit['sequence_number']} "
                    f"({signal['instrument']} @ {current_price:.5f}) "
                    f"— event: {news_event}"
                )
                await self._cancel_signal_during_guard(signal, current_price, "news", news_event)
                return

            if is_spread_hour and not self._is_crypto_signal(signal):
                logger.info(
                    f"Spread hour: suppressing limit hit for signal "
                    f"{signal['signal_id']} limit #{limit['sequence_number']} "
                    f"({signal['instrument']} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "spread_hour")
                return

            await self.alert_system.send_limit_hit_alert(
                signal,
                limit,
                current_price,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled,
            )
            await react_to_original_signal(self.bot, signal, "🎯")
            await self._process_limit_hit(signal, limit, current_price)

            limit["hit_alert_sent"] = True
            self.stats["limits_hit"] += 1

        # Check if approaching (first limit only)
        elif not is_hit and not limit.get("approaching_alert_sent", False):
            # Suppress approaching alerts during active news windows
            if self.bot.news_manager and self.bot.news_manager.is_news_active_for(
                signal["instrument"]
            ):
                return

            if limit["sequence_number"] == 1:
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
                        await self._mark_approaching_sent(limit["id"])
                        limit["approaching_alert_sent"] = True

        # Retraction: price has drifted past N x the alert threshold —
        # delete the embed and reset the flag so it can re-fire later.
        elif (
            not is_hit
            and limit.get("approaching_alert_sent", False)
            and limit["sequence_number"] == 1
            and signal.get("status") in ("active", None)
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
        signal_id = signal["signal_id"]
        await self.alert_system.retract_approaching_embed(signal_id)
        await self._reset_approaching_sent(limit["id"])
        limit["approaching_alert_sent"] = False
        self.nm_monitor.evict_signal(signal_id)
        logger.info(
            f"Retracted approaching alert for signal {signal_id} "
            f"({signal['instrument']}) — distance {distance:.5f} exceeded "
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
        self, signal: Dict, current_price: float, direction: str, is_spread_hour: bool
    ):
        """Check if stop loss is hit. Spread buffer is NOT applied."""
        stop_loss = signal["stop_loss"]

        if signal.get("sl_alert_sent", False):
            return

        if direction == "long":
            is_hit = current_price <= stop_loss
        else:
            is_hit = current_price >= stop_loss

        if is_hit:
            if is_spread_hour and not self._is_crypto_signal(signal):
                logger.info(
                    f"Spread hour: suppressing stop loss hit for signal "
                    f"{signal['signal_id']} ({signal['instrument']} @ {current_price:.5f})"
                )
                await self._cancel_signal_during_guard(signal, current_price, "spread_hour")
                return

            signal["sl_alert_sent"] = True

            await self.alert_system.send_stop_loss_alert(signal, current_price)
            await react_to_original_signal(self.bot, signal, "🛑")
            await self._process_stop_loss_hit(signal)
            self.stats["stop_losses_hit"] += 1

    async def _cancel_signal_during_guard(
        self, signal: Dict, current_price: float, reason: str, news_event=None
    ):
        """Shared evict-alert-react-process path for news and spread-hour guards."""
        signal_id = signal["signal_id"]
        if signal_id not in self.active_signals:
            return
        self.active_signals.pop(signal_id, None)
        if reason == "news":
            await self.alert_system.send_news_cancel_alert(signal, current_price, news_event)
        else:
            await self.alert_system.send_spread_hour_cancel_alert(signal, current_price)
        await react_to_original_signal(self.bot, signal, "❌")
        if reason == "news":
            await self._process_news_cancel(signal, news_event)
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
            result = await self.signal_db.process_limit_hit(limit["id"], actual_price)

            now = datetime.now(timezone.utc)
            limit["status"] = "hit"
            limit["hit_time"] = now
            limit["hit_price"] = actual_price

            await self.tp_monitor.refresh_hit_limits(signal["signal_id"])
            signal["status"] = "hit"
            self.nm_monitor.evict_signal(signal["signal_id"])

            if result.get("all_limits_hit"):
                logger.info(
                    "All limits hit for signal %s — continuing to watch for auto-TP",
                    signal["signal_id"],
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
        for limit in signal.get("limits") or []:
            if limit.get("id") == limit_id:
                limit["status"] = "hit"
                limit["hit_alert_sent"] = True
                limit["hit_time"] = now
                if hit_price is not None:
                    limit["hit_price"] = hit_price
                return

    def mark_first_pending_limit_hit_in_memory(
        self, signal_id: int, hit_price: Optional[float] = None
    ) -> None:
        """Mirror manually_set_signal_to_hit by mutating the first pending in-memory limit."""
        signal = self.active_signals.get(signal_id)
        if not signal:
            return
        pending = [l for l in (signal.get("limits") or []) if l.get("status") == "pending"]
        if not pending:
            return
        first = min(pending, key=lambda l: l.get("sequence_number", 999))
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
        signal["status"] = new_status
        terminal = {"cancelled", "profit", "breakeven", "stop_loss"}
        for limit in signal.get("limits") or []:
            current = limit.get("status")
            if new_status in terminal and current == "pending":
                limit["status"] = "cancelled"
            elif new_status in ("active", "hit") and current == "cancelled":
                limit["status"] = "pending"

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

    async def _process_stop_loss_hit(self, signal: Dict):
        """Process stop loss hit"""
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal["signal_id"],
                "stop_loss",
                closed_reason="automatic",
            )

            if success:
                self.sync_signal_status_in_memory(signal["signal_id"], "stop_loss")
                logger.info(f"Signal {signal['signal_id']} marked as stop loss")
                self.tp_monitor.evict_signal(signal["signal_id"])
                await self._maybe_unsubscribe_symbol(signal["instrument"], signal["signal_id"])

        except Exception as e:
            logger.error(f"Failed to process stop loss: {e}")

    async def _process_spread_hour_cancel(self, signal: Dict):
        """Cancel a signal that was falsely triggered during spread hour."""
        signal_id = signal["signal_id"]
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
                await self._maybe_unsubscribe_symbol(signal["instrument"], signal_id)
                self.stats["spread_hour_cancels"] = self.stats.get("spread_hour_cancels", 0) + 1
            else:
                logger.error(f"Failed to cancel signal {signal_id} for spread hour")
        except Exception as e:
            logger.error(f"Error cancelling signal {signal_id} for spread hour: {e}")

    async def _process_news_cancel(self, signal: Dict, news_event) -> None:
        """Cancel a signal that was triggered during an active news window."""
        signal_id = signal["signal_id"]
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
                await self._maybe_unsubscribe_symbol(signal["instrument"], signal_id)
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
