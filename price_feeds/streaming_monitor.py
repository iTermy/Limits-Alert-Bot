"""Streaming price monitor: event-driven signal evaluation from live price feeds."""

import asyncio
from datetime import datetime
from datetime import time as dtime
from typing import Dict, List

import discord
import pytz

from database.utils import calculate_sl_pnl
from models.signal import SignalData
from utils.config_loader import load_settings
from utils.logger import get_logger

logger = get_logger("stream_monitor")


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

        # Spread buffer cache: reloaded every 30 s from settings.json
        self._spread_buffer_enabled = None
        self._last_settings_load = None

        # Track spread hour transitions so we can update bot_mode_status exactly once
        # per transition (not on every price tick).
        self._spread_hour_active: bool = False

        # Performance tracking
        self.stats = {
            "price_updates": 0,
            "signals_checked": 0,
            "limits_hit": 0,
            "stop_losses_hit": 0,
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

        Spread hour runs from 5:00 PM to 6:00 PM US/Eastern (America/New_York)
        on weekdays (Monday-Friday).  During this window broker spreads widen
        significantly, causing false limit/stop hits.
        """
        est = pytz.timezone("America/New_York")
        now_est = datetime.now(est)

        if now_est.weekday() >= 5:
            return False

        spread_start = dtime(17, 0)
        spread_end = dtime(18, 0)

        return spread_start <= now_est.time() < spread_end

    async def start(self):
        """Start the streaming monitor"""
        if self.running:
            logger.warning("Monitor already running")
            return

        self.running = True

        await self._load_and_subscribe_signals()

        asyncio.create_task(self._periodic_signal_refresh())

        self.live_price_writer.start()

        logger.info("Streaming price monitor started")

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

                if symbol not in self.symbol_to_signals:
                    self.symbol_to_signals[symbol] = []
                self.symbol_to_signals[symbol].append(signal_id)

                symbols_needed.add(symbol)

            await self.stream_manager.bulk_subscribe(list(symbols_needed))

            for signal in signals:
                if signal.get("status") == "hit":
                    await self.tp_monitor.refresh_hit_limits(signal["signal_id"])

            logger.info(
                f"Loaded {len(signals)} active signals across {len(symbols_needed)} symbols"
            )

        except Exception as e:
            logger.error(f"Error loading signals: {e}")

    async def _periodic_signal_refresh(self):
        """Periodically refresh signal list (every 30 seconds)"""
        while self.running:
            await asyncio.sleep(30)

            try:
                signals = await self.db.get_active_signals_for_tracking()

                old_symbols = set(self.symbol_to_signals.keys())
                new_symbols = set(signal["instrument"] for signal in signals)

                symbols_to_remove = old_symbols - new_symbols
                for symbol in symbols_to_remove:
                    await self.stream_manager.unsubscribe_symbol(symbol)

                symbols_to_add = new_symbols - old_symbols
                if symbols_to_add:
                    await self.stream_manager.bulk_subscribe(list(symbols_to_add))

                self.active_signals.clear()
                self.symbol_to_signals.clear()

                guild_id = self.bot.guilds[0].id if self.bot.guilds else None

                for signal in signals:
                    signal_id = signal["signal_id"]
                    symbol = signal["instrument"]

                    self.active_signals[signal_id] = signal
                    if guild_id is not None:
                        signal["guild_id"] = guild_id

                    if symbol not in self.symbol_to_signals:
                        self.symbol_to_signals[symbol] = []
                    self.symbol_to_signals[symbol].append(signal_id)

                    if signal.get("status") == "hit":
                        await self.tp_monitor.refresh_hit_limits(signal_id)

                if symbols_to_add or symbols_to_remove:
                    logger.info(
                        f"Signal refresh: +{len(symbols_to_add)} -{len(symbols_to_remove)} symbols"
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

        signal_ids = self.symbol_to_signals.get(symbol, [])

        if not signal_ids:
            return

        # Refresh spread buffer setting at most every 30 s
        now = datetime.now()
        if (
            self._last_settings_load is None
            or (now - self._last_settings_load).total_seconds() > 30
        ):
            try:
                settings = load_settings()
                self._spread_buffer_enabled = settings.spread_buffer_enabled
            except Exception as e:
                logger.error(f"Error loading spread buffer setting: {e}, using default (True)")
                self._spread_buffer_enabled = True
            self._last_settings_load = now
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
                        f"Spread buffer ALLOWED alert for {symbol}: "
                        f"ask={current_price:.5f}, limit={limit_price:.5f}, "
                        f"spread={spread:.5f}, within buffer"
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
                        f"Spread buffer ALLOWED alert for {symbol}: "
                        f"bid={current_price:.5f}, limit={limit_price:.5f}, "
                        f"spread={spread:.5f}, within buffer"
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

            if is_spread_hour:
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
            if is_spread_hour:
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

            if result.get("all_limits_hit"):
                logger.info(
                    f"All limits hit for signal {signal['signal_id']} — refreshing TP cache, continuing to watch for auto-TP"
                )
                await self.tp_monitor.refresh_hit_limits(signal["signal_id"])
                signal["status"] = "hit"
                self.nm_monitor.evict_signal(signal["signal_id"])
            else:
                await self.tp_monitor.refresh_hit_limits(signal["signal_id"])
                signal["status"] = "hit"
                self.nm_monitor.evict_signal(signal["signal_id"])

        except Exception as e:
            logger.error(f"Failed to process limit hit: {e}")

    async def _process_stop_loss_hit(self, signal: Dict):
        """Process stop loss hit"""
        try:
            sl_result_pips = None
            try:
                sl_result_pips = await calculate_sl_pnl(
                    signal["signal_id"], signal, self.signal_db, self.tp_config
                )
            except Exception as e:
                logger.warning(
                    f"Could not calculate SL result_pips for signal {signal['signal_id']}: {e}"
                )

            success = await self.signal_db.manually_set_signal_status(
                signal["signal_id"],
                "stop_loss",
                result_pips=sl_result_pips,
                closed_reason="automatic",
            )

            if success:
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
                logger.info(f"Signal {signal_id} cancelled due to news mode (event: {news_event})")
                self.tp_monitor.evict_signal(signal_id)
                await self._maybe_unsubscribe_symbol(signal["instrument"], signal_id)
                self.stats["news_cancelled"] = self.stats.get("news_cancelled", 0) + 1
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
