"""
Streaming Price Monitor - Simplified version using real-time price streams
Replaces polling + caching with event-driven price updates
ENHANCED: Added spread buffer system for limit checks
ENHANCED: Passes spread info to alert system for display
"""

import asyncio
import logging
from typing import Dict, List
from datetime import datetime
import discord
from price_feeds.feed_health_monitor import FeedHealthMonitor

from price_feeds.price_stream_manager import PriceStreamManager
from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.alert_system import AlertSystem
from utils.logger import get_logger
from utils.config_loader import load_settings

logger = get_logger('stream_monitor')


class StreamingPriceMonitor:
    """
    Event-driven price monitor using streaming feeds

    Much simpler than polling version:
    - No cache management
    - No batch fetching
    - No priority calculations
    - Just react to price updates in real-time

    ENHANCED: Includes spread buffer system for approaching and hit alerts
    ENHANCED: Passes spread values to alert system for display
    """

    def __init__(self, bot, signal_db, db):
        """Initialize streaming monitor"""
        self.bot = bot
        self.signal_db = signal_db
        self.db = db

        # Initialize components
        self.alert_config = AlertDistanceConfig()
        self.stream_manager = PriceStreamManager()
        self.alert_system = AlertSystem(bot=bot)

        # Connect alert system to message handler
        if hasattr(bot, 'message_handler') and bot.message_handler:
            bot.message_handler.alert_system = self.alert_system
            logger.info("Connected alert system to message handler")

        # Monitoring state
        self.running = False
        self.active_signals: Dict[int, Dict] = {}  # signal_id -> signal_data
        self.symbol_to_signals: Dict[str, List[int]] = {}  # symbol -> [signal_ids]

        # Spread buffer cache (to avoid reloading settings every check)
        self._spread_buffer_enabled = None
        self._last_settings_load = None
        self._settings_cache_duration = 30  # Reload settings every 30 seconds

        # Performance tracking
        self.stats = {
            'price_updates': 0,
            'signals_checked': 0,
            'limits_hit': 0,
            'stop_losses_hit': 0,
            'errors': 0,
            'buffer_prevented_alerts': 0,
            'buffer_allowed_alerts': 0
        }

    async def initialize(self):
        """Initialize stream manager and alert system"""
        try:
            # Initialize streaming feeds
            await self.stream_manager.initialize()

            # Register this monitor as a subscriber for price updates
            self.stream_manager.add_subscriber(self._on_price_update)

            # Setup alert channel
            from pathlib import Path
            import json

            config_path = Path(__file__).resolve().parent.parent / 'config' / 'channels.json'

            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    channel_id = config.get('alert_channel')
                    if channel_id:
                        channel = await self.bot.fetch_channel(int(channel_id))
                        self.alert_system.set_channel(channel)
                        logger.info(f"Alert channel set: #{channel.name}")
                    # NEW: Setup PA alert channel
                    pa_channel_id = config.get('pa-alert-channel')
                    if pa_channel_id:
                        try:
                            pa_channel = await self.bot.fetch_channel(int(pa_channel_id))
                            self.alert_system.set_pa_channel(pa_channel)
                            logger.info(f"PA alert channel set: #{pa_channel.name}")
                        except Exception as e:
                            logger.error(f"Failed to set PA alert channel: {e}")
                    else:
                        logger.warning("No PA alert channel configured in channels.json")
                    # NEW: Setup toll alert channel
                    toll_channel_id = config.get('toll-alert-channel')
                    if toll_channel_id:
                        try:
                            toll_channel = await self.bot.fetch_channel(int(toll_channel_id))
                            self.alert_system.set_toll_channel(toll_channel)
                            logger.info(f"Toll alert channel set: #{toll_channel.name}")
                        except Exception as e:
                            logger.error(f"Failed to set toll alert channel: {e}")
                    else:
                        logger.warning("No toll alert channel configured in channels.json")
            except Exception as e:
                logger.error(f"Error setting up alert channel: {e}")

            config_path = Path(__file__).resolve().parent.parent / 'config' / 'health_config.json'
            admin_user_id = None

            try:
                with open(config_path, 'r') as f:
                    health_config = json.load(f)
                    admin_user_id_str = health_config.get('582358569542877184')
                    if admin_user_id_str and admin_user_id_str != '582358569542877184':
                        admin_user_id = int(admin_user_id_str)
            except Exception as e:
                logger.warning(f"Could not load admin user ID: {e}")

            self.health_monitor = FeedHealthMonitor(
                stream_manager=self.stream_manager,
                bot=self.bot,
                admin_user_id=admin_user_id
            )

            self.stream_manager.set_health_monitor(self.health_monitor)
            await self.health_monitor.start_monitoring()

            # Load initial spread buffer setting
            self._reload_spread_buffer_setting()

            logger.info("Streaming monitor initialized")

        except Exception as e:
            logger.error(f"Failed to initialize monitor: {e}")
            raise

    def _reload_spread_buffer_setting(self):
        """Reload spread buffer setting from config (with caching)"""
        now = datetime.now()

        # Check if we need to reload
        if (self._last_settings_load is None or
            (now - self._last_settings_load).total_seconds() > self._settings_cache_duration):

            try:
                settings = load_settings()
                self._spread_buffer_enabled = settings.get('spread_buffer_enabled', True)
                self._last_settings_load = now
                logger.debug(f"Spread buffer setting reloaded: {self._spread_buffer_enabled}")
            except Exception as e:
                logger.error(f"Error loading spread buffer setting: {e}, using default (True)")
                self._spread_buffer_enabled = True
                self._last_settings_load = now

    def _is_spread_buffer_enabled(self) -> bool:
        """
        Check if spread buffer is enabled (with caching)

        Returns:
            True if spread buffer is enabled
        """
        self._reload_spread_buffer_setting()
        return self._spread_buffer_enabled

    async def start(self):
        """Start the streaming monitor"""
        if self.running:
            logger.warning("Monitor already running")
            return

        self.running = True

        # Load active signals and subscribe to their symbols
        await self._load_and_subscribe_signals()

        # Start periodic signal refresh task (every 30 seconds)
        asyncio.create_task(self._periodic_signal_refresh())

        logger.info("Streaming price monitor started")

    async def stop(self):
        """Stop the streaming monitor"""
        self.running = False

        # Shutdown stream manager
        await self.stream_manager.shutdown()

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

            # Clear existing tracking
            self.active_signals.clear()
            self.symbol_to_signals.clear()

            # Group signals by symbol
            symbols_needed = set()

            for signal in signals:
                signal_id = signal['signal_id']
                symbol = signal['instrument']

                # Store signal data
                self.active_signals[signal_id] = signal

                # Track which signals use this symbol
                if symbol not in self.symbol_to_signals:
                    self.symbol_to_signals[symbol] = []
                self.symbol_to_signals[symbol].append(signal_id)

                symbols_needed.add(symbol)

            # Subscribe to all needed symbols
            await self.stream_manager.bulk_subscribe(list(symbols_needed))

            logger.info(f"Loaded {len(signals)} active signals across {len(symbols_needed)} symbols")

        except Exception as e:
            logger.error(f"Error loading signals: {e}")

    async def _periodic_signal_refresh(self):
        """Periodically refresh signal list (every 30 seconds)"""
        while self.running:
            await asyncio.sleep(30)

            try:
                # Reload signals and update subscriptions
                signals = await self.db.get_active_signals_for_tracking()

                old_symbols = set(self.symbol_to_signals.keys())
                new_symbols = set(signal['instrument'] for signal in signals)

                # Unsubscribe from symbols no longer needed
                symbols_to_remove = old_symbols - new_symbols
                for symbol in symbols_to_remove:
                    await self.stream_manager.unsubscribe_symbol(symbol)

                # Subscribe to new symbols
                symbols_to_add = new_symbols - old_symbols
                if symbols_to_add:
                    await self.stream_manager.bulk_subscribe(list(symbols_to_add))

                # Update active signals - CRITICAL: This refreshes alert flags
                self.active_signals.clear()
                self.symbol_to_signals.clear()

                for signal in signals:
                    signal_id = signal['signal_id']
                    symbol = signal['instrument']

                    # Store signal data (includes updated alert flags from database)
                    self.active_signals[signal_id] = signal

                    if symbol not in self.symbol_to_signals:
                        self.symbol_to_signals[symbol] = []
                    self.symbol_to_signals[symbol].append(signal_id)

                if symbols_to_add or symbols_to_remove:
                    logger.info(f"Signal refresh: +{len(symbols_to_add)} -{len(symbols_to_remove)} symbols")

            except Exception as e:
                logger.error(f"Error in periodic refresh: {e}")

    async def _on_price_update(self, symbol: str, price_data: Dict):
        """
        Callback for price updates from stream manager
        This is where the magic happens - instant reaction to price changes

        Args:
            symbol: Symbol that updated
            price_data: Price dictionary with bid, ask, timestamp, spread
        """
        self.stats['price_updates'] += 1

        # Check if we have any signals for this symbol
        signal_ids = self.symbol_to_signals.get(symbol, [])

        if not signal_ids:
            return

        # Check each signal for this symbol
        for signal_id in signal_ids:
            signal = self.active_signals.get(signal_id)

            if not signal:
                continue

            try:
                # Add current spread to signal dict for use in checks
                signal['current_spread'] = price_data.get('spread', 0.0)

                await self._check_signal(signal, price_data)
                self.stats['signals_checked'] += 1
            except Exception as e:
                logger.error(f"Error checking signal {signal_id}: {e}")
                self.stats['errors'] += 1

    async def _check_signal(self, signal: Dict, price_data: Dict):
        """
        Check a signal against current price

        Args:
            signal: Signal dictionary
            price_data: Current price data
        """
        direction = signal['direction'].lower()

        # Determine which price to use
        current_price = price_data['ask'] if direction == 'long' else price_data['bid']

        # Add guild_id for message links
        if hasattr(self.bot, 'guilds') and self.bot.guilds:
            signal['guild_id'] = self.bot.guilds[0].id

        # Check pending limits
        for limit in signal.get('pending_limits', []):
            await self._check_limit(signal, limit, current_price, direction)

        # Check stop loss
        if signal.get('stop_loss'):
            await self._check_stop_loss(signal, current_price, direction)

    async def _check_limit(self, signal: Dict, limit: Dict, current_price: float, direction: str):
        """
        Check if a limit is approaching or hit
        ENHANCED: Applies spread buffer and passes spread info to alerts

        Args:
            signal: Signal dictionary (includes current_spread)
            limit: Limit dictionary
            current_price: Current market price (ask for long, bid for short)
            direction: 'long' or 'short'
        """
        limit_price = limit['price_level']
        symbol = signal['instrument']

        # Get spread from signal dict (set in _on_price_update)
        spread = signal.get('current_spread', 0.0)

        # Validate spread
        if spread is None or spread < 0:
            logger.warning(f"Invalid spread for {symbol}: {spread}, using 0")
            spread = 0.0

        # Check if spread buffer is enabled
        spread_buffer_enabled = self._is_spread_buffer_enabled()

        # Calculate distance and determine if hit
        if direction == 'long':
            distance = current_price - limit_price

            # Apply spread buffer if enabled
            if spread_buffer_enabled:
                # For long: alert when ask <= limit + spread
                is_hit = current_price <= (limit_price + spread)

                if spread > 0 and is_hit and current_price > limit_price:
                    logger.debug(
                        f"Spread buffer ALLOWED alert for {symbol}: "
                        f"ask={current_price:.5f}, limit={limit_price:.5f}, "
                        f"spread={spread:.5f}, within buffer"
                    )
                    self.stats['buffer_allowed_alerts'] += 1
            else:
                # No buffer: exact price check
                is_hit = current_price <= limit_price

        else:  # short
            distance = limit_price - current_price

            # Apply spread buffer if enabled
            if spread_buffer_enabled:
                # For short: alert when bid >= limit - spread
                is_hit = current_price >= (limit_price - spread)

                if spread > 0 and is_hit and current_price < limit_price:
                    logger.debug(
                        f"Spread buffer ALLOWED alert for {symbol}: "
                        f"bid={current_price:.5f}, limit={limit_price:.5f}, "
                        f"spread={spread:.5f}, within buffer"
                    )
                    self.stats['buffer_allowed_alerts'] += 1
            else:
                # No buffer: exact price check
                is_hit = current_price >= limit_price

        # Check if hit (with in-memory flag check)
        if is_hit and not limit.get('hit_alert_sent', False):
            # ENHANCED: Pass spread and buffer status to alert system
            await self.alert_system.send_limit_hit_alert(
                signal, limit, current_price,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled
            )
            # Add reaction to original signal message for limit hit
            await self._react_to_original_signal(signal, "🎯")
            await self._process_limit_hit(signal, limit, current_price)

            # CRITICAL: Update in-memory flag immediately
            limit['hit_alert_sent'] = True

            self.stats['limits_hit'] += 1

        # Check if approaching (first limit only)
        elif not is_hit and not limit.get('approaching_alert_sent', False):
            if limit['sequence_number'] == 1:
                try:
                    approaching_distance = self.alert_config.get_approaching_distance(
                        symbol,
                        current_price=current_price
                    )
                except Exception as e:
                    logger.error(f"Error getting approaching distance for {symbol}: {e}")
                    approaching_distance = 0.0010

                # Distance is now in absolute price units, compare directly
                if abs(distance) <= approaching_distance:
                    # Format distance for display
                    formatted_distance = self.alert_config.format_distance_for_display(
                        symbol,
                        abs(distance),
                        current_price
                    )

                    # ENHANCED: Send alert with spread info
                    await self.alert_system.send_approaching_alert(
                        signal, limit, current_price, formatted_distance,
                        spread=spread,
                        spread_buffer_enabled=spread_buffer_enabled
                    )

                    # Mark as sent in database
                    await self._mark_approaching_sent(limit['limit_id'])

                    # CRITICAL: Update in-memory flag
                    limit['approaching_alert_sent'] = True

    async def _react_to_original_signal(self, signal: Dict, emoji: str):
        """
        Add a reaction to the original signal message

        Args:
            signal: Signal dictionary containing message_id and channel_id
            emoji: The emoji to add as a reaction
        """
        try:
            # Get the original message ID and channel ID
            message_id = signal.get('message_id')
            channel_id = signal.get('channel_id')

            # Skip if this is a manual signal or missing info
            if not message_id or not channel_id or str(message_id).startswith('manual_'):
                logger.debug(f"Skipping original message reaction - manual signal or missing IDs")
                return

            # Fetch the original signal message
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    # Try fetching the channel
                    channel = await self.bot.fetch_channel(int(channel_id))

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

            # Add the reaction
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
            # Don't fail the whole operation if reaction fails
            logger.error(f"Error adding reaction to original signal: {e}", exc_info=True)

    async def _check_stop_loss(self, signal: Dict, current_price: float, direction: str):
        """
        Check if stop loss is hit
        NOTE: Spread buffer is NOT applied to stop loss checks (must be exact)

        Args:
            signal: Signal dictionary
            current_price: Current market price (ask for long, bid for short)
            direction: 'long' or 'short'
        """
        stop_loss = signal['stop_loss']

        # Check if hit (NO SPREAD BUFFER - exact prices only)
        if direction == 'long':
            is_hit = current_price <= stop_loss
        else:
            is_hit = current_price >= stop_loss

        if is_hit:
            # Stop loss alerts never show spread
            await self.alert_system.send_stop_loss_alert(signal, current_price)
            # Add reaction to original signal message
            await self._react_to_original_signal(signal, "🛑")
            await self._process_stop_loss_hit(signal)
            self.stats['stop_losses_hit'] += 1

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
            result = await self.signal_db.process_limit_hit(
                limit['limit_id'],
                actual_price
            )

            if result.get('all_limits_hit'):
                logger.info(f"All limits hit for signal {signal['signal_id']}")

                # Remove from active signals and unsubscribe if no other signals need this symbol
                await self._maybe_unsubscribe_symbol(signal['instrument'], signal['signal_id'])

        except Exception as e:
            logger.error(f"Failed to process limit hit: {e}")

    async def _process_stop_loss_hit(self, signal: Dict):
        """Process stop loss hit"""
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal['signal_id'],
                'stop_loss'
            )

            if success:
                logger.info(f"Signal {signal['signal_id']} marked as stop loss")

                # Remove from active tracking
                await self._maybe_unsubscribe_symbol(signal['instrument'], signal['signal_id'])

        except Exception as e:
            logger.error(f"Failed to process stop loss: {e}")

    async def _maybe_unsubscribe_symbol(self, symbol: str, completed_signal_id: int):
        """Unsubscribe from symbol if no other active signals need it"""
        # Remove signal from tracking
        if symbol in self.symbol_to_signals:
            if completed_signal_id in self.symbol_to_signals[symbol]:
                self.symbol_to_signals[symbol].remove(completed_signal_id)

            # If no more signals for this symbol, unsubscribe
            if not self.symbol_to_signals[symbol]:
                await self.stream_manager.unsubscribe_symbol(symbol)
                del self.symbol_to_signals[symbol]
                logger.info(f"Unsubscribed from {symbol} (no active signals)")

        # Remove from active signals
        self.active_signals.pop(completed_signal_id, None)

    def get_stats(self) -> Dict:
        """Get monitoring statistics"""
        return {
            **self.stats,
            'running': self.running,
            'active_signals': len(self.active_signals),
            'monitored_symbols': len(self.symbol_to_signals),
            'spread_buffer_enabled': self._spread_buffer_enabled,
            'stream_manager': self.stream_manager.get_stats(),
            'alert_stats': self.alert_system.get_stats()
        }

    async def test_signal_monitoring(self, signal_id: int):
        """Test monitoring for a specific signal"""
        try:
            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return

            # Get latest price
            price = await self.stream_manager.get_latest_price(signal['instrument'])

            if price:
                # Manually trigger check
                await self._on_price_update(signal['instrument'], price)
                logger.info(f"Test check completed for signal {signal_id}")
            else:
                logger.error(f"No price data for {signal['instrument']}")

        except Exception as e:
            logger.error(f"Test monitoring failed: {e}", exc_info=True)