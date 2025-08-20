"""
Price Monitor - Updated with multi-feed support
Monitors active signals using FeedManager for intelligent feed routing
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import discord

from price_feeds.symbol_mapper import SymbolMapper
from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.smart_cache import SmartPriceCache, Priority
from price_feeds.feed_manager import FeedManager  # NEW: Use FeedManager instead of single feed
from price_feeds.alert_system import AlertSystem, AlertType
from utils.logger import get_logger

logger = get_logger('monitor')


class PriceMonitor:
    def __init__(self, bot, signal_db, db):
        """
        Initialize the price monitor with multi-feed support

        Args:
            bot: Discord bot instance for sending alerts
            signal_db: SignalDatabase instance
            db: DatabaseManager instance
        """
        self.bot = bot
        self.signal_db = signal_db
        self.db = db

        # Initialize components
        self.symbol_mapper = SymbolMapper()
        self.alert_config = AlertDistanceConfig()

        # CREATE SHARED CACHE INSTANCE
        self.cache = SmartPriceCache()

        # NEW: Use FeedManager instead of single ICMarkets feed
        self.feed_manager = FeedManager(cache_instance=self.cache)

        # Initialize alert system (pass bot for channel lookups)
        self.alert_system = AlertSystem(bot=bot)

        # Connect alert system to message handler if it exists
        if hasattr(bot, 'message_handler') and bot.message_handler:
            bot.message_handler.alert_system = self.alert_system
            logger.info("Connected alert system to message handler")

        # Monitoring state
        self.running = False
        self.monitoring_task = None
        self.last_check = {}  # Track last check time per signal

        # Performance tracking
        self.stats = {
            'checks_performed': 0,
            'limits_hit': 0,
            'errors': 0,
            'last_loop_time': 0
        }

    async def initialize(self):
        """Initialize monitor and connect to all feeds"""
        try:
            # NEW: Initialize all feeds through FeedManager
            await self.feed_manager.initialize()

            # Get feed status
            feed_status = self.feed_manager.get_feed_status()
            connected_feeds = [f for f, info in feed_status.items()
                             if info['status'] == 'connected']

            if not connected_feeds:
                logger.error("No feeds connected! Cannot start monitoring.")
                raise Exception("No price feeds available")

            logger.info(f"Connected to {len(connected_feeds)} price feeds: {', '.join(connected_feeds)}")

            # Setup alert channel
            from pathlib import Path
            import json

            config_path = Path(__file__).resolve().parent.parent / 'config' / 'channels.json'

            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    channel_id = config.get('alert_channel')
                    if channel_id:
                        try:
                            channel = await self.bot.fetch_channel(int(channel_id))
                            self.alert_system.set_channel(channel)
                            logger.info(f"Alert channel set: #{channel.name} ({channel.id})")
                        except discord.NotFound:
                            logger.error(f"Channel with ID {channel_id} not found")
                        except discord.Forbidden:
                            logger.error(f"Bot does not have permission to access channel {channel_id}")
                        except Exception as e:
                            logger.error(f"Error fetching channel {channel_id}: {e}")
                    else:
                        logger.error("No 'alert_channel' entry found in channels.json")
            except FileNotFoundError:
                logger.error(f"Channels config file not found: {config_path}")

        except Exception as e:
            logger.error(f"Failed to initialize monitor: {e}")
            raise

    async def start(self):
        """Start the monitoring loop"""
        if self.running:
            logger.warning("Monitor already running")
            return

        self.running = True
        logger.info("Monitor.running set to True")
        self.monitoring_task = asyncio.create_task(self.monitoring_loop())
        logger.info("Price monitoring started with multi-feed support")

    async def stop(self):
        """Stop the monitoring loop"""
        self.running = False
        if self.monitoring_task:
            await self.monitoring_task

        # Shutdown feed manager
        await self.feed_manager.shutdown()
        logger.info("Price monitoring stopped")

    async def monitoring_loop(self):
        """Enhanced monitoring loop with detailed metrics"""

        while self.running:
            loop_start = asyncio.get_event_loop().time()

            # Track phase timings
            timings = {}

            try:
                # Phase 1: Database query
                t1 = asyncio.get_event_loop().time()
                signals = await self.db.get_active_signals_for_tracking()
                timings['db_query'] = asyncio.get_event_loop().time() - t1

                if signals:
                    # Phase 2: Price fetching
                    t2 = asyncio.get_event_loop().time()
                    await self.process_signals(signals)
                    timings['processing'] = asyncio.get_event_loop().time() - t2

                # Log if slow
                total_time = asyncio.get_event_loop().time() - loop_start
                # logger.info(f"total monitoring loop time: {total_time}")
                if total_time > 0.5:  # Log if over 500ms
                    logger.warning(f"Slow loop: {total_time:.3f}s - Breakdown: {timings}")

            except Exception as e:
                logger.error(f"Monitoring error: {e}", exc_info=True)

            # Adaptive sleep time
            loop_time = asyncio.get_event_loop().time() - loop_start
            sleep_time = max(0, 1.0 - loop_time)
            await asyncio.sleep(sleep_time)

    async def process_signals(self, signals: List[Dict]):
        """Process all signals, checking limits and stop losses"""
        # Group signals by symbol and calculate priorities
        timings = {}

        # Phase 1: Grouping and priority
        t1 = asyncio.get_event_loop().time()
        symbol_priorities = {}
        signal_by_symbol = {}

        for signal in signals:
            symbol = signal['instrument']

            # Calculate priority based on closest pending limit
            priority = await self.calculate_signal_priority(signal)

            # Track highest priority per symbol
            if symbol not in symbol_priorities:
                symbol_priorities[symbol] = priority
            elif priority.value < symbol_priorities[symbol].value:
                symbol_priorities[symbol] = priority

            # Group signals by symbol
            if symbol not in signal_by_symbol:
                signal_by_symbol[symbol] = []
            signal_by_symbol[symbol].append((signal, priority))

        timings['grouping'] = asyncio.get_event_loop().time() - t1

        # Phase 2: Price fetching
        t2 = asyncio.get_event_loop().time()
        # Fetch prices using FeedManager (handles feed selection automatically)
        prices = await self.fetch_prices_batch(symbol_priorities)
        timings['price_fetch'] = asyncio.get_event_loop().time() - t2

        # Phase 3: Signal checking (now async)
        t3 = asyncio.get_event_loop().time()
        # Check each signal against current prices
        check_tasks = []

        for symbol, signal_list in signal_by_symbol.items():
            if symbol in prices:
                for signal, priority in signal_list:
                    task = asyncio.create_task(
                        self.check_signal(signal, prices[symbol])
                    )
                    check_tasks.append(task)

        if check_tasks:
            await asyncio.gather(*check_tasks, return_exceptions=True)

        timings['checking'] = asyncio.get_event_loop().time() - t3

        # Log if any phase is slow
        if any(t > 0.1 for t in timings.values()):
            logger.info(f"Processing breakdown: {timings}")

        return timings

    async def calculate_signal_priority(self, signal: Dict) -> Priority:
        """
        Calculate priority based on closest pending limit or stop loss
        """
        # If signal is already HIT status, it's critical
        if signal['status'] == 'hit':
            return Priority.CRITICAL

        symbol = signal['instrument']

        # Get current price estimate (use cached if available)
        cached_price = await self.cache.get_price(symbol, Priority.LOW)

        if not cached_price:
            # No cached price, assume medium priority for first fetch
            return Priority.MEDIUM

        current_price = cached_price['bid'] if signal['direction'] == 'short' else cached_price['ask']

        # Find closest distance (limits or stop loss)
        min_distance = float('inf')

        # Check pending limits
        for limit in signal.get('pending_limits', []):
            distance = abs(current_price - limit['price_level'])
            min_distance = min(min_distance, distance)

        # Check stop loss
        if signal.get('stop_loss'):
            sl_distance = abs(current_price - signal['stop_loss'])
            min_distance = min(min_distance, sl_distance)

        # Get proper pip size from alert config
        alert_config_for_symbol = self.alert_config.get_alert_config(symbol)
        pip_size = alert_config_for_symbol.get('pip_size', 0.0001)

        # Convert distance to pips
        distance_pips = min_distance / pip_size

        logger.debug(f"Priority calc for {symbol}: min_distance={min_distance:.5f}, "
                     f"pip_size={pip_size}, distance_pips={distance_pips:.1f}")

        # Determine asset class for priority calculation
        asset_class = self.symbol_mapper.determine_asset_class(symbol)

        # Special handling for JPY pairs
        if asset_class == 'forex' and 'JPY' in symbol.upper():
            asset_class = 'forex_jpy'

        # Determine priority based on distance
        return self.cache.calculate_priority(distance_pips, asset_class)

    async def fetch_prices_batch(self, symbol_priorities: Dict[str, Priority]) -> Dict[str, Dict]:
        """
        Fetch prices for multiple symbols efficiently using FeedManager

        Args:
            symbol_priorities: Dict mapping symbols to their priorities
        """
        if not symbol_priorities:
            return {}

        symbols = list(symbol_priorities.keys())

        try:
            # NEW: Use FeedManager to get prices (handles feed selection automatically)
            prices = await self.feed_manager.get_batch_prices(
                symbols,
                symbol_priorities
            )

            # Log feed usage statistics periodically
            if self.stats['checks_performed'] % 100 == 0:  # Every 100 checks
                feed_stats = self.feed_manager.get_stats()
                # logger.info(f"Feed usage stats: {feed_stats['feeds_used']}")

            return prices

        except Exception as e:
            logger.error(f"Error fetching batch prices: {e}", exc_info=True)
            return {}

    async def check_signal(self, signal: Dict, price_data: Dict):
        """
        Check a signal against current price and send alerts as needed
        """
        direction = signal['direction'].lower()

        # Determine which price to use based on direction
        current_price = price_data['ask'] if direction == 'long' else price_data['bid']

        # Add guild_id to signal for message link generation
        if hasattr(self.bot, 'guilds') and self.bot.guilds:
            signal['guild_id'] = self.bot.guilds[0].id

        # Check each pending limit
        for limit in signal.get('pending_limits', []):
            await self.check_limit(signal, limit, current_price, direction)

        # Check stop loss
        if signal.get('stop_loss'):
            await self.check_stop_loss(signal, current_price, direction)

    async def check_limit(self, signal: Dict, limit: Dict, current_price: float, direction: str):
        """Check if a limit is approaching or hit"""
        limit_price = limit['price_level']
        symbol = signal['instrument']

        # Calculate distance
        if direction == 'long':
            distance = current_price - limit_price
            is_hit = current_price <= limit_price
        else:
            distance = limit_price - current_price
            is_hit = current_price >= limit_price

        # Get proper configuration for this symbol
        alert_config_for_symbol = self.alert_config.get_alert_config(symbol)
        pip_size = alert_config_for_symbol.get('pip_size', 0.0001)

        # Convert to positive distance in pips
        distance_pips = abs(distance) / pip_size

        # Check if hit
        if is_hit and not limit['hit_alert_sent']:
            await self.alert_system.send_limit_hit_alert(signal, limit, current_price)
            await self.process_limit_hit(signal, limit, current_price)

        # Check if approaching (only for first limit and if not yet hit)
        elif not is_hit and not limit['approaching_alert_sent']:
            # Only check approaching for first limit
            if limit['sequence_number'] == 1:
                approaching_distance = self.alert_config.get_approaching_distance(symbol)

                if distance_pips <= approaching_distance:
                    await self.alert_system.send_approaching_alert(signal, limit, current_price, distance_pips)
                    await self.mark_approaching_sent(limit['limit_id'])

    async def check_stop_loss(self, signal: Dict, current_price: float, direction: str):
        """Check if stop loss is hit"""
        stop_loss = signal['stop_loss']

        # Check if stop loss hit
        if direction == 'long':
            is_hit = current_price <= stop_loss
        else:
            is_hit = current_price >= stop_loss

        if is_hit:
            await self.alert_system.send_stop_loss_alert(signal, current_price)
            await self.process_stop_loss_hit(signal)

    async def mark_approaching_sent(self, limit_id: int):
        """Mark that approaching alert has been sent for a limit"""
        try:
            query = "UPDATE limits SET approaching_alert_sent = 1 WHERE id = ?"
            async with self.db.get_connection() as conn:
                await conn.execute(query, (limit_id,))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark approaching sent: {e}")

    async def process_limit_hit(self, signal: Dict, limit: Dict, actual_price: float):
        """Process a limit hit in the database"""
        try:
            result = await self.signal_db.process_limit_hit(
                limit['limit_id'],
                actual_price
            )

            if result.get('all_limits_hit'):
                logger.info(f"All limits hit for signal {signal['signal_id']}")

            # Update local stats
            self.stats['limits_hit'] += 1

        except Exception as e:
            logger.error(f"Failed to process limit hit: {e}")

    async def process_stop_loss_hit(self, signal: Dict):
        """Process stop loss hit by updating signal status"""
        try:
            success = await self.signal_db.manually_set_signal_status(
                signal['signal_id'],
                'stop_loss'
            )

            if success:
                logger.info(f"Signal {signal['signal_id']} marked as stop loss")

        except Exception as e:
            logger.error(f"Failed to process stop loss: {e}")

    def get_stats(self) -> Dict:
        """Get monitoring statistics including feed manager stats"""
        return {
            **self.stats,
            'running': self.running,
            'feed_manager': self.feed_manager.get_stats() if self.feed_manager else {},
            'cache_stats': self.cache.get_stats() if self.cache else {},
            'alert_stats': self.alert_system.get_stats() if self.alert_system else {}
        }

    async def test_signal_monitoring(self, signal_id: int):
        """Test monitoring for a specific signal (for debugging)"""
        try:
            # Get signal from database
            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return

            # Format for monitoring
            formatted = {
                'signal_id': signal['id'],
                'instrument': signal['instrument'],
                'direction': signal['direction'],
                'stop_loss': signal['stop_loss'],
                'status': signal['status'],
                'limits_hit': len(signal['hit_limits']),
                'total_limits': signal['total_limits'],
                'pending_limits': [
                    {
                        'limit_id': l['id'],
                        'price_level': l['price'],
                        'sequence_number': l['sequence'],
                        'approaching_alert_sent': False,
                        'hit_alert_sent': False
                    }
                    for l in signal['pending_limits']
                ]
            }

            # Calculate priority and fetch price
            priority = await self.calculate_signal_priority(formatted)

            # NEW: Use FeedManager to get price
            price = await self.feed_manager.get_price(signal['instrument'], priority)

            if price:
                await self.check_signal(formatted, price)
                logger.info(f"Test check completed for signal {signal_id} using {price.get('feed', 'unknown')} feed")
            else:
                logger.error(f"Failed to get price for {signal['instrument']} from any feed")

        except Exception as e:
            logger.error(f"Test monitoring failed: {e}", exc_info=True)

    async def get_feed_health(self) -> Dict:
        """Get health status of all price feeds"""
        return self.feed_manager.get_feed_status()

    async def test_all_feeds(self) -> Dict:
        """Test all configured feeds"""
        return await self.feed_manager.test_all_feeds()