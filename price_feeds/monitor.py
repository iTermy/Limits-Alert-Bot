"""
Price Monitor - Main monitoring loop for ICMarkets
Monitors active signals, checks limits, and sends Discord alerts
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import discord

from price_feeds.symbol_mapper import SymbolMapper
from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.smart_cache import SmartPriceCache, Priority
from price_feeds.feeds.icmarkets import ICMarketsFeed
from price_feeds.alert_system import AlertSystem, AlertType
from utils.logger import get_logger

logger = get_logger('monitor')


class PriceMonitor:
    """
    Main monitoring loop for price tracking and alerts

    Monitors all ACTIVE and HIT signals, checks distances to limits,
    sends alerts when approaching or hitting limits/stop losses
    """

    def __init__(self, bot, signal_db, db):
        """
        Initialize the price monitor

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

        # PASS SHARED CACHE TO FEED
        self.feed = ICMarketsFeed(cache_instance=self.cache)

        # Initialize alert system (pass bot for channel lookups)
        self.alert_system = AlertSystem(bot=bot)

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
        """Initialize monitor and connect to feed"""
        try:
            # Connect to ICMarkets
            await self.feed.connect()
            logger.info("Connected to ICMarkets feed")

            from pathlib import Path
            import json

            # Get alert channel
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
                            logger.error(f"Channel with ID {channel_id} not found (bot may not have access).")
                        except discord.Forbidden:
                            logger.error(f"Bot does not have permission to access channel {channel_id}.")
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
        logger.info("Price monitoring started")

    async def stop(self):
        """Stop the monitoring loop"""
        self.running = False
        if self.monitoring_task:
            await self.monitoring_task
        logger.info("Price monitoring stopped")

    async def monitoring_loop(self):
        """
        Main monitoring loop - runs every second

        1. Get active signals
        2. Calculate priorities based on closest limits
        3. Fetch prices (using cache where appropriate)
        4. Check limits and stop losses
        5. Send alerts as needed
        """
        logger.info("Monitoring loop started!")
        while self.running:
            loop_start = asyncio.get_event_loop().time()

            try:
                # Get signals that need tracking
                signals = await self.db.get_active_signals_for_tracking()
                logger.info(f"Found {len(signals)} signals to monitor")
                if signals:
                    logger.debug(f"First signal: {signals[0]['instrument']}")

                if signals:
                    # Process signals by priority
                    await self.process_signals(signals)

                self.stats['checks_performed'] += 1

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                self.stats['errors'] += 1

            # Calculate sleep time to maintain 1-second intervals
            loop_time = asyncio.get_event_loop().time() - loop_start
            self.stats['last_loop_time'] = loop_time
            sleep_time = max(0, 1.0 - loop_time)

            if loop_time > 1.0:
                logger.warning(f"Monitoring loop took {loop_time:.2f}s")

            await asyncio.sleep(sleep_time)

    async def process_signals(self, signals: List[Dict]):
        """Process all signals, checking limits and stop losses"""
        logger.info(f"Processing {len(signals)} signals")

        # Group signals by symbol and calculate priorities
        symbol_priorities = {}
        signal_by_symbol = {}

        for signal in signals:
            symbol = signal['instrument']

            # Calculate priority based on closest pending limit
            priority = await self.calculate_signal_priority(signal)
            logger.info(f"Calculated priority for {symbol}: {priority.name}")

            # Track highest priority per symbol
            if symbol not in symbol_priorities:
                symbol_priorities[symbol] = priority
                logger.info(f"Initial priority for {symbol}: {priority.name}")
            elif priority.value < symbol_priorities[symbol].value:
                old_priority = symbol_priorities[symbol]
                symbol_priorities[symbol] = priority
                logger.info(f"Updated priority for {symbol}: {old_priority.name} -> {priority.name}")

            # Group signals by symbol
            if symbol not in signal_by_symbol:
                signal_by_symbol[symbol] = []
            signal_by_symbol[symbol].append((signal, priority))

        # Log final symbol_priorities
        logger.info(f"Final symbol_priorities dict: {[(k, v.name) for k, v in symbol_priorities.items()]}")

        # Fetch prices for all needed symbols
        logger.info(f"Symbol priorities to fetch: {list(symbol_priorities.keys())}")
        prices = await self.fetch_prices_batch(symbol_priorities)
        logger.info(f"Received prices for symbols: {list(prices.keys())}")

        # Check each signal against current prices
        for symbol, signal_list in signal_by_symbol.items():
            if symbol in prices:
                for signal, priority in signal_list:
                    # Create a fresh copy for each signal
                    price_data = dict(prices[symbol])
                    await self.check_signal(signal, price_data)
            else:
                logger.error(f"No price data for {symbol}!")

    async def calculate_signal_priority(self, signal: Dict) -> Priority:
        """
        Calculate priority based on closest pending limit or stop loss

        Args:
            signal: Signal dictionary with pending limits

        Returns:
            Priority enum value
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
        Fetch prices for multiple symbols efficiently

        Args:
            symbol_priorities: Dict mapping symbols to their priorities
        """
        if not symbol_priorities:
            return {}

        symbols = list(symbol_priorities.keys())
        logger.info(f"fetch_prices_batch called for symbols: {symbols}")

        try:
            # Get prices from feed with priorities
            prices = await self.feed.get_batch_prices(
                symbols,  # List of symbols
                symbol_priorities  # Dict of priorities
            )

            # Log what we got back
            logger.info(f"Received prices from feed:")
            for symbol, price_data in prices.items():
                if price_data:
                    logger.info(f"  {symbol}: Bid={price_data.get('bid', 'N/A')}, Ask={price_data.get('ask', 'N/A')}")

            # Verify we got all symbols
            missing_symbols = set(symbols) - set(prices.keys())
            if missing_symbols:
                logger.error(f"Missing prices for symbols: {missing_symbols}")

            return prices

        except Exception as e:
            logger.error(f"Error fetching batch prices: {e}", exc_info=True)
            return {}

    async def check_signal(self, signal: Dict, price_data: Dict):
        """
        Check a signal against current price and send alerts as needed

        Args:
            signal: Signal dictionary
            price_data: Current price data (bid, ask, timestamp)
        """
        direction = signal['direction'].lower()

        # Determine which price to use based on direction
        # LONG: watching for price to DROP to limit (use ASK)
        # SHORT: watching for price to RISE to limit (use BID)
        current_price = price_data['ask'] if direction == 'long' else price_data['bid']

        logger.info(f"Signal #{signal['signal_id']} ({signal['instrument']} {direction}): "
                    f"Current price={current_price:.5f}, "
                    f"Pending limits={len(signal.get('pending_limits', []))}")


        # Add guild_id to signal for message link generation
        if hasattr(self.bot, 'guilds') and self.bot.guilds:
            signal['guild_id'] = self.bot.guilds[0].id

        # Check each pending limit
        for limit in signal.get('pending_limits', []):
            logger.info(f"  Checking limit #{limit['sequence_number']}: {limit['price_level']:.5f}")
            await self.check_limit(signal, limit, current_price, direction)

        # Check stop loss
        if signal.get('stop_loss'):
            await self.check_stop_loss(signal, current_price, direction)

    async def check_limit(self, signal: Dict, limit: Dict, current_price: float, direction: str):
        """
        Check if a limit is approaching or hit
        """
        limit_price = limit['price_level']
        symbol = signal['instrument']

        logger.info(f"    Limit check for Signal #{signal['signal_id']}:")
        logger.info(f"      Symbol: {symbol}")
        logger.info(f"      Direction: {direction}")
        logger.info(f"      Current price: {current_price:.5f}")
        logger.info(f"      Limit price: {limit_price:.5f}")

        # Calculate distance
        if direction == 'long':
            # Long: limit is below, watching price drop
            distance = current_price - limit_price
            is_hit = current_price <= limit_price
            logger.info(f"      LONG: distance={distance:.5f}, is_hit={is_hit}")
        else:
            # Short: limit is above, watching price rise
            distance = limit_price - current_price
            is_hit = current_price >= limit_price
            logger.info(f"      SHORT: distance={distance:.5f}, is_hit={is_hit}")

        # Get proper configuration for this symbol
        alert_config_for_symbol = self.alert_config.get_alert_config(symbol)
        pip_size = alert_config_for_symbol.get('pip_size', 0.0001)

        # Convert to positive distance in pips
        distance_pips = abs(distance) / pip_size

        logger.info(f"      Alert config: {alert_config_for_symbol}")
        logger.info(f"      Pip size: {pip_size}")
        logger.info(f"      Distance in pips: {distance_pips:.1f}")
        logger.info(f"      Limit sequence: #{limit['sequence_number']}")
        logger.info(f"      Approaching alert sent: {limit['approaching_alert_sent']}")
        logger.info(f"      Hit alert sent: {limit['hit_alert_sent']}")

        # Check if hit
        if is_hit and not limit['hit_alert_sent']:
            await self.alert_system.send_limit_hit_alert(signal, limit, current_price)
            await self.process_limit_hit(signal, limit, current_price)

        # Check if approaching (only for first limit and if not yet hit)
        elif not is_hit and not limit['approaching_alert_sent']:
            # Only check approaching for first limit
            if limit['sequence_number'] == 1:
                approaching_distance = self.alert_config.get_approaching_distance(symbol)
                logger.info(f"      First limit approaching check: Distance={distance_pips:.1f} pips, Threshold={approaching_distance} pips")

                if distance_pips <= approaching_distance:
                    await self.alert_system.send_approaching_alert(signal, limit, current_price, distance_pips)
                    await self.mark_approaching_sent(limit['limit_id'])

    async def check_stop_loss(self, signal: Dict, current_price: float, direction: str):
        """
        Check if stop loss is hit

        Args:
            signal: Signal dictionary
            current_price: Current price
            direction: Trade direction
        """
        stop_loss = signal['stop_loss']

        # Check if stop loss hit
        if direction == 'long':
            # Long: stop loss is below entry, hit if price drops to it
            is_hit = current_price <= stop_loss
        else:
            # Short: stop loss is above entry, hit if price rises to it
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
        """Get monitoring statistics"""
        return {
            **self.stats,
            'running': self.running,
            'cache_stats': self.cache.get_stats() if self.cache else {},
            'alert_stats': self.alert_system.get_stats() if self.alert_system else {}
        }

    async def test_signal_monitoring(self, signal_id: int):
        """
        Test monitoring for a specific signal (for debugging)

        Args:
            signal_id: Signal ID to test
        """
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
            price = await self.feed.get_price(signal['instrument'], priority)

            if price:
                await self.check_signal(formatted, price)
                logger.info(f"Test check completed for signal {signal_id}")
            else:
                logger.error(f"Failed to get price for {signal['instrument']}")

        except Exception as e:
            logger.error(f"Test monitoring failed: {e}", exc_info=True)