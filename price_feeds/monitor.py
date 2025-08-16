"""
Price Monitor - Main monitoring loop for ICMarkets
Monitors active signals, checks limits, and sends Discord alerts
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import discord
from enum import Enum

from price_feeds.symbol_mapper import SymbolMapper
from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.smart_cache import SmartPriceCache, Priority
from price_feeds.feeds.icmarkets import ICMarketsFeed
from utils.logger import get_logger

logger = get_logger('monitor')


class AlertType(Enum):
    """Types of alerts"""
    APPROACHING = "approaching"
    HIT = "hit"
    STOP_LOSS = "stop_loss"


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
        self.feed = ICMarketsFeed(cache_instance=self.cache)  # Changed parameter name

        # Monitoring state
        self.running = False
        self.monitoring_task = None
        self.last_check = {}  # Track last check time per signal

        # Performance tracking
        self.stats = {
            'checks_performed': 0,
            'alerts_sent': 0,
            'limits_hit': 0,
            'errors': 0,
            'last_loop_time': 0
        }

        # Get alert channel from config
        self.alert_channel = None

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
                            self.alert_channel = await self.bot.fetch_channel(int(channel_id))
                            logger.info(f"Alert channel set: #{self.alert_channel.name} ({self.alert_channel.id})")
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
        logger.info("Monitor.running set to True")  # Add this
        self.monitoring_task = asyncio.create_task(self.monitoring_loop())
        logger.info("Price monitoring started")
        logger.info("Monitoring task created")  # Add this

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
                logger.info(f"Found {len(signals)} signals to monitor")  # Add this
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

        # Debug: Log all instruments
        instruments = [s['instrument'] for s in signals]
        logger.info(f"Signal instruments: {instruments}")

        # Group signals by symbol and calculate priorities
        symbol_priorities = {}
        signal_by_symbol = {}

        for signal in signals:
            symbol = signal['instrument']

            # Calculate priority based on closest pending limit
            priority = await self.calculate_signal_priority(signal)
            logger.info(f"Calculated priority for {symbol}: {priority.name}")  # <-- Add this

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
                logger.error(f"No price data for {symbol}!")  # <-- Add this

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

        # Get current price estimate (use cached if available)
        symbol = signal['instrument']
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

        # Convert distance to pips for priority calculation
        asset_class = self.symbol_mapper.determine_asset_class(symbol)
        pip_size = self.alert_config.config['defaults'][asset_class]['pip_size']
        distance_pips = min_distance / pip_size

        # Determine priority based on distance
        return self.cache.calculate_priority(distance_pips, asset_class)

    # In monitor.py::fetch_prices_batch()
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
            # IMPORTANT: Pass symbols as list, priorities as dict
            prices = await self.feed.get_batch_prices(
                symbols,  # List of symbols
                symbol_priorities  # Dict of priorities
            )

            # LOG WHAT WE GOT BACK
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
        ENHANCED WITH DEBUGGING
        """
        limit_price = limit['price_level']

        # Log the actual comparison being made
        logger.info(f"    Limit check for Signal #{signal['signal_id']}:")
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

        # Convert to positive distance in pips
        symbol = signal['instrument']
        asset_class = self.symbol_mapper.determine_asset_class(symbol)
        pip_size = self.alert_config.config['defaults'][asset_class]['pip_size']
        distance_pips = abs(distance) / pip_size

        logger.info(f"      Distance in pips: {distance_pips:.1f}")
        logger.info(f"      Approaching alert sent: {limit['approaching_alert_sent']}")
        logger.info(f"      Hit alert sent: {limit['hit_alert_sent']}")

        if limit['hit_alert_sent']:
            print('hit_alert_sent')

        # Check if hit
        if is_hit and not limit['hit_alert_sent']:
            await self.send_limit_hit_alert(signal, limit, current_price)
            await self.process_limit_hit(signal, limit, current_price)

        # Check if approaching (only if not yet hit)
        elif not is_hit and not limit['approaching_alert_sent']:
            approaching_distance = self.alert_config.get_approaching_distance(symbol)
            logger.debug(
                f"Signal {signal['signal_id']}: Distance={distance_pips:.1f} pips, Approaching threshold={approaching_distance} pips")

            if distance_pips <= approaching_distance:
                await self.send_approaching_alert(signal, limit, current_price, distance_pips)
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
            await self.send_stop_loss_alert(signal, current_price)
            await self.process_stop_loss_hit(signal)

    async def send_approaching_alert(self, signal: Dict, limit: Dict, current_price: float, distance_pips: float):
        """Send alert for approaching limit"""
        if not self.alert_channel:
            return

        try:
            embed = discord.Embed(
                title="🟡 Limit Approaching",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0xFFA500,  # Orange
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="Limit Details",
                value=f"Limit #{limit['sequence_number']}: {limit['price_level']:.5f}",
                inline=False
            )
            embed.add_field(
                name="Current Price",
                value=f"{current_price:.5f}",
                inline=True
            )
            embed.add_field(
                name="Distance",
                value=f"{distance_pips:.1f} pips",
                inline=True
            )

            embed.set_footer(text=f"Signal #{signal['signal_id']}")

            await self.alert_channel.send(embed=embed)
            self.stats['alerts_sent'] += 1
            logger.info(f"Approaching alert sent for signal {signal['signal_id']}, limit {limit['limit_id']}")

        except Exception as e:
            logger.error(f"Failed to send approaching alert: {e}")

    async def send_limit_hit_alert(self, signal: Dict, limit: Dict, current_price: float):
        """Send alert for limit hit"""
        if not self.alert_channel:
            return

        try:
            embed = discord.Embed(
                title="🎯 Limit Hit!",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0x00FF00,  # Green
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="Limit Hit",
                value=f"Limit #{limit['sequence_number']}: {limit['price_level']:.5f}",
                inline=False
            )
            embed.add_field(
                name="Hit Price",
                value=f"{current_price:.5f}",
                inline=True
            )
            embed.add_field(
                name="Progress",
                value=f"{signal['limits_hit'] + 1}/{signal['total_limits']} limits hit",
                inline=True
            )

            embed.set_footer(text=f"Signal #{signal['signal_id']}")

            await self.alert_channel.send(embed=embed)
            self.stats['alerts_sent'] += 1
            self.stats['limits_hit'] += 1
            logger.info(f"Limit hit alert sent for signal {signal['signal_id']}, limit {limit['limit_id']}")

        except Exception as e:
            logger.error(f"Failed to send limit hit alert: {e}")

    async def send_stop_loss_alert(self, signal: Dict, current_price: float):
        """Send alert for stop loss hit"""
        if not self.alert_channel:
            return

        try:
            embed = discord.Embed(
                title="🛑 Stop Loss Hit!",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0xFF0000,  # Red
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="Stop Loss",
                value=f"{signal['stop_loss']:.5f}",
                inline=True
            )
            embed.add_field(
                name="Hit Price",
                value=f"{current_price:.5f}",
                inline=True
            )

            embed.set_footer(text=f"Signal #{signal['signal_id']}")

            await self.alert_channel.send(embed=embed)
            self.stats['alerts_sent'] += 1
            logger.info(f"Stop loss alert sent for signal {signal['signal_id']}")

        except Exception as e:
            logger.error(f"Failed to send stop loss alert: {e}")

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
            'cache_stats': self.cache.get_stats() if self.cache else {}
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