"""
Feed Manager - Orchestrates multiple price feeds with intelligent routing
Handles feed selection, fallback logic, and unified interface for monitor
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from enum import Enum

from price_feeds.symbol_mapper import SymbolMapper
from price_feeds.smart_cache import SmartPriceCache, Priority
from price_feeds.feeds.base import BaseFeed
from price_feeds.feeds.icmarkets import ICMarketsFeed
from price_feeds.feeds.oanda import OANDAFeed
from price_feeds.feeds.binance import BinanceFeed

logger = logging.getLogger(__name__)


class FeedStatus(Enum):
    """Feed availability status"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    NOT_CONFIGURED = "not_configured"


class FeedManager:
    """
    Manages multiple price feeds with intelligent routing and fallback

    Features:
    - Automatic feed selection based on symbol type
    - Fallback to secondary feeds on failure
    - Shared smart cache across all feeds
    - Unified interface for price monitoring
    - Health monitoring and reporting
    """

    def __init__(self, cache_instance: Optional[SmartPriceCache] = None):
        """
        Initialize Feed Manager with all available feeds

        Args:
            cache_instance: Optional shared cache instance
        """
        # Create or use shared cache
        if cache_instance:
            self.cache = cache_instance
            logger.info("Using provided cache instance")
        else:
            self.cache = SmartPriceCache()
            logger.info("Created new cache instance")

        # Initialize symbol mapper
        self.symbol_mapper = SymbolMapper()

        # Initialize feeds (lazy loading - connect when needed)
        self.feeds: Dict[str, BaseFeed] = {}
        self.feed_status: Dict[str, FeedStatus] = {}

        # Track failed feeds for temporary blacklisting
        self.failed_feeds: Dict[str, datetime] = {}
        self.blacklist_duration = 300  # 5 minutes

        # Performance tracking
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'fallback_used': 0,
            'feeds_used': {}
        }

        logger.info("Feed Manager initialized")

    async def initialize(self):
        """
        Initialize all configured feeds
        Connects to each feed and updates status
        """
        logger.info("Initializing all configured feeds...")

        # Initialize ICMarkets (always available)
        try:
            self.feeds['icmarkets'] = ICMarketsFeed(cache_instance=self.cache)
            if await self.feeds['icmarkets'].connect():
                self.feed_status['icmarkets'] = FeedStatus.CONNECTED
                logger.info("✓ ICMarkets feed connected")
            else:
                self.feed_status['icmarkets'] = FeedStatus.ERROR
                logger.warning("✗ ICMarkets feed failed to connect")
        except Exception as e:
            logger.error(f"Failed to initialize ICMarkets: {e}")
            self.feed_status['icmarkets'] = FeedStatus.ERROR

        # Initialize OANDA (if configured)
        try:
            import os
            if os.getenv('OANDA_API_KEY') and os.getenv('OANDA_ACCOUNT_ID'):
                self.feeds['oanda'] = OANDAFeed(cache_instance=self.cache)
                if await self.feeds['oanda'].connect():
                    self.feed_status['oanda'] = FeedStatus.CONNECTED
                    logger.info("✓ OANDA feed connected")
                else:
                    self.feed_status['oanda'] = FeedStatus.ERROR
                    logger.warning("✗ OANDA feed failed to connect")
            else:
                self.feed_status['oanda'] = FeedStatus.NOT_CONFIGURED
                logger.info("OANDA feed not configured (missing API credentials)")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA: {e}")
            self.feed_status['oanda'] = FeedStatus.ERROR

        # Initialize Binance (always available - no auth needed)
        try:
            import os
            # Check if we should use international API
            use_international = os.getenv('BINANCE_USE_INTERNATIONAL', 'false').lower() == 'true'

            self.feeds['binance'] = BinanceFeed(
                cache_instance=self.cache,
                use_international=use_international
            )

            if await self.feeds['binance'].connect():
                self.feed_status['binance'] = FeedStatus.CONNECTED
                logger.info("✓ Binance feed connected")
            else:
                self.feed_status['binance'] = FeedStatus.ERROR
                logger.warning("✗ Binance feed failed to connect")
                logger.info("Note: Binance may be blocked in your region. Crypto prices won't be available.")
        except Exception as e:
            logger.error(f"Failed to initialize Binance: {e}")
            self.feed_status['binance'] = FeedStatus.ERROR

        # Log summary
        connected = [f for f, s in self.feed_status.items() if s == FeedStatus.CONNECTED]
        logger.info(f"Feed initialization complete: {len(connected)}/{len(self.feed_status)} feeds connected")
        logger.info(f"Connected feeds: {', '.join(connected)}")

    async def shutdown(self):
        """Disconnect all feeds gracefully"""
        logger.info("Shutting down all feeds...")

        for feed_name, feed in self.feeds.items():
            try:
                await feed.disconnect()
                logger.info(f"Disconnected {feed_name}")
            except Exception as e:
                logger.error(f"Error disconnecting {feed_name}: {e}")

        self.feeds.clear()
        self.feed_status.clear()
        logger.info("All feeds disconnected")

    def _is_feed_available(self, feed_name: str) -> bool:
        """
        Check if a feed is available for use

        Args:
            feed_name: Name of the feed

        Returns:
            True if feed is connected and not blacklisted
        """
        # Check if feed exists and is connected
        if feed_name not in self.feeds:
            return False

        if self.feed_status.get(feed_name) != FeedStatus.CONNECTED:
            return False

        # Check if temporarily blacklisted
        if feed_name in self.failed_feeds:
            blacklist_time = self.failed_feeds[feed_name]
            if (datetime.now() - blacklist_time).total_seconds() < self.blacklist_duration:
                return False
            else:
                # Remove from blacklist
                del self.failed_feeds[feed_name]

        return True

    def _get_available_feeds_for_symbol(self, symbol: str) -> List[str]:
        """
        Get list of available feeds for a symbol in priority order

        Args:
            symbol: Trading symbol

        Returns:
            List of feed names in priority order
        """
        # Get asset class and feed priority from symbol mapper
        asset_class = self.symbol_mapper.determine_asset_class(symbol)

        # Get feed priority from config
        feed_priority = {
            'forex': ['icmarkets', 'oanda'],
            'forex_jpy': ['icmarkets', 'oanda'],
            'indices': ['oanda', 'icmarkets'],
            'crypto': ['binance'],
            'metals': ['icmarkets'],  # FXCM redirects to ICMarkets
            'stocks': ['icmarkets'],
            'oil': []  # Not supported
        }

        priority_list = feed_priority.get(asset_class, ['icmarkets'])

        # Filter for available feeds
        available = [f for f in priority_list if self._is_feed_available(f)]

        return available

    async def get_price(self, symbol: str, priority: Priority = Priority.MEDIUM) -> Optional[Dict]:
        """
        Get price for a single symbol with automatic feed selection

        Args:
            symbol: Internal format symbol
            priority: Cache priority

        Returns:
            Price dict with bid, ask, timestamp, feed
        """
        self.stats['total_requests'] += 1

        # Get available feeds for this symbol
        available_feeds = self._get_available_feeds_for_symbol(symbol)

        if not available_feeds:
            logger.error(f"No available feeds for {symbol}")
            self.stats['failed_requests'] += 1
            return None

        # Try each feed in priority order
        for feed_name in available_feeds:
            try:
                # Convert symbol to feed format
                feed_symbol = self.symbol_mapper.get_feed_symbol(symbol, feed_name)
                if not feed_symbol:
                    logger.warning(f"Cannot map {symbol} to {feed_name} format")
                    continue

                # Get price from feed
                feed = self.feeds[feed_name]
                price = await feed.get_price(feed_symbol, priority)

                if price:
                    # Add feed info to result
                    price['feed'] = feed_name
                    price['original_symbol'] = symbol

                    # Update stats
                    self.stats['successful_requests'] += 1
                    self.stats['feeds_used'][feed_name] = self.stats['feeds_used'].get(feed_name, 0) + 1

                    # If not primary feed, count as fallback
                    if feed_name != available_feeds[0]:
                        self.stats['fallback_used'] += 1
                        logger.info(f"Used fallback feed {feed_name} for {symbol}")

                    return price

            except Exception as e:
                logger.error(f"Error getting price from {feed_name} for {symbol}: {e}")
                # Temporarily blacklist this feed
                self.failed_feeds[feed_name] = datetime.now()
                continue

        # All feeds failed
        logger.error(f"All feeds failed for {symbol}")
        self.stats['failed_requests'] += 1
        return None

    async def get_batch_prices(
        self,
        symbols: List[str],
        priorities: Optional[Dict[str, Priority]] = None
    ) -> Dict[str, Dict]:
        """
        Get prices for multiple symbols efficiently
        Groups symbols by best feed and fetches in batches

        Args:
            symbols: List of internal format symbols
            priorities: Optional priority mapping

        Returns:
            Dict of symbol -> price data
        """
        if not symbols:
            return {}

        results = {}

        # Group symbols by best available feed
        feed_groups: Dict[str, List[Tuple[str, str]]] = {}  # feed -> [(internal, feed_symbol)]

        for symbol in symbols:
            available_feeds = self._get_available_feeds_for_symbol(symbol)

            if not available_feeds:
                logger.warning(f"No available feeds for {symbol}")
                continue

            # Use primary feed
            feed_name = available_feeds[0]
            feed_symbol = self.symbol_mapper.get_feed_symbol(symbol, feed_name)

            if not feed_symbol:
                logger.warning(f"Cannot map {symbol} to {feed_name} format")
                continue

            if feed_name not in feed_groups:
                feed_groups[feed_name] = []
            feed_groups[feed_name].append((symbol, feed_symbol))

        # Fetch from each feed
        for feed_name, symbol_pairs in feed_groups.items():
            try:
                feed = self.feeds[feed_name]
                feed_symbols = [fs for _, fs in symbol_pairs]

                # Create priorities dict for feed symbols
                feed_priorities = None
                if priorities:
                    feed_priorities = {}
                    for internal, feed_sym in symbol_pairs:
                        if internal in priorities:
                            feed_priorities[feed_sym] = priorities[internal]

                # Fetch batch from feed
                try:
                    logger.info(f"Fetching {len(feed_symbols)} symbols from {feed_name}")
                    feed_results = await asyncio.wait_for(
                        feed.get_batch_prices(feed_symbols, feed_priorities),
                        timeout=3.0  # 3 seconds max per feed
                    )

                    # Map results back to internal symbols
                    for internal_symbol, feed_symbol in symbol_pairs:
                        if feed_symbol in feed_results:
                            price_data = feed_results[feed_symbol].copy()
                            price_data['feed'] = feed_name
                            price_data['original_symbol'] = internal_symbol
                            results[internal_symbol] = price_data

                            self.stats['feeds_used'][feed_name] = self.stats['feeds_used'].get(feed_name, 0) + 1
                except asyncio.TimeoutError:
                    logger.error(f"Timeout fetching from {feed_name} after 3 seconds")

            except Exception as e:
                logger.error(f"Error fetching batch from {feed_name}: {e}")
                # Temporarily blacklist
                self.failed_feeds[feed_name] = datetime.now()

                # Try fallback for failed symbols
                for internal_symbol, _ in symbol_pairs:
                    if internal_symbol not in results:
                        # Try to get individually with fallback
                        price = await self.get_price(
                            internal_symbol,
                            priorities.get(internal_symbol, Priority.MEDIUM) if priorities else Priority.MEDIUM
                        )
                        if price:
                            results[internal_symbol] = price

        # Update stats
        self.stats['total_requests'] += len(symbols)
        self.stats['successful_requests'] += len(results)
        self.stats['failed_requests'] += len(symbols) - len(results)

        return results

    def get_feed_status(self) -> Dict[str, Dict]:
        """
        Get status of all feeds

        Returns:
            Dict with feed statuses and health metrics
        """
        status = {}

        for feed_name in ['icmarkets', 'oanda', 'binance']:
            feed_info = {
                'status': self.feed_status.get(feed_name, FeedStatus.NOT_CONFIGURED).value,
                'blacklisted': feed_name in self.failed_feeds
            }

            # Add health metrics if connected
            if feed_name in self.feeds:
                feed_info['health'] = self.feeds[feed_name].get_health_status()

            status[feed_name] = feed_info

        return status

    def get_stats(self) -> Dict:
        """Get feed manager statistics"""
        success_rate = 0
        if self.stats['total_requests'] > 0:
            success_rate = (self.stats['successful_requests'] /
                          self.stats['total_requests']) * 100

        return {
            **self.stats,
            'success_rate': f"{success_rate:.1f}%",
            'active_feeds': len([f for f, s in self.feed_status.items()
                                if s == FeedStatus.CONNECTED]),
            'blacklisted_feeds': list(self.failed_feeds.keys()),
            'cache_stats': self.cache.get_stats() if self.cache else {}
        }

    async def test_all_feeds(self) -> Dict[str, Tuple[bool, str]]:
        """
        Test all configured feeds

        Returns:
            Dict of feed_name -> (success, message)
        """
        results = {}

        for feed_name, feed in self.feeds.items():
            if self.feed_status.get(feed_name) == FeedStatus.NOT_CONFIGURED:
                results[feed_name] = (False, "Not configured")
                continue

            success, message = await feed.test_connection()
            results[feed_name] = (success, message)

        return results