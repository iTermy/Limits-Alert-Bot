"""
ICMarkets/MT5 Price Feed - Optimized with Smart Cache
Phase 2C implementation with intelligent caching layer
"""

import asyncio
import logging
from typing import Dict, Optional, List
from datetime import datetime
import MetaTrader5 as mt5

# Import base class and smart cache
from price_feeds.feeds.base import BaseFeed
from price_feeds.smart_cache import SmartPriceCache, Priority

logger = logging.getLogger(__name__)


class ICMarketsFeed(BaseFeed):
    """
    Optimized MetaTrader 5 price feed for ICMarkets with Smart Caching

    Phase 2C Features:
    - Smart cache with priority-based TTL
    - 90% reduction in API calls for low-priority signals
    - Efficient batch price fetching
    - Symbol validation and caching
    - Enhanced error recovery
    """

    def __init__(self, cache_instance: Optional[SmartPriceCache] = None, cache_config: Optional[Dict] = None):
        """
        Initialize MT5 feed with smart cache

        Args:
            cache_instance: Existing cache instance to share (for monitor integration)
            cache_config: Optional cache TTL configuration (only used if creating new cache)
        """
        super().__init__("ICMarkets")

        # CRITICAL FIX: Use shared cache if provided, otherwise create new one
        if cache_instance is not None:
            self.cache = cache_instance
            logger.info("Using shared cache instance from monitor")
        else:
            self.cache = SmartPriceCache(custom_ttl=cache_config)
            logger.info("Created new cache instance")

        # Symbol validation cache
        self.valid_symbols = set()
        self.invalid_symbols = set()
        self.symbol_suffix_map = {}  # Cache successful suffix mappings

        # Common suffixes for ICMarkets
        self.suffixes = ['', '.a', '_m', '.r']

        # Batch operation settings
        self.max_batch_size = 50  # MT5 can handle large batches

        # Cache statistics tracking
        self.cache_enabled = True
        self.cache_bypass_count = 0

    async def connect(self) -> bool:
        """
        Initialize MT5 connection
        ENHANCED: Clear all symbol caches on connect
        """
        try:
            # CLEAR ALL CACHES on connect to avoid stale mappings
            self.valid_symbols.clear()
            self.invalid_symbols.clear()
            self.symbol_suffix_map.clear()
            logger.info("Cleared all symbol caches")

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, mt5.initialize)

            if result:
                self.connected = True
                self.consecutive_failures = 0
                self.connection_attempts += 1

                # Get terminal info
                terminal_info = mt5.terminal_info()
                if terminal_info:
                    logger.info(f"Connected to MT5 - Terminal: {terminal_info.name}, "
                                f"Build: {terminal_info.build}, "
                                f"Company: {terminal_info.company}")

                    # DON'T cache symbols - let them be found dynamically
                    # This avoids any caching issues
                    logger.info("Symbol caching disabled to avoid confusion")
                else:
                    logger.info("Connected to MT5")

                return True
            else:
                error = mt5.last_error()
                logger.error(f"MT5 initialization failed: {error}")
                self.connected = False
                return False

        except Exception as e:
            logger.error(f"Error connecting to MT5: {e}", exc_info=True)
            self.connected = False
            return False

    async def disconnect(self):
        """Shutdown MT5 connection"""
        try:
            if self.connected:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, mt5.shutdown)
                self.connected = False
                logger.info("Disconnected from MT5")

                # Log cache statistics on disconnect
                cache_stats = self.cache.get_stats()
                logger.info(f"Cache stats at disconnect: {cache_stats}")
        except Exception as e:
            logger.error(f"Error disconnecting from MT5: {e}")

    async def ensure_connected(self) -> bool:
        """
        Ensure MT5 is connected, reconnect if necessary

        Returns:
            True if connected or reconnected successfully
        """
        if not self.connected:
            logger.info("MT5 not connected, attempting to connect...")
            return await self.connect()

        # Quick connection check
        try:
            loop = asyncio.get_event_loop()
            terminal_info = await loop.run_in_executor(None, mt5.terminal_info)

            if terminal_info is None:
                logger.warning("MT5 connection lost, reconnecting...")
                self.connected = False
                # Clear cache on reconnection as prices may be stale
                await self.cache.invalidate()
                return await self.connect()

            return True

        except Exception as e:
            logger.error(f"Error checking MT5 connection: {e}")
            self.connected = False
            return await self.connect()

    async def _cache_available_symbols(self):
        """Cache list of available symbols for faster validation"""
        try:
            loop = asyncio.get_event_loop()
            symbols = await loop.run_in_executor(None, mt5.symbols_get)

            if symbols:
                self.valid_symbols = {s.name for s in symbols}
                logger.info(f"Cached {len(self.valid_symbols)} available symbols")
        except Exception as e:
            logger.error(f"Error caching symbols: {e}")

    def _find_symbol_with_suffix(self, base_symbol: str) -> Optional[str]:
        """
        Find symbol with appropriate suffix

        Args:
            base_symbol: Symbol without suffix

        Returns:
            Full symbol name with suffix, or None
        """
        # Check cache first
        if base_symbol in self.symbol_suffix_map:
            return self.symbol_suffix_map[base_symbol]

        # Try each suffix
        for suffix in self.suffixes:
            test_symbol = f"{base_symbol}{suffix}"
            if test_symbol in self.valid_symbols:
                self.symbol_suffix_map[base_symbol] = test_symbol
                return test_symbol

        return None

    async def get_price(self, symbol: str, priority: Priority = Priority.MEDIUM) -> Optional[Dict]:
        """
        Get current bid/ask price for a single symbol with caching
        ENHANCED WITH DEBUGGING
        """
        logger.info(f"get_price called for {symbol} with priority {priority.name}")

        # Check cache first if enabled
        if self.cache_enabled:
            cached_price = await self.cache.get_price(symbol, priority)
            if cached_price:
                logger.info(f"Cache hit for {symbol}: Bid={cached_price.get('bid')}, Ask={cached_price.get('ask')}")
                # Ensure we have both bid and ask
                if 'bid' in cached_price and 'ask' in cached_price:
                    if isinstance(cached_price.get('timestamp'), (int, float)):
                        cached_price['timestamp'] = datetime.fromtimestamp(cached_price['timestamp'])
                    return cached_price
                else:
                    logger.warning(f"Cached price missing bid or ask: {cached_price}")

        # Fetch from MT5
        logger.info(f"Cache miss or disabled, fetching fresh price for {symbol}")
        fresh_price = await self._fetch_single_price(symbol)

        # Update cache if successful
        if fresh_price and self.cache_enabled:
            logger.info(f"Fresh price from MT5: Bid={fresh_price['bid']}, Ask={fresh_price['ask']}")

            await self.cache.update_price(
                symbol=symbol,
                bid=fresh_price['bid'],
                ask=fresh_price['ask'],
                timestamp=fresh_price['timestamp'].timestamp(),
                priority=priority
            )

            # Verify what was cached
            test_cached = await self.cache.get_price(symbol, Priority.CRITICAL)  # Use CRITICAL to ensure fresh read
            if test_cached:
                logger.info(f"Verified cached: Bid={test_cached.get('bid')}, Ask={test_cached.get('ask')}")

        return fresh_price

    async def _fetch_single_price(self, symbol: str) -> Optional[Dict]:
        """
        Internal method to fetch price from MT5 without cache
        SIMPLIFIED: No suffix mapping for ICMarkets
        """
        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch price - MT5 not connected")
                return None

            logger.info(f"Fetching MT5 price for: '{symbol}'")

            # Fetch tick data DIRECTLY - no mapping
            loop = asyncio.get_event_loop()
            tick = await loop.run_in_executor(
                None,
                mt5.symbol_info_tick,
                symbol  # Use symbol exactly as provided
            )

            if tick is None:
                logger.warning(f"MT5 returned None for '{symbol}'")
                return None

            logger.info(f"MT5 tick for '{symbol}': Bid={tick.bid:.5f}, Ask={tick.ask:.5f}")

            # Validate price ranges
            if 'JPY' in symbol.upper():
                if tick.bid < 50:
                    logger.error(f"❌ CRITICAL: {symbol} price is wrong! Got {tick.bid:.5f}, expected >50")
                    # Return None to avoid caching wrong price
                    return None

            # Extract prices
            result = {
                'bid': tick.bid,
                'ask': tick.ask,
                'timestamp': datetime.fromtimestamp(tick.time),
                'volume': tick.volume,
                'last': tick.last,
                'spread': tick.ask - tick.bid
            }

            return result

        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}", exc_info=True)
            return None

    async def get_batch_prices(
            self,
            symbols: List[str],
            priorities: Optional[Dict[str, Priority]] = None
    ) -> Dict[str, Dict]:
        """
        Get prices for multiple symbols efficiently with smart caching
        ENHANCED: Better symbol tracking
        """
        if not symbols:
            return {}

        logger.info(f"=== BATCH FETCH START ===")
        logger.info(f"Requested symbols: {symbols}")
        logger.info(f"Priorities: {priorities}")

        # Update batch metrics
        self.total_batch_requests += 1
        self.total_symbols_requested += len(symbols)

        # Use cache if enabled
        if self.cache_enabled and priorities:
            cached_prices, symbols_to_fetch = await self.cache.get_batch_with_filter(
                symbols, priorities
            )

            # Log cache results
            logger.info(f"Cache provided {len(cached_prices)} prices:")
            for sym, price in cached_prices.items():
                logger.info(f"  {sym}: Bid={price.get('bid'):.5f}, Ask={price.get('ask'):.5f}")

                # Validate cached JPY prices
                if 'JPY' in sym.upper() and price.get('bid', 0) < 50:
                    logger.error(f"❌ CACHED JPY PRICE WRONG for {sym}: {price.get('bid'):.5f}")
                    # Remove from cache and refetch
                    symbols_to_fetch.append(sym)
                    del cached_prices[sym]
                    await self.cache.invalidate(sym)
                    logger.info(f"Invalidated cache for {sym}, will refetch")

            # If all prices are cached and valid, return immediately
            if not symbols_to_fetch:
                logger.info(f"All prices from cache (and valid)")
                return cached_prices

            # Fetch only uncached symbols
            logger.info(f"Need to fetch: {symbols_to_fetch}")
            fresh_prices = await self._fetch_batch_prices(symbols_to_fetch)

            # Validate fresh prices before caching
            for sym, price in fresh_prices.items():
                if 'JPY' in sym.upper() and price.get('bid', 0) < 50:
                    logger.error(f"❌ FRESH JPY PRICE WRONG for {sym}: {price.get('bid'):.5f}")
                    # Don't cache wrong prices
                    del fresh_prices[sym]

            # Update cache with validated fresh prices
            if fresh_prices:
                cache_updates = {}
                for sym, price_data in fresh_prices.items():
                    cache_updates[sym] = {
                        'bid': price_data['bid'],
                        'ask': price_data['ask'],
                        'timestamp': price_data['timestamp'].timestamp()
                    }
                await self.cache.update_batch(cache_updates, priorities)

            # Combine cached and fresh prices
            cached_prices.update(fresh_prices)

            logger.info(f"=== BATCH FETCH END ===")
            return cached_prices
        else:
            # Cache disabled or no priorities, fetch all
            return await self._fetch_batch_prices(symbols)

    async def _fetch_batch_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        results = {}

        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch batch prices - MT5 not connected")
                return results

            loop = asyncio.get_event_loop()

            for symbol in symbols:
                logger.info(f"Batch fetching '{symbol}'...")

                tick = await loop.run_in_executor(
                    None,
                    mt5.symbol_info_tick,
                    symbol
                )

                if tick is not None:
                    # LOG THE ACTUAL TICK DATA
                    logger.info(f"  MT5 returned for {symbol}: Bid={tick.bid:.5f}, Ask={tick.ask:.5f}")

                    results[symbol] = {
                        'bid': tick.bid,
                        'ask': tick.ask,
                        'timestamp': datetime.fromtimestamp(tick.time),
                        'volume': tick.volume,
                        'last': tick.last,
                        'spread': tick.ask - tick.bid
                    }

                    # VERIFY WHAT WAS STORED
                    logger.info(f"  Stored in results[{symbol}]: Bid={results[symbol]['bid']:.5f}")

                    # Check for obvious errors
                    if 'JPY' in symbol.upper():
                        if symbol.startswith('CAD') and results[symbol]['bid'] > 150:
                            logger.error(f"  ❌ WRONG: CADJPY price {results[symbol]['bid']} is too high!")
                        elif symbol.startswith('CHF') and results[symbol]['bid'] < 150:
                            logger.error(f"  ❌ WRONG: CHFJPY price {results[symbol]['bid']} is too low!")
                else:
                    logger.warning(f"  ✗ No data for {symbol}")

            # FINAL VERIFICATION
            logger.info(f"Final results dictionary:")
            for sym, price in results.items():
                logger.info(f"  {sym}: Bid={price['bid']:.5f}, Ask={price['ask']:.5f}")

            return results

        except Exception as e:
            logger.error(f"Error in batch price fetch: {e}", exc_info=True)
            return results

        except Exception as e:
            logger.error(f"Error in batch price fetch: {e}", exc_info=True)
            self._update_metrics(False, len(symbols))

            if self.should_reconnect():
                self.connected = False
                await self.ensure_connected()

            return results

    def _get_test_symbol(self) -> str:
        """Get test symbol for ICMarkets"""
        return 'EURUSD'

    async def set_cache_enabled(self, enabled: bool):
        """
        Enable or disable caching

        Args:
            enabled: True to enable cache, False to disable
        """
        self.cache_enabled = enabled
        if not enabled:
            await self.cache.invalidate()
        logger.info(f"Cache {'enabled' if enabled else 'disabled'}")

    def get_cache_stats(self) -> Dict:
        """
        Get cache statistics

        Returns:
            Dict with cache performance metrics
        """
        stats = self.cache.get_stats()
        stats['cache_enabled'] = self.cache_enabled
        stats['cache_bypass_count'] = self.cache_bypass_count
        return stats

    async def clear_cache(self, symbol: Optional[str] = None):
        """
        Clear cache for specific symbol or all

        Args:
            symbol: Specific symbol to clear, or None for all
        """
        await self.cache.invalidate(symbol)
        logger.info(f"Cache cleared for {symbol if symbol else 'all symbols'}")