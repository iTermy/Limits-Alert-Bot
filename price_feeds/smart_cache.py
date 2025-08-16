# price_feeds/smart_cache.py
"""
Smart Price Cache with Priority-Based TTL
Reduces API calls by 90% for low-priority signals while maintaining 
real-time accuracy for critical ones.
"""

import asyncio
import time
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class Priority(Enum):
    """Signal monitoring priority levels"""
    CRITICAL = "critical"  # <10 pips away
    MEDIUM = "medium"  # 10-50 pips away
    LOW = "low"  # >50 pips away


@dataclass
class CachedPrice:
    """Cached price data with metadata"""
    symbol: str
    bid: float
    ask: float
    timestamp: float  # market timestamp
    cached_at: float  # when we cached it
    priority: Priority
    fetch_count: int = 0  # number of times fetched
    hit_count: int = 0  # number of cache hits

    def is_expired(self, ttl: float) -> bool:
        """Check if cache entry has expired"""
        return (time.time() - self.cached_at) > ttl

    def to_dict(self) -> Dict[str, float]:
        """Convert to price dictionary format"""
        return {
            'bid': self.bid,
            'ask': self.ask,
            'timestamp': self.timestamp
        }


class SmartPriceCache:
    """
    Priority-aware price cache with variable TTL.
    Dramatically reduces API calls while maintaining accuracy for critical signals.
    """

    def __init__(self, custom_ttl: Optional[Dict[str, float]] = None):
        """
        Initialize smart cache with configurable TTL values.

        Args:
            custom_ttl: Optional custom TTL values by priority
        """
        # Default TTL values (in seconds)
        self.ttl = {
            Priority.CRITICAL: 1,  # 1 second for critical
            Priority.MEDIUM: 30,  # 30 seconds for medium
            Priority.LOW: 120  # 2 minutes for low
        }

        # Override with custom values if provided
        if custom_ttl:
            for priority_str, ttl_value in custom_ttl.items():
                try:
                    priority = Priority(priority_str)
                    self.ttl[priority] = ttl_value
                except ValueError:
                    logger.warning(f"Invalid priority '{priority_str}' in custom TTL")

        # Thread-safe cache storage
        self.cache: Dict[str, CachedPrice] = {}
        self.lock = asyncio.Lock()

        # Performance metrics
        self.total_fetches = 0
        self.cache_hits = 0
        self.cache_misses = 0

        # Cache size limit to prevent memory issues
        self.max_cache_size = 1000

        logger.info(f"SmartPriceCache initialized with TTL: {self.format_ttl()}")

    def format_ttl(self) -> str:
        """Format TTL settings for logging"""
        return ", ".join([f"{p.value}={t}s" for p, t in self.ttl.items()])

    async def get_price(
            self,
            symbol: str,
            priority: Priority = Priority.MEDIUM
    ) -> Optional[Dict[str, float]]:
        """
        Get price from cache if valid, returns None if expired/missing.
        ENHANCED WITH DEBUGGING
        """
        async with self.lock:
            if symbol not in self.cache:
                self.cache_misses += 1
                logger.debug(f"Cache miss for {symbol} - not in cache")
                return None

            cached = self.cache[symbol]
            ttl = self.ttl[priority]

            if cached.is_expired(ttl):
                self.cache_misses += 1
                age = time.time() - cached.cached_at
                logger.debug(f"Cache expired for {symbol} (age={age:.1f}s > ttl={ttl}s)")
                return None

            # Update hit count
            cached.hit_count += 1
            self.cache_hits += 1

            age = time.time() - cached.cached_at
            logger.debug(f"Cache hit for {symbol}: Bid={cached.bid}, Ask={cached.ask}, Age={age:.1f}s")

            result = cached.to_dict()

            # Verify the result
            if result['bid'] == result['ask']:
                logger.warning(f"WARNING: Returning identical bid/ask from cache for {symbol}: {result['bid']}")

            return result

    async def update_price(
            self,
            symbol: str,
            bid: float,
            ask: float,
            timestamp: float,
            priority: Priority = Priority.MEDIUM
    ) -> None:
        """
        Update cache with new price data.
        ENHANCED WITH DEBUGGING
        """
        logger.info(f"update_price called: {symbol} Bid={bid}, Ask={ask}, Priority={priority.name}")

        if bid == ask:
            logger.warning(f"WARNING: Identical bid/ask being cached for {symbol}: {bid}")

        async with self.lock:
            # Check cache size limit
            if len(self.cache) >= self.max_cache_size:
                await self._evict_oldest()

            # Update fetch count if exists
            fetch_count = 1
            if symbol in self.cache:
                old_entry = self.cache[symbol]
                fetch_count = old_entry.fetch_count + 1
                logger.debug(f"Replacing cache entry for {symbol} (was Bid={old_entry.bid}, Ask={old_entry.ask})")

            self.cache[symbol] = CachedPrice(
                symbol=symbol,
                bid=bid,
                ask=ask,
                timestamp=timestamp,
                cached_at=time.time(),
                priority=priority,
                fetch_count=fetch_count
            )

            self.total_fetches += 1

            # Verify what was stored
            stored = self.cache[symbol]
            logger.info(f"Cache stored for {symbol}: Bid={stored.bid}, Ask={stored.ask}")

            if stored.bid != bid or stored.ask != ask:
                logger.error(f"ERROR: Cache corruption! Stored Bid={stored.bid} vs Input Bid={bid}, "
                             f"Stored Ask={stored.ask} vs Input Ask={ask}")

    async def update_batch(
            self,
            prices: Dict[str, Dict[str, float]],
            priorities: Optional[Dict[str, Priority]] = None
    ) -> None:
        """
        Update multiple prices at once.
        FIXED: Ensure bid/ask are properly stored
        """
        logger.info(f"update_batch called with {len(prices)} prices")

        for symbol, price_data in prices.items():
            priority = priorities.get(symbol, Priority.MEDIUM) if priorities else Priority.MEDIUM

            # Ensure we have both bid and ask
            if 'bid' not in price_data or 'ask' not in price_data:
                logger.error(f"Missing bid or ask for {symbol}: {price_data}")
                continue

            bid = price_data['bid']
            ask = price_data['ask']

            # Check for issues
            if bid == ask:
                logger.warning(f"WARNING: Identical bid/ask in batch for {symbol}: {bid}")

            # Ensure timestamp exists
            timestamp = price_data.get('timestamp', time.time())

            logger.debug(f"Batch updating {symbol}: Bid={bid}, Ask={ask}")

            await self.update_price(
                symbol=symbol,
                bid=bid,
                ask=ask,
                timestamp=timestamp,
                priority=priority
            )

    async def get_batch_with_filter(
            self,
            symbols: List[str],
            priorities: Optional[Dict[str, Priority]] = None
    ) -> Tuple[Dict[str, Dict], List[str]]:
        """
        Get cached prices and list of symbols needing fetch.

        Args:
            symbols: List of symbols to check
            priorities: Optional dict of symbol -> priority

        Returns:
            Tuple of (cached_prices, symbols_to_fetch)
        """
        cached_prices = {}
        symbols_to_fetch = []

        for symbol in symbols:
            priority = priorities.get(symbol, Priority.MEDIUM) if priorities else Priority.MEDIUM
            price = await self.get_price(symbol, priority)

            if price:
                cached_prices[symbol] = price.copy() if isinstance(price, dict) else price
            else:
                symbols_to_fetch.append(symbol)

        if cached_prices:
            logger.info(f"Cache provided {len(cached_prices)}/{len(symbols)} prices")

        return cached_prices, symbols_to_fetch

    async def _evict_oldest(self) -> None:
        """Evict oldest cache entries when size limit reached"""
        if not self.cache:
            return

        # Find oldest entry
        oldest_symbol = min(self.cache.keys(),
                            key=lambda s: self.cache[s].cached_at)

        evicted = self.cache.pop(oldest_symbol)
        logger.debug(f"Evicted {oldest_symbol} from cache (age={time.time() - evicted.cached_at:.1f}s)")

    async def invalidate(self, symbol: Optional[str] = None) -> None:
        """
        Invalidate cache entries.

        Args:
            symbol: Specific symbol to invalidate, or None for all
        """
        async with self.lock:
            if symbol:
                if symbol in self.cache:
                    del self.cache[symbol]
                    logger.debug(f"Invalidated cache for {symbol}")
            else:
                self.cache.clear()
                logger.info("Cleared entire cache")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        hit_rate = 0
        if self.cache_hits + self.cache_misses > 0:
            hit_rate = self.cache_hits / (self.cache_hits + self.cache_misses)

        return {
            'cache_size': len(self.cache),
            'total_fetches': self.total_fetches,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': f"{hit_rate:.1%}",
            'symbols_cached': list(self.cache.keys())[:10]  # First 10 for brevity
        }

    def calculate_priority(
            self,
            distance_pips: float,
            asset_class: str = "forex"
    ) -> Priority:
        """
        Calculate priority based on distance to limit.

        Args:
            distance_pips: Distance in pips/points to limit
            asset_class: Type of asset (forex, metals, crypto, stocks)

        Returns:
            Priority level
        """
        # Adjust thresholds based on asset class
        thresholds = {
            "forex": (10, 50),  # critical<10, medium<50
            "metals": (15, 75),  # metals are more volatile
            "crypto": (100, 500),  # crypto much more volatile
            "stocks": (5, 25)  # stocks relatively stable
        }

        critical_threshold, medium_threshold = thresholds.get(asset_class, (10, 50))

        if distance_pips < critical_threshold:
            return Priority.CRITICAL
        elif distance_pips < medium_threshold:
            return Priority.MEDIUM
        else:
            return Priority.LOW

    async def cleanup_expired(self) -> int:
        """
        Remove all expired entries from cache.

        Returns:
            Number of entries removed
        """
        async with self.lock:
            expired_symbols = []
            current_time = time.time()

            for symbol, cached in self.cache.items():
                # Use the maximum TTL for cleanup
                max_ttl = max(self.ttl.values())
                if (current_time - cached.cached_at) > max_ttl:
                    expired_symbols.append(symbol)

            for symbol in expired_symbols:
                del self.cache[symbol]

            if expired_symbols:
                logger.info(f"Cleaned up {len(expired_symbols)} expired cache entries")

            return len(expired_symbols)


class CacheIntegratedFeed:
    """
    Mixin class to add caching to any price feed.
    This should be inherited alongside BaseFeed.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = SmartPriceCache()

    async def get_batch_prices_with_cache(
            self,
            symbols: List[str],
            priorities: Optional[Dict[str, Priority]] = None
    ) -> Dict[str, Dict[str, float]]:
        """
        Get batch prices using cache when possible.

        Args:
            symbols: List of symbols to fetch
            priorities: Optional priority mapping

        Returns:
            Dict of symbol -> price data
        """
        # Check cache first
        cached_prices, symbols_to_fetch = await self.cache.get_batch_with_filter(
            symbols, priorities
        )

        # Fetch missing prices
        if symbols_to_fetch:
            logger.info(f"Fetching {len(symbols_to_fetch)} prices from feed")
            fresh_prices = await self.get_batch_prices(symbols_to_fetch)

            # Update cache with fresh data
            await self.cache.update_batch(fresh_prices, priorities)

            # Combine results
            cached_prices.update(fresh_prices)

        return cached_prices

    async def get_price_with_cache(
            self,
            symbol: str,
            priority: Priority = Priority.MEDIUM
    ) -> Optional[Dict[str, float]]:
        """
        Get single price using cache when possible.

        Args:
            symbol: Symbol to fetch
            priority: Priority level

        Returns:
            Price data or None
        """
        # Check cache first
        cached = await self.cache.get_price(symbol, priority)
        if cached:
            return cached

        # Fetch from feed
        fresh = await self.get_price(symbol)
        if fresh:
            await self.cache.update_price(
                symbol=symbol,
                bid=fresh['bid'],
                ask=fresh['ask'],
                timestamp=fresh['timestamp'],
                priority=priority
            )

        return fresh


# Example integration with ICMarkets feed
async def integrate_cache_with_icmarkets():
    """
    Example of how to integrate cache with existing ICMarkets feed.
    Add this to feeds/icmarkets.py:
    """
    # In ICMarketsFeed.__init__:
    # self.cache = SmartPriceCache()

    # Modify get_batch_prices method:
    """
    async def get_batch_prices(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        # Check cache first
        cached_prices, symbols_to_fetch = await self.cache.get_batch_with_filter(symbols)

        if not symbols_to_fetch:
            return cached_prices

        # Fetch only uncached symbols
        fresh_prices = await self._fetch_from_mt5(symbols_to_fetch)

        # Update cache
        await self.cache.update_batch(fresh_prices)

        # Return combined results
        return {**cached_prices, **fresh_prices}
    """