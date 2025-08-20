"""
Binance.US Price Feed - REST API Implementation
Handles crypto spot prices with smart caching
No API key required for public market data
"""

import asyncio
import aiohttp
import logging
from typing import Dict, Optional, List
from datetime import datetime
import json

from price_feeds.feeds.base import BaseFeed
from price_feeds.smart_cache import SmartPriceCache, Priority

logger = logging.getLogger(__name__)


class BinanceFeed(BaseFeed):
    """
    Binance.US REST API price feed implementation

    Primary for: Crypto (spot prices only)
    Uses smart caching to minimize API calls
    No authentication required for public endpoints
    """

    def __init__(self, cache_instance: Optional[SmartPriceCache] = None,
                 use_testnet: bool = False,
                 use_international: bool = False):
        """
        Initialize Binance feed (US or International)

        Args:
            cache_instance: Shared cache instance from monitor
            use_testnet: Use testnet if True (for testing)
            use_international: Use api.binance.com instead of api.binance.us
        """
        super().__init__("Binance")

        # Use shared cache or create new one
        if cache_instance is not None:
            self.cache = cache_instance
            logger.info("Using shared cache instance from monitor")
        else:
            self.cache = SmartPriceCache()
            logger.info("Created new cache instance")

        # Determine which Binance API to use
        self.use_international = use_international

        if use_testnet:
            self.base_url = "https://testnet.binance.vision/api/v3"
            logger.info("Using Binance testnet")
        elif use_international:
            self.base_url = "https://api.binance.com/api/v3"
            logger.info("Using Binance International API")
        else:
            self.base_url = "https://api.binance.us/api/v3"
            logger.info("Using Binance.US API")

        # Endpoints
        self.ticker_url = f"{self.base_url}/ticker/bookTicker"  # Best bid/ask
        self.price_url = f"{self.base_url}/ticker/price"  # Latest price
        self.exchange_info_url = f"{self.base_url}/exchangeInfo"
        self.ping_url = f"{self.base_url}/ping"

        # Session for connection pooling
        self.session: Optional[aiohttp.ClientSession] = None

        # Rate limiting (both Binance.US and .com have similar limits)
        # Weight: 1200 per minute for public endpoints
        self.last_request_time = 0
        self.min_request_interval = 0.05  # 20 requests per second max

        # Supported symbols cache
        self.supported_symbols = set()
        self.symbol_info = {}  # Store precision info

        # Track which API we're actually using
        self.api_type = "testnet" if use_testnet else ("international" if use_international else "US")

        logger.info(f"Binance feed initialized (API: {self.api_type})")

    async def connect(self) -> bool:
        """
        Initialize Binance connection with fallback logic
        """
        try:
            # Create session
            self.session = aiohttp.ClientSession(
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'TradingBot/1.0'
                },
                timeout=aiohttp.ClientTimeout(total=10)
            )

            # Try to connect
            success = await self._try_connect()

            if not success and not self.use_international and "binance.us" in self.base_url:
                # If Binance.US fails, try international
                logger.warning("Binance.US connection failed, trying Binance International...")
                self.base_url = "https://api.binance.com/api/v3"
                self.api_type = "international (fallback)"

                # Update endpoints
                self.ticker_url = f"{self.base_url}/ticker/bookTicker"
                self.price_url = f"{self.base_url}/ticker/price"
                self.exchange_info_url = f"{self.base_url}/exchangeInfo"
                self.ping_url = f"{self.base_url}/ping"

                success = await self._try_connect()

            if success:
                self.connected = True
                return True
            else:
                await self.session.close()
                self.session = None
                self.connected = False
                return False

        except Exception as e:
            logger.error(f"Error connecting to Binance: {e}", exc_info=True)
            if self.session:
                await self.session.close()
                self.session = None
            self.connected = False
            return False

    async def _try_connect(self) -> bool:
        """
        Try to connect to current Binance endpoint
        """
        try:
            # Test connection with ping
            async with self.session.get(self.ping_url, timeout=5) as response:
                if response.status != 200:
                    logger.error(f"Binance ping failed: {response.status}")
                    return False

            # Get exchange info for available symbols
            async with self.session.get(self.exchange_info_url, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()

                    # Cache trading symbols and their info
                    self.supported_symbols.clear()
                    self.symbol_info.clear()

                    for symbol_data in data['symbols']:
                        if symbol_data['status'] == 'TRADING':
                            symbol = symbol_data['symbol']
                            self.supported_symbols.add(symbol)

                            # Store precision info for price formatting
                            self.symbol_info[symbol] = {
                                'baseAsset': symbol_data['baseAsset'],
                                'quoteAsset': symbol_data['quoteAsset'],
                                'quotePrecision': symbol_data.get('quotePrecision', 8),
                                'baseAssetPrecision': symbol_data.get('baseAssetPrecision', 8),
                                'filters': symbol_data.get('filters', [])
                            }

                    logger.info(f"Connected to Binance {self.api_type} - {len(self.supported_symbols)} trading symbols available")

                    # Log some crypto symbols to verify
                    crypto_symbols = [s for s in self.supported_symbols if 'USDT' in s][:5]
                    logger.info(f"Sample crypto symbols: {crypto_symbols}")

                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get Binance exchange info: {error_text}")
                    return False

        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to Binance {self.api_type}")
            return False
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Cannot connect to Binance {self.api_type}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error in Binance connection attempt: {e}")
            return False

    async def disconnect(self):
        """Close Binance session"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
            self.connected = False
            logger.info("Disconnected from Binance")

            # Log cache stats
            if self.cache:
                cache_stats = self.cache.get_stats()
                logger.info(f"Cache stats at disconnect: {cache_stats}")

        except Exception as e:
            logger.error(f"Error disconnecting from Binance: {e}")

    async def ensure_connected(self) -> bool:
        """Ensure Binance connection is active"""
        if not self.connected or not self.session:
            logger.info("Binance not connected, attempting to connect...")
            return await self.connect()

        # Quick ping check
        try:
            async with self.session.get(self.ping_url, timeout=5) as response:
                if response.status != 200:
                    logger.warning("Binance connection lost, reconnecting...")
                    await self.disconnect()
                    return await self.connect()
                return True

        except Exception as e:
            logger.error(f"Binance connection check failed: {e}")
            await self.disconnect()
            return await self.connect()

    async def _rate_limit(self):
        """Simple rate limiting to avoid hitting Binance limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)

        self.last_request_time = asyncio.get_event_loop().time()

    def _validate_symbol(self, symbol: str) -> bool:
        """Check if symbol is supported on Binance.US"""
        if not self.supported_symbols:
            # If we haven't cached symbols yet, assume it's valid
            return True
        return symbol.upper() in self.supported_symbols

    async def get_price(self, symbol: str, priority: Priority = Priority.MEDIUM) -> Optional[Dict]:
        """
        Get current bid/ask price for a single symbol with caching

        Args:
            symbol: Binance format symbol (e.g., BTCUSDT, ETHUSDT)
            priority: Cache priority level
        """
        symbol = symbol.upper()

        # Check cache first
        if self.cache:
            cached_price = await self.cache.get_price(symbol, priority)
            if cached_price:
                logger.debug(f"Cache hit for {symbol}")
                # Ensure timestamp is datetime
                if isinstance(cached_price.get('timestamp'), (int, float)):
                    cached_price['timestamp'] = datetime.fromtimestamp(cached_price['timestamp'])
                return cached_price

        # Fetch from Binance
        logger.debug(f"Cache miss for {symbol}, fetching from Binance")
        fresh_price = await self._fetch_single_price(symbol)

        # Update cache if successful
        if fresh_price and self.cache:
            await self.cache.update_price(
                symbol=symbol,
                bid=fresh_price['bid'],
                ask=fresh_price['ask'],
                timestamp=fresh_price['timestamp'].timestamp(),
                priority=priority
            )

        return fresh_price

    async def _fetch_single_price(self, symbol: str) -> Optional[Dict]:
        """
        Fetch single price from Binance API using bookTicker endpoint
        """
        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch price - Binance not connected")
                return None

            # Validate symbol
            if not self._validate_symbol(symbol):
                logger.warning(f"Symbol {symbol} not available on Binance.US")
                return None

            # Rate limiting
            await self._rate_limit()

            # Fetch best bid/ask from bookTicker
            params = {'symbol': symbol}
            async with self.session.get(self.ticker_url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()

                    # bookTicker returns best bid and ask prices
                    bid = float(data['bidPrice'])
                    ask = float(data['askPrice'])

                    result = {
                        'bid': bid,
                        'ask': ask,
                        'timestamp': datetime.now(),  # Binance doesn't provide timestamp in bookTicker
                        'spread': ask - bid,
                        'bid_qty': float(data.get('bidQty', 0)),
                        'ask_qty': float(data.get('askQty', 0))
                    }

                    self._update_metrics(True)
                    return result

                elif response.status == 400:
                    error_data = await response.json()
                    if error_data.get('code') == -1121:  # Invalid symbol
                        logger.warning(f"Invalid symbol {symbol} for Binance")
                    else:
                        logger.error(f"Binance error: {error_data}")
                    self._update_metrics(False)
                    return None
                else:
                    error_text = await response.text()
                    logger.error(f"Binance price fetch failed ({response.status}): {error_text}")
                    self._update_metrics(False)
                    return None

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching price for {symbol}")
            self._update_metrics(False)
            return None
        except Exception as e:
            logger.error(f"Error fetching Binance price for {symbol}: {e}", exc_info=True)
            self._update_metrics(False)
            return None

    async def get_batch_prices(
        self,
        symbols: List[str],
        priorities: Optional[Dict[str, Priority]] = None
    ) -> Dict[str, Dict]:
        """
        Get prices for multiple symbols efficiently with smart caching

        Binance allows fetching all tickers at once, then we filter
        """
        if not symbols:
            return {}

        # Ensure symbols are uppercase
        symbols = [s.upper() for s in symbols]

        logger.debug(f"Batch fetch for {len(symbols)} symbols")

        # Update metrics
        self.total_batch_requests += 1
        self.total_symbols_requested += len(symbols)

        # Use cache if enabled
        if self.cache and priorities:
            cached_prices, symbols_to_fetch = await self.cache.get_batch_with_filter(
                symbols, priorities
            )

            if not symbols_to_fetch:
                logger.debug(f"All {len(symbols)} prices from cache")
                return cached_prices

            # Fetch uncached symbols
            logger.debug(f"Fetching {len(symbols_to_fetch)} prices from Binance")
            fresh_prices = await self._fetch_batch_prices(symbols_to_fetch)

            # Update cache with fresh prices
            if fresh_prices:
                cache_updates = {}
                for sym, price_data in fresh_prices.items():
                    cache_updates[sym] = {
                        'bid': price_data['bid'],
                        'ask': price_data['ask'],
                        'timestamp': price_data['timestamp'].timestamp()
                    }
                await self.cache.update_batch(cache_updates, priorities)

            # Combine results
            cached_prices.update(fresh_prices)
            return cached_prices
        else:
            # No cache, fetch all
            return await self._fetch_batch_prices(symbols)

    async def _fetch_batch_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Fetch batch prices from Binance

        Strategy: Fetch all bookTickers, then filter for needed symbols
        This is more efficient than multiple individual requests
        """
        results = {}

        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch batch prices - Binance not connected")
                return results

            # Rate limiting
            await self._rate_limit()

            # Fetch all book tickers (no params = all symbols)
            async with self.session.get(self.ticker_url, timeout=2) as response:
                if response.status == 200:
                    data = await response.json()

                    # Create a set for faster lookup
                    symbols_set = set(symbols)

                    # Filter for our symbols
                    for ticker in data:
                        symbol = ticker['symbol']
                        if symbol in symbols_set:
                            bid = float(ticker['bidPrice'])
                            ask = float(ticker['askPrice'])

                            results[symbol] = {
                                'bid': bid,
                                'ask': ask,
                                'timestamp': datetime.now(),
                                'spread': ask - bid,
                                'bid_qty': float(ticker.get('bidQty', 0)),
                                'ask_qty': float(ticker.get('askQty', 0))
                            }

                            self.total_symbols_fetched += 1

                    # Log any symbols we couldn't find
                    missing = symbols_set - set(results.keys())
                    if missing:
                        logger.warning(f"Symbols not found on Binance: {missing}")

                elif response.status == 429:  # Rate limit
                    logger.error("Binance rate limit hit! Backing off...")
                    # Mark feed as failed to trigger blacklist
                    raise Exception("Rate limit exceeded")

                else:
                    error_text = await response.text()
                    logger.error(f"Binance batch fetch failed ({response.status}): {error_text}")

            self._update_metrics(len(results) > 0, len(symbols))
            return results

        except Exception as e:
            logger.error(f"Error in Binance batch fetch: {e}", exc_info=True)
            self._update_metrics(False, len(symbols))
            return results

    def _get_test_symbol(self) -> str:
        """Get test symbol for Binance"""
        return 'BTCUSDT'  # Most liquid crypto pair

    async def get_24hr_stats(self, symbol: str) -> Optional[Dict]:
        """
        Get 24hr ticker statistics for a symbol

        Args:
            symbol: Binance format symbol

        Returns:
            Dict with 24hr stats including volume, high, low, etc.
        """
        try:
            if not await self.ensure_connected():
                return None

            symbol = symbol.upper()

            # Rate limiting
            await self._rate_limit()

            url = f"{self.base_url}/ticker/24hr"
            params = {'symbol': symbol}

            async with self.session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'symbol': data['symbol'],
                        'price_change': float(data['priceChange']),
                        'price_change_percent': float(data['priceChangePercent']),
                        'weighted_avg_price': float(data['weightedAvgPrice']),
                        'prev_close': float(data['prevClosePrice']),
                        'last_price': float(data['lastPrice']),
                        'bid': float(data['bidPrice']),
                        'ask': float(data['askPrice']),
                        'open': float(data['openPrice']),
                        'high': float(data['highPrice']),
                        'low': float(data['lowPrice']),
                        'volume': float(data['volume']),
                        'quote_volume': float(data['quoteVolume']),
                        'open_time': datetime.fromtimestamp(data['openTime'] / 1000),
                        'close_time': datetime.fromtimestamp(data['closeTime'] / 1000),
                        'count': data['count']  # Number of trades
                    }

        except Exception as e:
            logger.error(f"Error getting 24hr stats for {symbol}: {e}")

        return None