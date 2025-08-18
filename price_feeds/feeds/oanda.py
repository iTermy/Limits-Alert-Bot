"""
OANDA v20 Price Feed - REST API Implementation
Handles forex and indices with smart caching
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


class OANDAFeed(BaseFeed):
    """
    OANDA v20 REST API price feed implementation

    Primary for: Indices
    Fallback for: Forex
    Uses smart caching to minimize API calls
    """

    def __init__(self, cache_instance: Optional[SmartPriceCache] = None,
                 api_key: Optional[str] = None,
                 account_id: Optional[str] = None,
                 practice: bool = False):
        """
        Initialize OANDA feed with v20 API

        Args:
            cache_instance: Shared cache instance from monitor
            api_key: OANDA API key (will read from env if not provided)
            account_id: OANDA account ID (will read from env if not provided)
            practice: Use practice server if True, live if False
        """
        super().__init__("OANDA")

        # Use shared cache or create new one
        if cache_instance is not None:
            self.cache = cache_instance
            logger.info("Using shared cache instance from monitor")
        else:
            self.cache = SmartPriceCache()
            logger.info("Created new cache instance")

        # API Configuration
        import os
        self.api_key = api_key or os.getenv('OANDA_API_KEY')
        self.account_id = account_id or os.getenv('OANDA_ACCOUNT_ID')

        if not self.api_key or not self.account_id:
            logger.warning("OANDA API key or account ID not configured")

        # Server URLs
        if practice:
            self.base_url = "https://api-fxpractice.oanda.com"
        else:
            self.base_url = "https://api-fxtrade.oanda.com"

        self.pricing_url = f"{self.base_url}/v3/accounts/{self.account_id}/pricing"

        # Session for connection pooling
        self.session: Optional[aiohttp.ClientSession] = None

        # Rate limiting (OANDA allows 120 requests per second)
        self.last_request_time = 0
        self.min_request_interval = 0.01  # 100 requests per second max

        # Supported instruments cache
        self.supported_instruments = set()

        logger.info(f"OANDA feed initialized for {'practice' if practice else 'live'} account")

    async def connect(self) -> bool:
        """
        Initialize OANDA connection and validate credentials
        """
        try:
            if not self.api_key or not self.account_id:
                logger.error("OANDA API credentials not configured")
                return False

            # Create session with headers
            self.session = aiohttp.ClientSession(
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json',
                    'Accept-Datetime-Format': 'UNIX'
                }
            )

            # Test connection by getting account info
            async with self.session.get(f"{self.base_url}/v3/accounts/{self.account_id}") as response:
                if response.status == 200:
                    data = await response.json()
                    self.connected = True
                    logger.info(f"Connected to OANDA - Account: {data['account']['alias']}, "
                                f"Balance: {data['account']['balance']}, "
                                f"Currency: {data['account']['currency']}")

                    # Cache available instruments
                    await self._cache_available_instruments()
                    return True
                else:
                    error_data = await response.text()
                    logger.error(f"OANDA connection failed: {response.status} - {error_data}")
                    return False

        except Exception as e:
            logger.error(f"Error connecting to OANDA: {e}", exc_info=True)
            self.connected = False
            return False

    async def disconnect(self):
        """Close OANDA session"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
            self.connected = False
            logger.info("Disconnected from OANDA")

            # Log cache stats
            if self.cache:
                cache_stats = self.cache.get_stats()
                logger.info(f"Cache stats at disconnect: {cache_stats}")

        except Exception as e:
            logger.error(f"Error disconnecting from OANDA: {e}")

    async def ensure_connected(self) -> bool:
        """Ensure OANDA session is active"""
        if not self.connected or not self.session:
            logger.info("OANDA not connected, attempting to connect...")
            return await self.connect()

        # Quick health check
        try:
            async with self.session.get(
                    f"{self.base_url}/v3/accounts/{self.account_id}/summary",
                    timeout=5
            ) as response:
                if response.status != 200:
                    logger.warning("OANDA connection lost, reconnecting...")
                    await self.disconnect()
                    return await self.connect()
                return True

        except Exception as e:
            logger.error(f"OANDA connection check failed: {e}")
            await self.disconnect()
            return await self.connect()

    async def _cache_available_instruments(self):
        """Cache list of available instruments from OANDA"""
        try:
            async with self.session.get(
                    f"{self.base_url}/v3/accounts/{self.account_id}/instruments"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    self.supported_instruments = {
                        inst['name'] for inst in data['instruments']
                    }
                    logger.info(f"Cached {len(self.supported_instruments)} OANDA instruments")

        except Exception as e:
            logger.error(f"Error caching OANDA instruments: {e}")

    async def _rate_limit(self):
        """Simple rate limiting to avoid hitting OANDA limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)

        self.last_request_time = asyncio.get_event_loop().time()

    async def get_price(self, symbol: str, priority: Priority = Priority.MEDIUM) -> Optional[Dict]:
        """
        Get current bid/ask price for a single symbol with caching

        Args:
            symbol: OANDA format symbol (e.g., EUR_USD, SPX500_USD)
            priority: Cache priority level
        """
        # Check cache first
        if self.cache:
            cached_price = await self.cache.get_price(symbol, priority)
            if cached_price:
                logger.debug(f"Cache hit for {symbol}")
                # Ensure timestamp is datetime
                if isinstance(cached_price.get('timestamp'), (int, float)):
                    cached_price['timestamp'] = datetime.fromtimestamp(cached_price['timestamp'])
                return cached_price

        # Fetch from OANDA
        logger.debug(f"Cache miss for {symbol}, fetching from OANDA")
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
        Fetch single price from OANDA API

        Args:
            symbol: OANDA format symbol
        """
        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch price - OANDA not connected")
                return None

            # Check if instrument is supported
            if self.supported_instruments and symbol not in self.supported_instruments:
                logger.warning(f"Instrument {symbol} not available on OANDA")
                return None

            # Rate limiting
            await self._rate_limit()

            # Fetch price
            params = {'instruments': symbol}
            async with self.session.get(self.pricing_url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()

                    if 'prices' in data and len(data['prices']) > 0:
                        price_data = data['prices'][0]

                        # Extract bid/ask from best prices
                        if 'bids' in price_data and len(price_data['bids']) > 0:
                            bid = float(price_data['bids'][0]['price'])
                        else:
                            bid = float(price_data.get('closeoutBid', 0))

                        if 'asks' in price_data and len(price_data['asks']) > 0:
                            ask = float(price_data['asks'][0]['price'])
                        else:
                            ask = float(price_data.get('closeoutAsk', 0))

                        result = {
                            'bid': bid,
                            'ask': ask,
                            'timestamp': datetime.fromtimestamp(float(price_data.get('time', 0))),
                            'spread': ask - bid,
                            'tradeable': price_data.get('tradeable', False),
                            'instrument': symbol
                        }

                        self._update_metrics(True)
                        return result
                    else:
                        logger.warning(f"No price data for {symbol}")
                        self._update_metrics(False)
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"OANDA price fetch failed ({response.status}): {error_text}")
                    self._update_metrics(False)
                    return None

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching price for {symbol}")
            self._update_metrics(False)
            return None
        except Exception as e:
            logger.error(f"Error fetching OANDA price for {symbol}: {e}", exc_info=True)
            self._update_metrics(False)
            return None

    async def get_batch_prices(
            self,
            symbols: List[str],
            priorities: Optional[Dict[str, Priority]] = None
    ) -> Dict[str, Dict]:
        """
        Get prices for multiple symbols efficiently with smart caching

        OANDA allows up to 20 instruments per request
        """
        if not symbols:
            return {}

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
            logger.debug(f"Fetching {len(symbols_to_fetch)} prices from OANDA")
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
        Fetch batch prices from OANDA (max 20 per request)
        """
        results = {}

        try:
            if not await self.ensure_connected():
                logger.error("Cannot fetch batch prices - OANDA not connected")
                return results

            # OANDA allows max 20 instruments per request
            batch_size = 20

            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]

                # Rate limiting
                await self._rate_limit()

                # Create comma-separated list
                instruments_param = ','.join(batch)
                params = {'instruments': instruments_param}

                async with self.session.get(self.pricing_url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()

                        for price_data in data.get('prices', []):
                            symbol = price_data['instrument']

                            # Extract bid/ask
                            if 'bids' in price_data and len(price_data['bids']) > 0:
                                bid = float(price_data['bids'][0]['price'])
                            else:
                                bid = float(price_data.get('closeoutBid', 0))

                            if 'asks' in price_data and len(price_data['asks']) > 0:
                                ask = float(price_data['asks'][0]['price'])
                            else:
                                ask = float(price_data.get('closeoutAsk', 0))

                            results[symbol] = {
                                'bid': bid,
                                'ask': ask,
                                'timestamp': datetime.fromtimestamp(float(price_data.get('time', 0))),
                                'spread': ask - bid,
                                'tradeable': price_data.get('tradeable', False)
                            }

                            self.total_symbols_fetched += 1
                    else:
                        error_text = await response.text()
                        logger.error(f"OANDA batch fetch failed ({response.status}): {error_text}")

            self._update_metrics(len(results) > 0, len(symbols))
            return results

        except Exception as e:
            logger.error(f"Error in OANDA batch fetch: {e}", exc_info=True)
            self._update_metrics(False, len(symbols))
            return results

    def _get_test_symbol(self) -> str:
        """Get test symbol for OANDA"""
        return 'EUR_USD'  # Most liquid forex pair

    async def get_account_info(self) -> Optional[Dict]:
        """Get OANDA account information"""
        try:
            if not await self.ensure_connected():
                return None

            async with self.session.get(
                    f"{self.base_url}/v3/accounts/{self.account_id}/summary"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['account']

        except Exception as e:
            logger.error(f"Error getting OANDA account info: {e}")

        return None