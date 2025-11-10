"""
OANDA v20 Streaming Feed
Uses OANDA's native pricing stream endpoint for real-time updates
"""

import asyncio
import aiohttp
import logging
import json
from typing import Dict, Set, AsyncIterator, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class OANDAStream:
    """
    OANDA v20 streaming price feed

    Uses the /v3/accounts/{accountId}/pricing/stream endpoint
    which provides continuous real-time pricing updates
    """

    def __init__(self, api_key: str = None, account_id: str = None, practice: bool = False):
        """Initialize OANDA stream"""
        import os
        self.api_key = api_key or os.getenv('OANDA_API_KEY')
        self.account_id = account_id or os.getenv('OANDA_ACCOUNT_ID')

        if not self.api_key or not self.account_id:
            raise ValueError("OANDA API key and account ID required")

        # Server URLs
        if practice:
            self.base_url = "https://stream-fxpractice.oanda.com"
        else:
            self.base_url = "https://stream-fxtrade.oanda.com"

        self.stream_url = f"{self.base_url}/v3/accounts/{self.account_id}/pricing/stream"

        # Connection management
        self.session: aiohttp.ClientSession = None
        self.connected = False
        self.subscribed_symbols: Set[str] = set()

        # Stream control
        self.streaming = False
        self.stream_response = None

        logger.info("OANDAStream initialized")

    async def connect(self) -> bool:
        """Initialize OANDA session"""
        try:
            self.session = aiohttp.ClientSession(
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json',
                    'Accept-Datetime-Format': 'UNIX'
                },
                timeout=aiohttp.ClientTimeout(total=None)  # No timeout for streaming
            )

            # Test connection
            test_url = f"{self.base_url.replace('stream-', 'api-')}/v3/accounts/{self.account_id}/summary"
            async with self.session.get(test_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    self.connected = True
                    logger.info("Connected to OANDA stream")
                    return True
                else:
                    logger.error(f"OANDA connection failed: {response.status}")
                    return False

        except Exception as e:
            logger.error(f"Error connecting to OANDA: {e}")
            return False

    async def disconnect(self):
        """Close OANDA session"""
        self.streaming = False

        if self.stream_response:
            self.stream_response.close()

        if self.session:
            await self.session.close()
            self.session = None

        self.connected = False
        logger.info("Disconnected from OANDA")

    async def reconnect(self):
        """Reconnect to OANDA"""
        await self.disconnect()
        await asyncio.sleep(2)
        return await self.connect()

    async def subscribe(self, symbol: str):
        """
        Subscribe to a symbol
        Note: OANDA requires restarting the stream with new instrument list

        Args:
            symbol: OANDA format symbol (e.g., EUR_USD, SPX500_USD)
        """
        if not self.connected:
            raise Exception("Not connected to OANDA")

        self.subscribed_symbols.add(symbol)
        logger.info(f"Subscribed to {symbol} on OANDA")

        # If already streaming, need to restart with updated symbols
        if self.streaming:
            logger.info("Restarting stream with updated symbol list")
            await self._restart_stream()

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        self.subscribed_symbols.discard(symbol)

        # Restart stream if actively streaming
        if self.streaming:
            await self._restart_stream()

    async def bulk_subscribe(self, symbols: list):
        """Subscribe to multiple symbols at once"""
        for symbol in symbols:
            self.subscribed_symbols.add(symbol)

        logger.info(f"Bulk subscribed to {len(symbols)} symbols on OANDA")

        # Restart stream if needed
        if self.streaming:
            await self._restart_stream()

    async def _restart_stream(self):
        """Restart the stream with current symbol list"""
        was_streaming = self.streaming
        self.streaming = False

        if self.stream_response:
            self.stream_response.close()
            self.stream_response = None

        await asyncio.sleep(0.5)

        if was_streaming:
            self.streaming = True

    async def stream_prices(self) -> AsyncIterator[Tuple[str, Dict]]:
        """
        Stream price updates from OANDA

        Yields:
            Tuple of (symbol, price_data) on each update
        """
        if not self.connected:
            raise Exception("Not connected to OANDA")

        if not self.subscribed_symbols:
            logger.warning("No symbols subscribed, waiting...")
            await asyncio.sleep(5)
            return

        self.streaming = True

        while self.streaming:
            try:
                # Build instrument list
                instruments = ','.join(self.subscribed_symbols)
                params = {'instruments': instruments}

                # Open streaming connection
                async with self.session.get(self.stream_url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"OANDA stream failed: {response.status}")
                        await asyncio.sleep(5)
                        continue

                    self.stream_response = response

                    # Read stream line by line
                    async for line in response.content:
                        if not self.streaming:
                            break

                        # Parse JSON line
                        try:
                            data = json.loads(line)

                            # Check message type
                            if data.get('type') == 'PRICE':
                                # Extract price data
                                symbol = data['instrument']

                                # Get best bid/ask
                                bids = data.get('bids', [])
                                asks = data.get('asks', [])

                                if bids and asks:
                                    bid = float(bids[0]['price'])
                                    ask = float(asks[0]['price'])

                                    price_data = {
                                        'bid': bid,
                                        'ask': ask,
                                        'timestamp': datetime.fromtimestamp(float(data['time'])),
                                        'tradeable': data.get('tradeable', True)
                                    }

                                    yield symbol, price_data

                            elif data.get('type') == 'HEARTBEAT':
                                # Keep-alive message
                                logger.debug("OANDA heartbeat received")
                                continue

                        except json.JSONDecodeError:
                            # Skip invalid JSON
                            continue
                        except Exception as e:
                            logger.error(f"Error parsing OANDA message: {e}")
                            continue

            except aiohttp.ClientError as e:
                logger.error(f"OANDA stream connection error: {e}")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error in OANDA stream: {e}")
                await asyncio.sleep(5)

    def get_subscribed_symbols(self) -> Set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()