"""
Binance WebSocket Streaming Feed
Uses Binance's WebSocket API for real-time crypto price updates
"""

import asyncio
import aiohttp
import logging
import json
from typing import Dict, Set, AsyncIterator, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class BinanceStream:
    """
    Binance WebSocket streaming feed

    Uses Binance's combined streams for efficient multi-symbol monitoring
    Max 1024 streams can be combined in a single connection
    """

    def __init__(self, use_testnet: bool = False, use_international: bool = False):
        """Initialize Binance stream"""
        import os
        self.use_international = use_international or \
                                 os.getenv('BINANCE_USE_INTERNATIONAL', 'false').lower() == 'true'

        # WebSocket URLs
        if use_testnet:
            self.ws_url = "wss://testnet.binance.vision/stream"
        elif self.use_international:
            self.ws_url = "wss://stream.binance.com:9443/stream"
        else:
            self.ws_url = "wss://stream.binance.us:9443/stream"

        # Connection management
        self.ws_session: aiohttp.ClientSession = None
        self.ws_connection = None
        self.connected = False

        # Symbol tracking
        self.subscribed_symbols: Set[str] = set()

        # Stream control
        self.streaming = False
        self.stream_id = 1

        logger.info(f"BinanceStream initialized ({'international' if self.use_international else 'US'} API)")

    async def connect(self) -> bool:
        """Initialize Binance WebSocket session"""
        try:
            self.ws_session = aiohttp.ClientSession()
            self.connected = True
            logger.info("Binance WebSocket ready")
            return True

        except Exception as e:
            logger.error(f"Error initializing Binance WebSocket: {e}")
            return False

    async def disconnect(self):
        """Close Binance WebSocket"""
        self.streaming = False

        if self.ws_connection:
            await self.ws_connection.close()
            self.ws_connection = None

        if self.ws_session:
            await self.ws_session.close()
            self.ws_session = None

        self.connected = False
        logger.info("Disconnected from Binance WebSocket")

    async def reconnect(self):
        """Reconnect to Binance WebSocket"""
        await self.disconnect()
        await asyncio.sleep(2)
        return await self.connect()

    async def subscribe(self, symbol: str):
        """
        Subscribe to a symbol

        Args:
            symbol: Binance format symbol (e.g., BTCUSDT)
        """
        if not self.connected:
            raise Exception("Not connected to Binance WebSocket")

        self.subscribed_symbols.add(symbol)
        logger.info(f"Subscribed to {symbol} on Binance")

        # If already streaming, send subscribe message
        if self.streaming and self.ws_connection:
            await self._send_subscribe_message([symbol])

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        if symbol in self.subscribed_symbols:
            self.subscribed_symbols.remove(symbol)

            # Send unsubscribe message if actively streaming
            if self.streaming and self.ws_connection:
                await self._send_unsubscribe_message([symbol])

    async def bulk_subscribe(self, symbols: list):
        """Subscribe to multiple symbols at once"""
        for symbol in symbols:
            self.subscribed_symbols.add(symbol)

        logger.info(f"Bulk subscribed to {len(symbols)} symbols on Binance")

        # Send subscribe messages if streaming
        if self.streaming and self.ws_connection:
            await self._send_subscribe_message(symbols)

    async def _send_subscribe_message(self, symbols: list):
        """Send WebSocket subscribe message"""
        # Convert symbols to stream names (lowercase + @bookTicker)
        streams = [f"{symbol.lower()}@bookTicker" for symbol in symbols]

        message = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": self.stream_id
        }
        self.stream_id += 1

        try:
            await self.ws_connection.send_json(message)
            logger.debug(f"Sent subscribe message for {len(symbols)} symbols")
        except Exception as e:
            logger.error(f"Failed to send subscribe message: {e}")

    async def _send_unsubscribe_message(self, symbols: list):
        """Send WebSocket unsubscribe message"""
        streams = [f"{symbol.lower()}@bookTicker" for symbol in symbols]

        message = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": self.stream_id
        }
        self.stream_id += 1

        try:
            await self.ws_connection.send_json(message)
            logger.debug(f"Sent unsubscribe message for {len(symbols)} symbols")
        except Exception as e:
            logger.error(f"Failed to send unsubscribe message: {e}")

    async def stream_prices(self) -> AsyncIterator[Tuple[str, Dict]]:
        """
        Stream price updates from Binance

        Yields:
            Tuple of (symbol, price_data) on each update
        """
        if not self.connected:
            raise Exception("Not connected to Binance WebSocket")

        if not self.subscribed_symbols:
            logger.warning("No symbols subscribed, waiting...")
            await asyncio.sleep(5)
            return

        self.streaming = True

        while self.streaming:
            try:
                # Create WebSocket connection
                async with self.ws_session.ws_connect(self.ws_url) as ws:
                    self.ws_connection = ws

                    # Subscribe to all symbols
                    await self._send_subscribe_message(list(self.subscribed_symbols))

                    # Read messages
                    async for msg in ws:
                        if not self.streaming:
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)

                                # Check if it's a price update (has 'stream' field)
                                if 'stream' in data and 'data' in data:
                                    ticker_data = data['data']

                                    # Extract symbol and prices
                                    symbol = ticker_data['s']  # e.g., BTCUSDT
                                    bid = float(ticker_data['b'])
                                    ask = float(ticker_data['a'])

                                    price_data = {
                                        'bid': bid,
                                        'ask': ask,
                                        'timestamp': datetime.now(),  # Binance doesn't include timestamp in bookTicker
                                        'bid_qty': float(ticker_data.get('B', 0)),
                                        'ask_qty': float(ticker_data.get('A', 0))
                                    }

                                    yield symbol, price_data

                                # Handle subscription confirmations
                                elif 'result' in data:
                                    if data['result'] is None:
                                        logger.debug(f"Subscription confirmed: {data.get('id')}")
                                    else:
                                        logger.warning(f"Subscription response: {data}")

                            except json.JSONDecodeError:
                                logger.warning("Invalid JSON received from Binance")
                                continue
                            except Exception as e:
                                logger.error(f"Error parsing Binance message: {e}")
                                continue

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"Binance WebSocket error: {ws.exception()}")
                            break

            except aiohttp.ClientError as e:
                logger.error(f"Binance WebSocket connection error: {e}")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error in Binance stream: {e}")
                await asyncio.sleep(5)

            finally:
                self.ws_connection = None

    def get_subscribed_symbols(self) -> Set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()