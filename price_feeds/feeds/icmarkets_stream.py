"""
ICMarkets/MT5 Streaming Feed
Uses continuous tick polling with asyncio for real-time price updates
"""

import asyncio
import logging
from typing import Dict, Set, AsyncIterator, Tuple
from datetime import datetime
import MetaTrader5 as mt5

logger = logging.getLogger(__name__)


class ICMarketsStream:
    """
    MT5 streaming price feed

    MT5 doesn't have native WebSocket streaming, so we use continuous polling
    with symbol_info_tick() in a tight loop (runs every 100ms per symbol)
    """

    def __init__(self):
        """Initialize MT5 stream"""
        self.connected = False
        self.subscribed_symbols: Set[str] = set()

        # Price cache to detect changes
        self.last_prices: Dict[str, Dict] = {}

        # Stream control
        self.streaming = False
        self.stream_task = None

        logger.info("ICMarketsStream initialized")

    async def connect(self) -> bool:
        """Initialize MT5 connection"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, mt5.initialize)

            if result:
                self.connected = True

                terminal_info = mt5.terminal_info()
                if terminal_info:
                    logger.info(f"Connected to MT5 - {terminal_info.name}")

                return True
            else:
                error = mt5.last_error()
                logger.error(f"MT5 initialization failed: {error}")
                return False

        except Exception as e:
            logger.error(f"Error connecting to MT5: {e}")
            return False

    async def disconnect(self):
        """Shutdown MT5 connection"""
        self.streaming = False

        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                pass

        if self.connected:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, mt5.shutdown)
            self.connected = False
            logger.info("Disconnected from MT5")

    async def reconnect(self):
        """Reconnect to MT5"""
        await self.disconnect()
        await asyncio.sleep(2)
        success = await self.connect()

        if success and self.subscribed_symbols:
            # Re-enable streaming
            self.streaming = True

        return success

    async def subscribe(self, symbol: str):
        """
        Subscribe to price updates for a symbol

        Args:
            symbol: MT5 format symbol (e.g., EURUSD, XAUUSD)
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        # Validate symbol exists
        loop = asyncio.get_event_loop()
        symbol_info = await loop.run_in_executor(None, mt5.symbol_info, symbol)

        if symbol_info is None:
            raise Exception(f"Symbol {symbol} not found in MT5")

        # Enable symbol if not visible
        if not symbol_info.visible:
            await loop.run_in_executor(None, mt5.symbol_select, symbol, True)

        self.subscribed_symbols.add(symbol)
        logger.info(f"Subscribed to {symbol} on MT5")

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        self.subscribed_symbols.discard(symbol)
        self.last_prices.pop(symbol, None)

    async def bulk_subscribe(self, symbols: list):
        """Subscribe to multiple symbols"""
        for symbol in symbols:
            try:
                await self.subscribe(symbol)
            except Exception as e:
                logger.error(f"Failed to subscribe to {symbol}: {e}")

    async def stream_prices(self) -> AsyncIterator[Tuple[str, Dict]]:
        """
        Stream price updates for all subscribed symbols

        Yields:
            Tuple of (symbol, price_data) when price changes
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        self.streaming = True
        loop = asyncio.get_event_loop()

        while self.streaming:
            try:
                # Check each subscribed symbol
                for symbol in list(self.subscribed_symbols):
                    # Fetch current tick
                    tick = await loop.run_in_executor(None, mt5.symbol_info_tick, symbol)

                    if tick is None:
                        continue

                    # Build price data
                    current_price = {
                        'bid': tick.bid,
                        'ask': tick.ask,
                        'timestamp': datetime.fromtimestamp(tick.time),
                        'last': tick.last,
                        'volume': tick.volume
                    }

                    # Check if price changed
                    if symbol not in self.last_prices:
                        # First time seeing this symbol
                        self.last_prices[symbol] = current_price
                        yield symbol, current_price
                    else:
                        # Check if bid or ask changed
                        last = self.last_prices[symbol]
                        if last['bid'] != current_price['bid'] or last['ask'] != current_price['ask']:
                            self.last_prices[symbol] = current_price
                            yield symbol, current_price

                # Small delay between checks (100ms = 10 updates/sec max per symbol)
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in MT5 stream: {e}")
                await asyncio.sleep(1)

    def get_subscribed_symbols(self) -> Set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()