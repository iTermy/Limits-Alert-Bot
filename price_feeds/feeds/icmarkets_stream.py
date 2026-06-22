"""
ICMarkets/MT5 Streaming Feed
Uses continuous tick polling with asyncio for real-time price updates
"""

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from time import monotonic
from typing import Callable, Dict, Optional, Set, Tuple

import MetaTrader5 as mt5

logger = logging.getLogger(__name__)

# Reference-only symbols (subscribed at startup purely so live_prices.ic_bid/ic_ask
# can be stamped for the execution bot's broker-offset calc) are polled at this
# slower cadence instead of the 100 ms used for signal-bearing symbols.
_REFERENCE_POLL_INTERVAL = 15 * 60  # seconds


class ICMarketsStream:
    """
    MT5 streaming price feed

    MT5 doesn't have native WebSocket streaming, so we use continuous polling
    with symbol_info_tick() in a tight loop (runs every 100ms per symbol).
    Reference-only symbols (see subscribe_reference) poll at a 15-minute
    cadence since their only consumer is the broker-offset stamp.
    """

    def __init__(self):
        """Initialize MT5 stream"""
        self.connected = False
        self.subscribed_symbols: Set[str] = set()

        # Symbols subscribed only for ic_bid/ic_ask reference data — polled at
        # _REFERENCE_POLL_INTERVAL unless they're also in subscribed_symbols
        # (in which case they're signal-bearing and poll at 100 ms).
        self.reference_symbols: Set[str] = set()

        # monotonic timestamp of the next allowed poll per reference symbol.
        self._reference_next_poll: Dict[str, float] = {}

        # Resolved MT5 name per requested stock "-24" symbol. Some stocks have
        # no 24-hour variant on this broker, so we fall back to the bare symbol
        # and cache the result to avoid re-probing on every subscribe.
        self._stock_symbol_cache: Dict[str, str] = {}

        # Price cache to detect changes
        self.last_prices: Dict[str, Dict] = {}

        # Stream control
        self.streaming = False
        self.stream_task = None

        # Optional callback invoked on every successful MT5 poll, regardless of
        # whether the price changed. Used by the health monitor to refresh its
        # last_seen timer so quiet periods (spread widening, illiquid windows)
        # don't get misread as a stale feed. Argument is the MT5-format symbol.
        self.on_poll: Optional[Callable[[str], None]] = None

        logger.info("ICMarketsStream initialized")

    async def connect(self) -> bool:
        """Initialize MT5 connection"""
        try:
            loop = asyncio.get_event_loop()
            mt5_path = os.getenv("MT5_PATH")
            if mt5_path:
                result = await loop.run_in_executor(None, lambda: mt5.initialize(mt5_path))
            else:
                result = await loop.run_in_executor(None, mt5.initialize)

            if result:
                self.connected = True

                terminal_info = mt5.terminal_info()
                if terminal_info:
                    logger.info(f"Connected to MT5 - {terminal_info.name}")

                return True
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
        Subscribe to price updates for a symbol (signal-bearing, 100 ms cadence).

        If the symbol was previously a reference-only subscription, this
        promotes it: the slow-poll gate is cleared so the next loop iteration
        polls immediately and stays at 100 ms.

        Args:
            symbol: MT5 format symbol (e.g., EURUSD, XAUUSD)
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        if symbol not in self.reference_symbols:
            # Resolve the 24-hour stock fallback before validating, so a stock
            # without a "-24" variant subscribes to its bare symbol instead.
            symbol = await self._resolve_stock_symbol(symbol)

            # Validate symbol exists. Reference symbols are already validated
            # at subscribe_reference time, so skip here for the promotion case.
            loop = asyncio.get_event_loop()
            symbol_info = await loop.run_in_executor(None, mt5.symbol_info, symbol)

            if symbol_info is None:
                raise Exception(f"Symbol {symbol} not found in MT5")

            if not symbol_info.visible:
                await loop.run_in_executor(None, mt5.symbol_select, symbol, True)

        self.subscribed_symbols.add(symbol)
        # Promote a reference symbol to signal-bearing cadence immediately.
        self._reference_next_poll.pop(symbol, None)
        logger.info(f"Subscribed to {symbol} on MT5")

    async def _resolve_stock_symbol(self, symbol: str) -> str:
        """
        Resolve a stock "-24" symbol to a name that exists on this broker.

        Most stocks expose a 24-hour feed (e.g. AMD.NAS-24), but some only
        have the bare symbol (AMD.NAS). When the "-24" variant is missing,
        fall back to the bare symbol. The result is cached so each symbol is
        probed at most once.
        """
        if not symbol.endswith("-24"):
            return symbol

        if symbol in self._stock_symbol_cache:
            return self._stock_symbol_cache[symbol]

        loop = asyncio.get_event_loop()
        if await loop.run_in_executor(None, mt5.symbol_info, symbol) is not None:
            self._stock_symbol_cache[symbol] = symbol
            return symbol

        bare = symbol[:-3]
        if await loop.run_in_executor(None, mt5.symbol_info, bare) is not None:
            logger.info("Stock %s has no 24-hour variant; using %s", symbol, bare)
            self._stock_symbol_cache[symbol] = bare
            return bare

        # Neither exists — keep the original so the normal not-found error fires.
        self._stock_symbol_cache[symbol] = symbol
        return symbol

    async def subscribe_reference(self, symbol: str):
        """
        Subscribe a symbol purely as reference data (15-minute polling cadence).

        Used for live_prices.ic_bid/ic_ask stamping — the offset between IC
        and the signal feed (OANDA/Binance) drifts slowly enough that minute-
        scale refresh is sufficient. Signal-bearing subscriptions go via
        subscribe() and override the slow cadence.
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        loop = asyncio.get_event_loop()
        symbol_info = await loop.run_in_executor(None, mt5.symbol_info, symbol)

        if symbol_info is None:
            raise Exception(f"Symbol {symbol} not found in MT5")

        if not symbol_info.visible:
            await loop.run_in_executor(None, mt5.symbol_select, symbol, True)

        self.reference_symbols.add(symbol)
        # Poll immediately on the first loop iteration so ic_bid/ic_ask is
        # populated quickly after startup, then drop to the slow cadence.
        self._reference_next_poll[symbol] = 0.0
        logger.info(f"Subscribed reference symbol {symbol} on MT5 (15 min cadence)")

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        # Resolve through the stock fallback cache so we discard the name that
        # was actually subscribed (bare symbol when "-24" didn't exist).
        symbol = self._stock_symbol_cache.get(symbol, symbol)
        self.subscribed_symbols.discard(symbol)
        # If the symbol was *also* a reference subscription it stays subscribed
        # at the slow cadence; only drop last_prices when it's gone from both.
        if symbol not in self.reference_symbols:
            self.last_prices.pop(symbol, None)
        else:
            # Reset the next-poll timer so it's not held to the fast schedule.
            self._reference_next_poll[symbol] = monotonic() + _REFERENCE_POLL_INTERVAL

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
                now_mono = monotonic()
                # Poll every signal-bearing symbol every iteration; reference-
                # only symbols only when their per-symbol next-poll time has
                # elapsed (slow cadence).
                symbols_to_poll = set(self.subscribed_symbols)
                for ref_sym in list(self.reference_symbols):
                    if ref_sym in self.subscribed_symbols:
                        continue
                    if self._reference_next_poll.get(ref_sym, 0.0) <= now_mono:
                        symbols_to_poll.add(ref_sym)

                for symbol in symbols_to_poll:
                    # Fetch current tick
                    tick = await loop.run_in_executor(None, mt5.symbol_info_tick, symbol)

                    is_reference_only = (
                        symbol in self.reference_symbols
                        and symbol not in self.subscribed_symbols
                    )
                    if is_reference_only:
                        self._reference_next_poll[symbol] = now_mono + _REFERENCE_POLL_INTERVAL

                    if tick is None:
                        continue

                    # Refresh health-monitor liveness on every successful poll,
                    # even when bid/ask are unchanged — quiet markets still mean
                    # the feed is alive. Reference-only symbols skip this so the
                    # 15-minute cadence doesn't trip the 5-minute stale check.
                    if self.on_poll is not None and not is_reference_only:
                        try:
                            self.on_poll(symbol)
                        except Exception as cb_err:
                            logger.debug("on_poll callback error for %s: %s", symbol, cb_err)

                    # Build price data
                    current_price = {
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "timestamp": datetime.fromtimestamp(tick.time, tz=timezone.utc),
                        "last": tick.last,
                        "volume": tick.volume,
                    }

                    # Check if price changed
                    if symbol not in self.last_prices:
                        # First time seeing this symbol
                        self.last_prices[symbol] = current_price
                        if not is_reference_only:
                            yield symbol, current_price
                    else:
                        # Check if bid or ask changed
                        last = self.last_prices[symbol]
                        if (
                            last["bid"] != current_price["bid"]
                            or last["ask"] != current_price["ask"]
                        ):
                            self.last_prices[symbol] = current_price
                            if not is_reference_only:
                                yield symbol, current_price

                # Small delay between checks (100ms = 10 updates/sec max per symbol)
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in MT5 stream: {e}")
                await asyncio.sleep(1)

    def get_subscribed_symbols(self) -> Set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()
