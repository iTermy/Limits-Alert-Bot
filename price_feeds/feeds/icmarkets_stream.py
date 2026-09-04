"""
ICMarkets/MT5 Streaming Feed
Uses continuous tick polling with asyncio for real-time price updates
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import MetaTrader5 as mt5

logger = logging.getLogger(__name__)

# The MetaTrader5 package makes blocking C calls. Running them on a dedicated
# thread pool (rather than asyncio's shared default executor) keeps a hung MT5
# terminal — common during the broker's spread-hour rollover — from starving the
# machinery the rest of the loop depends on (DNS resolution, feed reconnects,
# Discord I/O). A timeout lets the poll loop abandon a wedged sweep and retry.
#
# The whole symbol sweep runs on one hand-off: at ~30 subscribed symbols, a
# hand-off per symbol cost hundreds of thread wake-ups a second and was a
# standing source of event-loop latency for everything else on the loop.
_MT5_EXECUTOR_WORKERS = 4
_MT5_SWEEP_TIMEOUT_SECONDS = 10


class ICMarketsStream:
    """
    MT5 streaming price feed

    MT5 doesn't have native WebSocket streaming, so we use continuous polling
    with symbol_info_tick() in a tight loop (runs every 100ms per symbol).
    """

    def __init__(self):
        """Initialize MT5 stream"""
        self.connected = False
        self.subscribed_symbols: set[str] = set()

        # Resolved MT5 name per requested stock "-24" symbol. Some stocks have
        # no 24-hour variant on this broker, so we fall back to the bare symbol
        # and cache the result to avoid re-probing on every subscribe.
        self._stock_symbol_cache: dict[str, str] = {}

        # Price cache to detect changes
        self.last_prices: dict[str, dict] = {}

        # Stream control
        self.streaming = False
        self.stream_task = None

        # Dedicated pool for MetaTrader5's blocking calls; recreated on each
        # connect so threads leaked to a wedged terminal don't accumulate.
        self._mt5_executor = ThreadPoolExecutor(
            max_workers=_MT5_EXECUTOR_WORKERS, thread_name_prefix="mt5"
        )

        # Optional callback invoked on every successful MT5 poll, regardless of
        # whether the price changed. Used by the health monitor to refresh its
        # last_seen timer so quiet periods (spread widening, illiquid windows)
        # don't get misread as a stale feed. Argument is the MT5-format symbol.
        self.on_poll: Optional[Callable[[str], None]] = None

        logger.debug("ICMarketsStream initialized")

    async def _run_mt5(self, func, *args):
        """Run a blocking MT5 call on the dedicated pool, never on the loop thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._mt5_executor, func, *args)

    def _poll_symbols(self, symbols: list[str]) -> list[tuple[str, Any]]:
        """Fetch the current tick for each symbol. Runs on the MT5 executor.

        Returns (symbol, tick) pairs for the symbols that responded; a symbol
        that errors or has no tick is skipped so one bad symbol can't cost the
        rest of the sweep.
        """
        ticks = []
        for symbol in symbols:
            try:
                tick = mt5.symbol_info_tick(symbol)
            except Exception as e:
                logger.debug("MT5 tick fetch failed for %s: %s", symbol, e)
                continue
            if tick is not None:
                ticks.append((symbol, tick))
        return ticks

    def _reset_mt5_executor(self):
        """Swap in a fresh MT5 pool, abandoning any threads wedged on a hung
        terminal (shutdown(wait=False) does not join them)."""
        self._mt5_executor.shutdown(wait=False)
        self._mt5_executor = ThreadPoolExecutor(
            max_workers=_MT5_EXECUTOR_WORKERS, thread_name_prefix="mt5"
        )

    async def connect(self) -> bool:
        """Initialize MT5 connection"""
        try:
            self._reset_mt5_executor()
            mt5_path = os.getenv("MT5_PATH")
            if mt5_path:
                result = await self._run_mt5(mt5.initialize, mt5_path)
            else:
                result = await self._run_mt5(mt5.initialize)

            if result:
                self.connected = True

                terminal_info = await self._run_mt5(mt5.terminal_info)
                if terminal_info:
                    logger.debug(f"Connected to MT5 - {terminal_info.name}")

                return True
            error = await self._run_mt5(mt5.last_error)
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
            with contextlib.suppress(asyncio.CancelledError):
                await self.stream_task

        if self.connected:
            await self._run_mt5(mt5.shutdown)
            self.connected = False
            logger.debug("Disconnected from MT5")

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
        Subscribe to price updates for a symbol (100 ms polling cadence).

        Args:
            symbol: MT5 format symbol (e.g., EURUSD, XAUUSD)
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        # Resolve the 24-hour stock fallback before validating, so a stock
        # without a "-24" variant subscribes to its bare symbol instead.
        symbol = await self._resolve_stock_symbol(symbol)

        symbol_info = await self._run_mt5(mt5.symbol_info, symbol)

        if symbol_info is None:
            raise Exception(f"Symbol {symbol} not found in MT5")

        if not symbol_info.visible:
            await self._run_mt5(mt5.symbol_select, symbol, True)

        self.subscribed_symbols.add(symbol)
        logger.debug(f"Subscribed to {symbol} on MT5")

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

        if await self._run_mt5(mt5.symbol_info, symbol) is not None:
            self._stock_symbol_cache[symbol] = symbol
            return symbol

        bare = symbol[:-3]
        if await self._run_mt5(mt5.symbol_info, bare) is not None:
            logger.debug("Stock %s has no 24-hour variant; using %s", symbol, bare)
            self._stock_symbol_cache[symbol] = bare
            return bare

        # Neither exists — keep the original so the normal not-found error fires.
        self._stock_symbol_cache[symbol] = symbol
        return symbol

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        # Resolve through the stock fallback cache so we discard the name that
        # was actually subscribed (bare symbol when "-24" didn't exist).
        symbol = self._stock_symbol_cache.get(symbol, symbol)
        self.subscribed_symbols.discard(symbol)
        self.last_prices.pop(symbol, None)

    async def bulk_subscribe(self, symbols: list):
        """Subscribe to multiple symbols"""
        for symbol in symbols:
            try:
                await self.subscribe(symbol)
            except Exception as e:
                logger.error(f"Failed to subscribe to {symbol}: {e}")

    async def stream_prices(self) -> AsyncIterator[tuple[str, dict]]:
        """
        Stream price updates for all subscribed symbols

        Yields:
            Tuple of (symbol, price_data) when price changes
        """
        if not self.connected:
            raise Exception("Not connected to MT5")

        self.streaming = True

        while self.streaming:
            try:
                symbols = sorted(self.subscribed_symbols)
                ticks = []
                if symbols:
                    # Sweep every symbol on one thread hand-off. Bound the sweep
                    # so a wedged terminal (e.g. spread-hour rollover) can't stall
                    # polling indefinitely.
                    try:
                        ticks = await asyncio.wait_for(
                            self._run_mt5(self._poll_symbols, symbols),
                            timeout=_MT5_SWEEP_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "MT5 sweep of %d symbol(s) timed out after %ss; skipping cycle",
                            len(symbols), _MT5_SWEEP_TIMEOUT_SECONDS,
                        )

                for symbol, tick in ticks:
                    # Refresh health-monitor liveness on every successful poll,
                    # even when bid/ask are unchanged — quiet markets still mean
                    # the feed is alive.
                    if self.on_poll is not None:
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
                        yield symbol, current_price
                    else:
                        # Check if bid or ask changed
                        last = self.last_prices[symbol]
                        if (
                            last["bid"] != current_price["bid"]
                            or last["ask"] != current_price["ask"]
                        ):
                            self.last_prices[symbol] = current_price
                            yield symbol, current_price

                # Small delay between checks (100ms = 10 updates/sec max per symbol)
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in MT5 stream: {e}")
                await asyncio.sleep(1)

    def get_subscribed_symbols(self) -> set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()
