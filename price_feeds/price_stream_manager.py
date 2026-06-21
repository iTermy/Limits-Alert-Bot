"""Price Stream Manager — coordinates streaming price feeds."""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

from price_feeds.feeds.binance_stream import BinanceStream
from price_feeds.feeds.icmarkets_stream import ICMarketsStream
from price_feeds.feeds.oanda_stream import OANDAStream
from price_feeds.symbol_mapper import SymbolMapper

logger = logging.getLogger(__name__)

# ICMarkets symbols subscribed unconditionally at startup so that last_prices
# always has reference prices for the execution-bot offset calculation,
# independent of which signals happen to be active.
_IC_REFERENCE_SYMBOLS = ["XTIUSD", "US500", "USTEC", "US30", "JP225", "DE40", "BTCUSD", "ETHUSD"]


class PriceStreamManager:
    """
    Manages streaming price feeds and distributes updates to subscribers

    Features:
    - Automatic feed selection based on symbol
    - Reconnection handling for all feeds
    - Price update broadcasting to multiple subscribers
    - Symbol subscription management
    - Automatic spread calculation
    """

    def __init__(self):
        """Initialize the stream manager"""
        self.symbol_mapper = SymbolMapper()

        # Initialize streaming feeds
        self.feeds: Dict[str, any] = {}
        self.feed_status: Dict[str, bool] = {}

        # Symbol tracking
        self.subscribed_symbols: Set[str] = set()
        self.symbol_to_feed: Dict[str, str] = {}  # Maps symbol to feed name

        # Price storage (latest prices only)
        self.latest_prices: Dict[str, Dict] = {}
        self.price_lock = asyncio.Lock()

        # Subscribers (callbacks to notify on price updates)
        self.subscribers: List[Callable] = []

        # Start health monitor
        self.health_monitor = None

        # Statistics
        self.stats = {
            "updates_received": 0,
            "updates_distributed": 0,
            "reconnections": 0,
            "errors": 0,
        }

        logger.info("PriceStreamManager initialized")

    async def initialize(self):
        """Initialize all streaming feeds"""
        logger.info("Initializing streaming feeds...")

        # Initialize MT5 stream
        try:
            self.feeds["icmarkets"] = ICMarketsStream()
            await self.feeds["icmarkets"].connect()
            self.feed_status["icmarkets"] = True

            # Subscribe reference symbols needed for execution-bot offset calc.
            # These are polled at a 15-minute cadence by ICMarketsStream since
            # the offset they feed only drifts on minute timescales.
            for sym in _IC_REFERENCE_SYMBOLS:
                try:
                    await self.feeds["icmarkets"].subscribe_reference(sym)
                except Exception as e:
                    logger.warning("Could not subscribe IC reference symbol %s: %s", sym, e)

            # Start MT5 stream handler
            asyncio.create_task(self._handle_icmarkets_stream())
            logger.info("✓ ICMarkets stream initialized")
        except Exception as e:
            logger.error(f"Failed to initialize ICMarkets stream: {e}")
            self.feed_status["icmarkets"] = False

        # Initialize OANDA stream
        try:
            import os

            if os.getenv("OANDA_API_KEY") and os.getenv("OANDA_ACCOUNT_ID"):
                # Check if using practice account
                practice = os.getenv("OANDA_PRACTICE", "false").lower() == "true"

                logger.debug(f"Initializing OANDA with practice={practice}")

                # Initialize with practice flag
                self.feeds["oanda"] = OANDAStream(practice=practice)
                await self.feeds["oanda"].connect()
                self.feed_status["oanda"] = True

                # Start OANDA stream handler
                asyncio.create_task(self._handle_oanda_stream())

                # Log which server we're connected to
                server_type = "practice" if practice else "live"
                logger.info(f"✓ OANDA stream initialized ({server_type} account)")
            else:
                logger.info("OANDA credentials not configured, skipping")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA stream: {e}")
            self.feed_status["oanda"] = False

        # Initialize Binance WebSocket
        try:
            self.feeds["binance"] = BinanceStream()
            await self.feeds["binance"].connect()
            self.feed_status["binance"] = True

            # Start Binance stream handler
            asyncio.create_task(self._handle_binance_stream())
            logger.info("✓ Binance WebSocket initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Binance stream: {e}")
            self.feed_status["binance"] = False

        # Initialize Exness MT5 stream (oil)
        try:
            import os

            if os.getenv("EXNESS_MT5_PATH"):
                from price_feeds.feeds.exness_stream import ExnessStream

                self.feeds["exness"] = ExnessStream()
                await self.feeds["exness"].connect()
                self.feed_status["exness"] = True

                asyncio.create_task(self._handle_exness_stream())
                logger.info("Exness MT5 stream initialized")
            else:
                logger.info("Exness MT5 path not configured, skipping")
        except Exception as e:
            logger.error(f"Failed to initialize Exness stream: {e}")
            self.feed_status["exness"] = False

        connected = sum(1 for status in self.feed_status.values() if status)
        logger.info(
            f"Stream initialization complete: {connected}/{len(self.feed_status)} feeds connected"
        )

    async def subscribe_symbol(self, symbol: str):
        """
        Subscribe to price updates for a symbol

        Args:
            symbol: Internal format symbol
        """
        if symbol in self.subscribed_symbols:
            logger.debug(f"Already subscribed to {symbol}")
            return

        # Determine which feed to use
        feed_name = self.symbol_mapper.get_best_feed(symbol)
        feed_symbol = self.symbol_mapper.get_feed_symbol(symbol, feed_name)

        if not feed_symbol:
            logger.error(f"Cannot map {symbol} to any feed")
            return

        if feed_name not in self.feeds or not self.feed_status.get(feed_name):
            logger.error(f"Feed {feed_name} not available for {symbol}")
            return

        # Subscribe to the feed
        try:
            await self.feeds[feed_name].subscribe(feed_symbol)
            self.subscribed_symbols.add(symbol)
            self.symbol_to_feed[symbol] = feed_name
            logger.info(f"Subscribed to {symbol} via {feed_name} (as {feed_symbol})")
        except Exception as e:
            logger.error(f"Failed to subscribe to {symbol}: {e}")

    async def unsubscribe_symbol(self, symbol: str):
        """
        Unsubscribe from price updates for a symbol

        Args:
            symbol: Internal format symbol
        """
        if symbol not in self.subscribed_symbols:
            return

        feed_name = self.symbol_to_feed.get(symbol)
        if feed_name and feed_name in self.feeds:
            feed_symbol = self.symbol_mapper.get_feed_symbol(symbol, feed_name)
            try:
                await self.feeds[feed_name].unsubscribe(feed_symbol)
                logger.info(f"Unsubscribed from {symbol}")
            except Exception as e:
                logger.error(f"Failed to unsubscribe from {symbol}: {e}")

        self.subscribed_symbols.discard(symbol)
        self.symbol_to_feed.pop(symbol, None)

        async with self.price_lock:
            self.latest_prices.pop(symbol, None)

        # Remove from health monitor tracking so stale entries don't
        # trigger false feed-down alerts after the symbol is unsubscribed
        if self.health_monitor:
            self.health_monitor.clear_symbol(symbol)

    async def bulk_subscribe(self, symbols: List[str]):
        """
        Subscribe to multiple symbols at once

        Args:
            symbols: List of internal format symbols
        """
        logger.info(f"Bulk subscribing to {len(symbols)} symbols")

        # Group by feed
        feed_symbols: Dict[str, List[tuple]] = defaultdict(list)

        for symbol in symbols:
            if symbol in self.subscribed_symbols:
                continue

            feed_name = self.symbol_mapper.get_best_feed(symbol)
            feed_symbol = self.symbol_mapper.get_feed_symbol(symbol, feed_name)

            if feed_symbol and feed_name in self.feeds and self.feed_status.get(feed_name):
                feed_symbols[feed_name].append((symbol, feed_symbol))
            else:
                logger.warning(
                    "Cannot subscribe %s: feed=%s feed_symbol=%s in_feeds=%s status=%s",
                    symbol, feed_name, feed_symbol,
                    feed_name in self.feeds, self.feed_status.get(feed_name),
                )

        # Subscribe to each feed
        for feed_name, symbol_pairs in feed_symbols.items():
            try:
                feed_syms = [fs for _, fs in symbol_pairs]
                await self.feeds[feed_name].bulk_subscribe(feed_syms)

                # Track subscriptions
                for internal, feed_sym in symbol_pairs:
                    self.subscribed_symbols.add(internal)
                    self.symbol_to_feed[internal] = feed_name

                logger.info(f"Bulk subscribed {len(symbol_pairs)} symbols to {feed_name}")
            except Exception as e:
                logger.error(f"Failed to bulk subscribe to {feed_name}: {e}")

    def add_subscriber(self, callback: Callable):
        """
        Add a callback to be notified of price updates

        Args:
            callback: Async function(symbol, price_data) to call on updates
        """
        self.subscribers.append(callback)
        logger.info(f"Added subscriber: {callback.__name__}")

    def remove_subscriber(self, callback: Callable):
        """Remove a subscriber callback"""
        if callback in self.subscribers:
            self.subscribers.remove(callback)

    def set_health_monitor(self, health_monitor):
        """
        Set the health monitor reference

        Args:
            health_monitor: FeedHealthMonitor instance
        """
        self.health_monitor = health_monitor
        logger.info("Health monitor connected to stream manager")

        # Wire the MT5 stream's per-poll callback into the health monitor so
        # quiet ticks (no bid/ask change) still refresh last_seen.
        ic_feed = self.feeds.get("icmarkets")
        if ic_feed is not None:
            def _mark_icmarkets_seen(mt5_symbol: str):
                internal = self.symbol_mapper.get_internal_symbol(mt5_symbol, "icmarkets")
                if internal:
                    health_monitor.update_last_seen(internal, "icmarkets")

            ic_feed.on_poll = _mark_icmarkets_seen

    async def _handle_icmarkets_stream(self):
        """Handle MT5 price stream"""
        feed = self.feeds["icmarkets"]

        while True:
            try:
                async for symbol, price_data in feed.stream_prices():
                    # Convert feed symbol back to internal format
                    internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, "icmarkets")

                    if internal_symbol:
                        await self._process_price_update(internal_symbol, price_data, "icmarkets")
            except Exception as e:
                logger.error(f"ICMarkets stream error: {e}")
                self.stats["errors"] += 1

                # Reconnect
                await asyncio.sleep(5)
                try:
                    await feed.reconnect()
                    self.stats["reconnections"] += 1
                except Exception as e2:
                    logger.error(f"ICMarkets reconnection failed: {e2}")
                    await asyncio.sleep(30)

    async def _handle_oanda_stream(self):
        """Handle OANDA price stream"""
        feed = self.feeds["oanda"]

        while True:
            try:
                async for symbol, price_data in feed.stream_prices():
                    # Convert feed symbol back to internal format
                    internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, "oanda")

                    if internal_symbol:
                        await self._process_price_update(internal_symbol, price_data, "oanda")
            except Exception as e:
                logger.error(f"OANDA stream error: {e}")
                self.stats["errors"] += 1

                # Reconnect
                await asyncio.sleep(5)
                try:
                    await feed.reconnect()
                    self.stats["reconnections"] += 1
                except Exception as e2:
                    logger.error(f"OANDA reconnection failed: {e2}")
                    await asyncio.sleep(30)

    async def _handle_binance_stream(self):
        """Handle Binance WebSocket stream"""
        feed = self.feeds["binance"]

        while True:
            try:
                async for symbol, price_data in feed.stream_prices():
                    # Convert feed symbol back to internal format
                    internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, "binance")

                    if internal_symbol:
                        await self._process_price_update(internal_symbol, price_data, "binance")
            except Exception as e:
                logger.error(f"Binance stream error: {e}")
                self.stats["errors"] += 1

                # Reconnect
                await asyncio.sleep(5)
                try:
                    await feed.reconnect()
                    self.stats["reconnections"] += 1
                except Exception as e2:
                    logger.error(f"Binance reconnection failed: {e2}")
                    await asyncio.sleep(30)

    async def _handle_exness_stream(self):
        """Handle Exness MT5 price stream (oil symbols)"""
        feed = self.feeds["exness"]

        while True:
            try:
                async for symbol, price_data in feed.stream_prices():
                    internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, "exness")

                    if internal_symbol:
                        await self._process_price_update(internal_symbol, price_data, "exness")
            except Exception as e:
                logger.error(f"Exness stream error: {e}")
                self.stats["errors"] += 1

                await asyncio.sleep(5)
                try:
                    await feed.reconnect()
                    self.stats["reconnections"] += 1
                except Exception as e2:
                    logger.error(f"Exness reconnection failed: {e2}")
                    await asyncio.sleep(30)

    async def _process_price_update(self, symbol: str, price_data: Dict, feed: str):
        """Process a price update, compute spread if absent, and notify subscribers."""
        # Calculate spread if not already present
        if "spread" not in price_data and "bid" in price_data and "ask" in price_data:
            try:
                spread = price_data["ask"] - price_data["bid"]
                # Validate spread is non-negative
                if spread < 0:
                    logger.warning(f"Negative spread detected for {symbol}: {spread}, using 0")
                    spread = 0.0
                price_data["spread"] = spread
            except (TypeError, KeyError) as e:
                logger.warning(f"Could not calculate spread for {symbol}: {e}, using 0")
                price_data["spread"] = 0.0

        # Update health monitor
        if self.health_monitor:
            self.health_monitor.update_last_seen(symbol, feed)

        # Stamp updated_at: use broker tick time (ICMarkets) or current wall-clock.
        # downstream staleness gate in streaming_monitor uses this field.
        if "updated_at" not in price_data:
            ts = price_data.get("timestamp")
            if isinstance(ts, datetime) and ts.tzinfo is not None:
                price_data["updated_at"] = ts
            else:
                price_data["updated_at"] = datetime.now(timezone.utc)

        # Store latest price
        async with self.price_lock:
            self.latest_prices[symbol] = {**price_data, "feed": feed, "received_at": datetime.now()}

        self.stats["updates_received"] += 1

        # Notify all subscribers
        for subscriber in self.subscribers:
            try:
                await subscriber(symbol, price_data)
                self.stats["updates_distributed"] += 1
            except Exception as e:
                logger.error(f"Subscriber {subscriber.__name__} failed: {e}")

    async def get_latest_price(self, symbol: str) -> Optional[Dict]:
        """
        Get the latest cached price for a symbol

        This method checks if we have a recent price update for the symbol.
        If the symbol is subscribed but we don't have a price yet, it will
        wait a short time for an update.

        Args:
            symbol: Internal format symbol

        Returns:
            Latest price data or None
        """
        # Check if we already have a price
        async with self.price_lock:
            if symbol in self.latest_prices:
                return self.latest_prices[symbol].copy()

        # If symbol is subscribed but no price yet, wait briefly for an update
        if symbol in self.subscribed_symbols:
            logger.debug(f"Waiting for price update for subscribed symbol: {symbol}")

            # Wait up to 2 seconds for a price update
            for _ in range(20):  # 20 * 0.1s = 2 seconds
                await asyncio.sleep(0.1)
                async with self.price_lock:
                    if symbol in self.latest_prices:
                        logger.debug(f"Received price for {symbol}")
                        return self.latest_prices[symbol].copy()

            logger.warning(f"Subscribed to {symbol} but no price received after 2 seconds")
        else:
            logger.debug(f"Symbol {symbol} not subscribed")

        return None

    async def shutdown(self):
        """Shutdown all streaming feeds"""
        logger.info("Shutting down streaming feeds...")

        for feed_name, feed in self.feeds.items():
            try:
                await feed.disconnect()
                logger.info(f"Disconnected {feed_name}")
            except Exception as e:
                logger.error(f"Error disconnecting {feed_name}: {e}")

        self.feeds.clear()
        self.subscribed_symbols.clear()
        self.symbol_to_feed.clear()

    async def reconnect_feed(self, feed_name: str) -> bool:
        """Reconnect a single feed without disturbing the others.

        Reconnecting every feed when only one has stalled tears down healthy
        connections (and the MT5 terminal), so the health monitor targets just
        the stale feed.
        """
        feed = self.feeds.get(feed_name)
        if feed is None:
            logger.warning(f"reconnect_feed: unknown feed {feed_name}")
            return False
        try:
            logger.info(f"Reconnecting {feed_name}...")
            await feed.reconnect()
            self.feed_status[feed_name] = feed.connected
            self.stats["reconnections"] += 1
            if feed.connected:
                logger.info(f"✓ {feed_name} reconnected")
            else:
                logger.warning(f"{feed_name} reconnect did not restore connection")
            return feed.connected
        except Exception as e:
            logger.error(f"Failed to reconnect {feed_name}: {e}")
            self.feed_status[feed_name] = False
            return False

    async def reconnect_all(self):
        """Reconnect all streaming feeds"""
        logger.info("Reconnecting all streaming feeds...")

        reconnect_results = {}

        for feed_name, feed in self.feeds.items():
            try:
                logger.info(f"Reconnecting {feed_name}...")
                await feed.reconnect()
                self.feed_status[feed_name] = feed.connected
                reconnect_results[feed_name] = True
                self.stats["reconnections"] += 1
                logger.info(f"✓ {feed_name} reconnected")
            except Exception as e:
                logger.error(f"Failed to reconnect {feed_name}: {e}")
                self.feed_status[feed_name] = False
                reconnect_results[feed_name] = False

        connected_count = sum(1 for success in reconnect_results.values() if success)
        logger.info(
            f"Reconnection complete: {connected_count}/{len(reconnect_results)} feeds connected"
        )

        return reconnect_results

    def get_stats(self) -> Dict:
        """Get streaming statistics"""
        return {
            **self.stats,
            "subscribed_symbols": len(self.subscribed_symbols),
            "connected_feeds": sum(1 for status in self.feed_status.values() if status),
            "total_feeds": len(self.feed_status),
            "subscribers": len(self.subscribers),
        }
