"""Price Stream Manager — coordinates streaming price feeds."""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Optional

from price_feeds.feeds.binance_stream import BinanceStream
from price_feeds.feeds.icmarkets_stream import ICMarketsStream
from price_feeds.feeds.oanda_stream import OANDAStream
from price_feeds.config.symbol_mapper import SymbolMapper

logger = logging.getLogger(__name__)

# Hard ceiling on a single feed reconnect (teardown + respawn + resubscribe).
RECONNECT_TIMEOUT_SECONDS = 45

# A feed can be legitimately unreachable for a long stretch (weekend, broker
# maintenance). Retries back off to a ceiling low enough that the reopen is
# picked up promptly.
RECONNECT_BACKOFF_START_SECONDS = 5
RECONNECT_BACKOFF_MAX_SECONDS = 60


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
        self.feeds: dict[str, any] = {}
        self.feed_status: dict[str, bool] = {}

        # Symbol tracking
        self.subscribed_symbols: set[str] = set()
        self.symbol_to_feed: dict[str, str] = {}  # Maps symbol to feed name

        # Price storage (latest prices only)
        self.latest_prices: dict[str, dict] = {}
        self.price_lock = asyncio.Lock()

        # Subscribers (callbacks to notify on price updates)
        self.subscribers: list[Callable] = []

        # Start health monitor
        self.health_monitor = None

        # Statistics
        self.stats = {
            "updates_received": 0,
            "updates_distributed": 0,
            "reconnections": 0,
            "errors": 0,
        }

        logger.debug("PriceStreamManager initialized")

    async def initialize(self):
        """Initialize all streaming feeds"""
        logger.debug("Initializing streaming feeds...")

        # Initialize MT5 stream
        try:
            self.feeds["icmarkets"] = ICMarketsStream()
            await self.feeds["icmarkets"].connect()
            self.feed_status["icmarkets"] = True

            # Start MT5 stream handler
            asyncio.create_task(self._run_feed_stream("icmarkets"))
            logger.debug("ICMarkets stream initialized")
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
                asyncio.create_task(self._run_feed_stream("oanda"))

                # Log which server we're connected to
                server_type = "practice" if practice else "live"
                logger.debug(f"OANDA stream initialized ({server_type} account)")
            else:
                logger.debug("OANDA credentials not configured, skipping")
        except Exception as e:
            logger.error(f"Failed to initialize OANDA stream: {e}")
            self.feed_status["oanda"] = False

        # Initialize Binance WebSocket
        try:
            self.feeds["binance"] = BinanceStream()
            await self.feeds["binance"].connect()
            self.feed_status["binance"] = True

            # Start Binance stream handler
            asyncio.create_task(self._run_feed_stream("binance"))
            logger.debug("Binance WebSocket initialized")
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

                asyncio.create_task(self._run_feed_stream("exness"))
                logger.debug("Exness MT5 stream initialized")
            else:
                logger.debug("Exness MT5 path not configured, skipping")
        except Exception as e:
            logger.error(f"Failed to initialize Exness stream: {e}")
            self.feed_status["exness"] = False

        connected = [name for name, status in self.feed_status.items() if status]
        logger.info(
            "Feeds connected: %d/%d (%s)",
            len(connected),
            len(self.feed_status),
            ", ".join(connected) or "none",
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
            logger.debug(f"Subscribed to {symbol} via {feed_name} (as {feed_symbol})")
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
                logger.debug(f"Unsubscribed from {symbol}")
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

    async def bulk_subscribe(self, symbols: list[str]):
        """
        Subscribe to multiple symbols at once

        Args:
            symbols: List of internal format symbols
        """
        logger.debug(f"Bulk subscribing to {len(symbols)} symbols")

        # Group by feed
        feed_symbols: dict[str, list[tuple]] = defaultdict(list)

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
                for internal, _feed_sym in symbol_pairs:
                    self.subscribed_symbols.add(internal)
                    self.symbol_to_feed[internal] = feed_name

                logger.debug(f"Bulk subscribed {len(symbol_pairs)} symbols to {feed_name}")
            except Exception as e:
                logger.error(f"Failed to bulk subscribe to {feed_name}: {e}")

    def add_subscriber(self, callback: Callable):
        """
        Add a callback to be notified of price updates

        Args:
            callback: Async function(symbol, price_data) to call on updates
        """
        self.subscribers.append(callback)
        logger.debug(f"Added subscriber: {callback.__name__}")

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
        logger.debug("Health monitor connected to stream manager")

        # Wire the MT5 stream's per-poll callback into the health monitor so
        # quiet ticks (no bid/ask change) still refresh last_seen.
        ic_feed = self.feeds.get("icmarkets")
        if ic_feed is not None:
            def _mark_icmarkets_seen(mt5_symbol: str):
                internal = self.symbol_mapper.get_internal_symbol(mt5_symbol, "icmarkets")
                if internal:
                    health_monitor.update_last_seen(internal, "icmarkets")

            ic_feed.on_poll = _mark_icmarkets_seen

    async def _run_feed_stream(self, feed_name: str):
        """Consume a feed's price stream, reconnecting with backoff on failure.

        Only the first failure of an outage is logged at WARNING: a feed that is
        down for a whole weekend would otherwise repeat the same error every few
        seconds for two days.
        """
        feed = self.feeds[feed_name]
        failures = 0

        while True:
            try:
                async for symbol, price_data in feed.stream_prices():
                    if failures:
                        logger.info("%s feed recovered", feed_name)
                        failures = 0

                    internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, feed_name)
                    if internal_symbol:
                        await self._process_price_update(internal_symbol, price_data, feed_name)
            except Exception as e:
                self.stats["errors"] += 1
                failures += 1
                if failures == 1:
                    logger.warning("%s stream error: %s", feed_name, e)
                else:
                    logger.debug("%s stream error (attempt %d): %s", feed_name, failures, e)
            else:
                # Stream ended without an error — nothing subscribed to it yet.
                await asyncio.sleep(1)
                continue

            backoff = min(
                RECONNECT_BACKOFF_START_SECONDS * 2 ** (failures - 1),
                RECONNECT_BACKOFF_MAX_SECONDS,
            )
            await asyncio.sleep(backoff)

            try:
                await asyncio.wait_for(feed.reconnect(), timeout=RECONNECT_TIMEOUT_SECONDS)
                self.stats["reconnections"] += 1
            except Exception as e:
                logger.debug("%s reconnect failed: %s", feed_name, e)

    async def _process_price_update(self, symbol: str, price_data: dict, feed: str):
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

    async def get_latest_price(self, symbol: str) -> Optional[dict]:
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
        logger.debug("Shutting down streaming feeds...")

        for feed_name, feed in self.feeds.items():
            try:
                await feed.disconnect()
                logger.debug(f"Disconnected {feed_name}")
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
            logger.debug(f"Reconnecting {feed_name}...")
            # Bound the reconnect: a feed teardown/respawn that hangs (e.g. a
            # stuck MT5 child-process spawn) must fail, not wedge the caller.
            await asyncio.wait_for(feed.reconnect(), timeout=RECONNECT_TIMEOUT_SECONDS)
            self.feed_status[feed_name] = feed.connected
            self.stats["reconnections"] += 1
            if feed.connected:
                logger.debug(f"{feed_name} reconnected")
            else:
                logger.warning(f"{feed_name} reconnect did not restore connection")
            return feed.connected
        except asyncio.TimeoutError:
            logger.error(
                f"{feed_name} reconnect timed out after {RECONNECT_TIMEOUT_SECONDS}s"
            )
            self.feed_status[feed_name] = False
            return False
        except Exception as e:
            logger.error(f"Failed to reconnect {feed_name}: {e}")
            self.feed_status[feed_name] = False
            return False

    async def reconnect_all(self):
        """Reconnect all streaming feeds"""
        logger.info("Reconnecting all feeds")

        reconnect_results = {}

        for feed_name, feed in self.feeds.items():
            try:
                await feed.reconnect()
                self.feed_status[feed_name] = feed.connected
                reconnect_results[feed_name] = True
                self.stats["reconnections"] += 1
            except Exception as e:
                logger.error(f"Failed to reconnect {feed_name}: {e}")
                self.feed_status[feed_name] = False
                reconnect_results[feed_name] = False

        connected_count = sum(1 for success in reconnect_results.values() if success)
        logger.info("Feeds reconnected: %d/%d", connected_count, len(reconnect_results))

        return reconnect_results

    def get_stats(self) -> dict:
        """Get streaming statistics"""
        return {
            **self.stats,
            "subscribed_symbols": len(self.subscribed_symbols),
            "connected_feeds": sum(1 for status in self.feed_status.values() if status),
            "total_feeds": len(self.feed_status),
            "subscribers": len(self.subscribers),
        }
