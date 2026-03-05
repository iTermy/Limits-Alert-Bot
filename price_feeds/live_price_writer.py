"""
LivePriceWriter - Batches OANDA/Binance price updates and writes to the
live_prices table in Supabase every WRITE_INTERVAL seconds.

Only OANDA (indices) and Binance (crypto) prices are written.
ICMarkets prices are intentionally excluded.

The execution bot can then read live_prices to calculate the distance
between its MT5 prices and the feed prices used by the alert bot.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Feeds whose prices we want to persist
TRACKED_FEEDS = {'oanda', 'binance'}

# How often to flush the buffer to the DB (seconds)
WRITE_INTERVAL = 5


class LivePriceWriter:
    """
    Subscribes to PriceStreamManager updates, buffers the latest price
    per symbol, and upserts them to the live_prices table every 5 seconds.

    Only symbols served by OANDA or Binance are written.
    """

    def __init__(self, db_manager, stream_manager):
        """
        Args:
            db_manager:     DatabaseManager instance (asyncpg-backed)
            stream_manager: PriceStreamManager instance
        """
        self._db = db_manager
        self._stream = stream_manager

        # Buffer: symbol -> latest price snapshot {bid, ask, feed, updated_at}
        self._buffer: Dict[str, Dict] = {}
        self._buffer_lock = asyncio.Lock()

        self._task: Optional[asyncio.Task] = None
        self._running = False

        logger.info("LivePriceWriter initialised")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        """Start the background flush loop. Call after stream_manager is ready."""
        if self._running:
            return
        self._running = True
        # Register as a subscriber to receive every price tick
        self._stream.add_subscriber(self._on_price_update)
        self._task = asyncio.create_task(self._flush_loop(), name="live_price_writer")
        logger.info("LivePriceWriter started (flush every %ds, feeds: %s)",
                    WRITE_INTERVAL, TRACKED_FEEDS)

    async def stop(self):
        """Graceful shutdown: do a final flush then cancel the loop."""
        self._running = False
        self._stream.remove_subscriber(self._on_price_update)
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # Final flush so we don't lose the last few ticks
        await self._flush_to_db()
        logger.info("LivePriceWriter stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _on_price_update(self, symbol: str, price_data: Dict):
        """
        Callback registered with PriceStreamManager.
        Called on every tick for every subscribed symbol.
        We only buffer ticks coming from tracked feeds.
        """
        feed = price_data.get('feed') or self._stream.symbol_to_feed.get(symbol)
        if feed not in TRACKED_FEEDS:
            return

        bid = price_data.get('bid')
        ask = price_data.get('ask')
        if bid is None or ask is None:
            return

        async with self._buffer_lock:
            self._buffer[symbol] = {
                'bid': float(bid),
                'ask': float(ask),
                'feed': feed,
                'updated_at': datetime.now(timezone.utc),
            }

    async def _flush_loop(self):
        """Background task: flush buffer to DB every WRITE_INTERVAL seconds."""
        while self._running:
            await asyncio.sleep(WRITE_INTERVAL)
            try:
                await self._flush_to_db()
            except Exception as e:
                logger.error("LivePriceWriter flush error: %s", e)

    async def _flush_to_db(self):
        """Upsert all buffered prices to live_prices in a single executemany call."""
        async with self._buffer_lock:
            if not self._buffer:
                return
            snapshot = dict(self._buffer)
            self._buffer.clear()

        rows = [
            (symbol, data['bid'], data['ask'], data['feed'], data['updated_at'])
            for symbol, data in snapshot.items()
        ]

        query = """
            INSERT INTO live_prices (symbol, bid, ask, feed, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (symbol)
            DO UPDATE SET
                bid        = EXCLUDED.bid,
                ask        = EXCLUDED.ask,
                feed       = EXCLUDED.feed,
                updated_at = EXCLUDED.updated_at
        """

        try:
            await self._db.execute_many(query, rows)
            logger.debug("LivePriceWriter flushed %d symbols to DB", len(rows))
        except Exception as e:
            logger.error("LivePriceWriter DB write failed: %s", e)