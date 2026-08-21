"""
LivePriceWriter - Batches price updates and writes them to the live_prices
table in Supabase every WRITE_INTERVAL seconds.

Each row stores bid/ask/feed for one symbol, sourced from whichever feed serves
it (OANDA, Binance, Exness, or ICMarkets). The execution bot reads these prices
and computes its own broker offset by comparing its MT5 feed to the stored price
at the same timestamp, so no separate ICMarkets reference price is persisted.
"""

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Feeds whose prices we want to persist
TRACKED_FEEDS = {"icmarkets", "oanda", "binance", "exness"}

# How often to flush the buffer to the DB (seconds)
WRITE_INTERVAL = 5

# Re-write an unchanged price at least this often. The EX bot treats a
# live_prices row as dark when updated_at is older than its
# feed_max_staleness_seconds (120 s) and anchors broker offsets to it, so
# quiet markets must still heartbeat well inside that window.
HEARTBEAT_SECONDS = 30


class LivePriceWriter:
    """
    Subscribes to PriceStreamManager updates, buffers the latest price
    per symbol, and upserts them to the live_prices table every 5 seconds.
    """

    def __init__(self, db_manager, stream_manager):
        """
        Args:
            db_manager:     DatabaseManager instance (asyncpg-backed)
            stream_manager: PriceStreamManager instance
        """
        self._db = db_manager
        self._stream = stream_manager

        # Latest price snapshot per symbol {bid, ask, feed, updated_at}.
        # Not cleared on flush — kept so quiet symbols can still heartbeat.
        self._latest: dict[str, dict] = {}
        self._latest_lock = asyncio.Lock()

        # Last successfully written (bid, ask) and monotonic write time per
        # symbol — used to skip upserts for unchanged prices between heartbeats.
        self._last_written: dict[str, tuple] = {}
        self._last_written_at: dict[str, float] = {}

        self._task: Optional[asyncio.Task] = None
        self._running = False

        logger.debug("LivePriceWriter initialised")

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
        logger.debug(
            "LivePriceWriter started (flush every %ds, feeds: %s)", WRITE_INTERVAL, TRACKED_FEEDS
        )

    async def stop(self):
        """Graceful shutdown: do a final flush then cancel the loop."""
        self._running = False
        self._stream.remove_subscriber(self._on_price_update)
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        # Final flush so we don't lose the last few ticks
        await self._flush_to_db()
        logger.debug("LivePriceWriter stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _on_price_update(self, symbol: str, price_data: dict):
        """
        Callback registered with PriceStreamManager.
        Called on every tick for every subscribed symbol.
        We only buffer ticks coming from tracked feeds.
        """
        feed = price_data.get("feed") or self._stream.symbol_to_feed.get(symbol)
        if feed not in TRACKED_FEEDS:
            return

        bid = price_data.get("bid")
        ask = price_data.get("ask")
        if bid is None or ask is None:
            return

        async with self._latest_lock:
            self._latest[symbol] = {
                "bid": float(bid),
                "ask": float(ask),
                "feed": feed,
                "updated_at": datetime.now(timezone.utc),
                "tick_mono": asyncio.get_event_loop().time(),
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
        """Upsert buffered prices to live_prices in a single executemany call.

        Unchanged prices are skipped until HEARTBEAT_SECONDS has elapsed since
        the symbol's last write, cutting egress in quiet markets while keeping
        updated_at fresh for the EX bot's staleness gate.
        """
        async with self._latest_lock:
            if not self._latest:
                return
            snapshot = {symbol: dict(data) for symbol, data in self._latest.items()}

        now_mono = asyncio.get_event_loop().time()
        now_utc = datetime.now(timezone.utc)
        rows = []
        written_symbols = []
        for symbol, data in snapshot.items():
            price = (data["bid"], data["ask"])
            unchanged = self._last_written.get(symbol) == price
            fresh = now_mono - self._last_written_at.get(symbol, 0.0) < HEARTBEAT_SECONDS
            if unchanged and fresh:
                continue
            if unchanged:
                # Heartbeat: re-assert the last known price as still current —
                # but only while ticks keep arriving. A silent feed must let
                # updated_at age so the EX bot's staleness gate can fire.
                if now_mono - data["tick_mono"] > HEARTBEAT_SECONDS:
                    continue
                updated_at = now_utc
            else:
                updated_at = data["updated_at"]
            rows.append((symbol, data["bid"], data["ask"], data["feed"], updated_at))
            written_symbols.append((symbol, price))

        if not rows:
            return

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
            for symbol, price in written_symbols:
                self._last_written[symbol] = price
                self._last_written_at[symbol] = now_mono
            logger.debug("LivePriceWriter flushed %d symbols", len(rows))
        except Exception as e:
            logger.error("LivePriceWriter DB write failed: %r", e)
