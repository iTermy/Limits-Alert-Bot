"""
OANDA v20 Streaming Feed
Uses OANDA's native pricing stream endpoint for real-time updates
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# OANDA sends a HEARTBEAT every ~5 s, so no data for this long means the
# connection has silently half-died (no exception raised). Force a reconnect.
_STREAM_READ_TIMEOUT = 15


class OANDAStream:
    """
    OANDA v20 streaming price feed

    Uses the /v3/accounts/{accountId}/pricing/stream endpoint
    which provides continuous real-time pricing updates
    """

    def __init__(self, api_key: Optional[str] = None, account_id: Optional[str] = None, practice: bool = False):
        """Initialize OANDA stream"""
        import os

        self.api_key = api_key or os.getenv("OANDA_API_KEY")
        self.account_id = account_id or os.getenv("OANDA_ACCOUNT_ID")

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
        self.last_connect_error: Optional[str] = None
        self.subscribed_symbols: set[str] = set()

        # Stream control
        self.streaming = False
        self.stream_response = None

        logger.debug("OANDAStream initialized")

    async def connect(self) -> bool:
        """Initialize OANDA session"""
        try:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "UNIX",
                },
                timeout=aiohttp.ClientTimeout(total=None),  # No timeout for streaming
            )

            # Test connection
            test_url = (
                f"{self.base_url.replace('stream-', 'api-')}/v3/accounts/{self.account_id}/summary"
            )
            async with self.session.get(
                test_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    self.connected = True
                    self.last_connect_error = None
                    logger.debug("Connected to OANDA stream")
                    return True
                # The stream handler owns the noise policy for a feed that is
                # down (weekend, maintenance) — it retries on a backoff and
                # logs once per outage, so record the reason rather than log it.
                self.last_connect_error = f"HTTP {response.status}"
                logger.debug("OANDA connect rejected: %s", self.last_connect_error)
                return False

        except Exception as e:
            self.last_connect_error = str(e)
            logger.debug("OANDA connect failed: %s", e)
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
        logger.debug("Disconnected from OANDA")

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
        logger.debug(f"Subscribed to {symbol} on OANDA")

        # If already streaming, need to restart with updated symbols
        if self.streaming:
            logger.debug("Restarting stream with updated symbol list")
            await self._restart_stream()

    async def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol"""
        self.subscribed_symbols.discard(symbol)

        # Restart stream if actively streaming
        if self.streaming:
            await self._restart_stream()

    async def bulk_subscribe(self, symbols: list) -> list[str]:
        """Subscribe to multiple symbols; return the ones that were accepted."""
        for symbol in symbols:
            self.subscribed_symbols.add(symbol)

        logger.debug(f"Bulk subscribed to {len(symbols)} symbols on OANDA")

        # Restart stream if needed
        if self.streaming:
            await self._restart_stream()

        return list(symbols)

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

    @staticmethod
    def _log_stream_failure(failures: int, kind: str, error: Exception):
        """Report a stream drop at a level that matches what it costs.

        OANDA's stream is severed several times a day (`Connection closed`,
        a truncated chunked body) and the loop re-requests it within 5 s, so a
        single drop is recoverable and belongs at WARNING. A run of them is the
        same outage restating itself — the reconnect budget and the feed-health
        monitor own that escalation, so repeats go to DEBUG rather than filling
        errors.log with lines nothing acts on.
        """
        if failures == 1:
            logger.warning("OANDA stream %s: %s", kind, error)
        else:
            logger.debug("OANDA stream %s (attempt %d): %s", kind, failures, error)

    async def stream_prices(self) -> AsyncIterator[tuple[str, dict]]:
        """
        Stream price updates from OANDA

        Yields:
            Tuple of (symbol, price_data) on each update
        """
        if not self.connected:
            raise ConnectionError(
                f"Not connected to OANDA ({self.last_connect_error or 'no session'})"
            )

        if not self.subscribed_symbols:
            logger.debug("No symbols subscribed, waiting...")
            await asyncio.sleep(5)
            return

        self.streaming = True
        failures = 0

        while self.streaming:
            try:
                if not self.subscribed_symbols:
                    # Every OANDA signal closed at once (a mass expiry does it
                    # daily) and the set emptied mid-stream. An empty
                    # `instruments` is rejected with a 400, and the old code
                    # re-sent it every 5 s for as long as the set stayed empty —
                    # 94 rejected requests over two such windows. Wait for a
                    # subscribe instead; the next pass picks it up.
                    logger.debug("No OANDA symbols subscribed — idling")
                    await asyncio.sleep(5)
                    continue

                # Build instrument list
                instruments = ",".join(self.subscribed_symbols)
                params = {"instruments": instruments}

                # Open streaming connection
                async with self.session.get(self.stream_url, params=params) as response:
                    if response.status != 200:
                        # A single unrecognised instrument rejects the whole
                        # request, silencing every OANDA symbol — log the body
                        # so the offending name is identifiable.
                        body = (await response.text())[:300]
                        logger.warning(
                            "OANDA stream request rejected (%s): %s", response.status, body
                        )
                        await asyncio.sleep(5)
                        continue

                    self.stream_response = response
                    failures = 0

                    # Read stream line by line with a watchdog: OANDA's heartbeat
                    # keeps data flowing, so a read that stalls past the timeout
                    # means the connection is dead — break out to reconnect.
                    while self.streaming:
                        try:
                            line = await asyncio.wait_for(
                                response.content.readline(),
                                timeout=_STREAM_READ_TIMEOUT,
                            )
                        except asyncio.TimeoutError:
                            logger.warning(
                                "OANDA stream stalled (no data for %ss) — reconnecting",
                                _STREAM_READ_TIMEOUT,
                            )
                            break

                        if not line:
                            # EOF — server closed the stream; reconnect.
                            logger.debug("OANDA stream closed by server — reconnecting")
                            break

                        # Parse JSON line
                        try:
                            data = json.loads(line)

                            # Check message type
                            if data.get("type") == "PRICE":
                                # Extract price data
                                symbol = data["instrument"]

                                # Get best bid/ask
                                bids = data.get("bids", [])
                                asks = data.get("asks", [])

                                if bids and asks:
                                    bid = float(bids[0]["price"])
                                    ask = float(asks[0]["price"])

                                    price_data = {
                                        "bid": bid,
                                        "ask": ask,
                                        "timestamp": datetime.fromtimestamp(float(data["time"])),
                                        "tradeable": data.get("tradeable", True),
                                    }

                                    yield symbol, price_data

                            elif data.get("type") == "HEARTBEAT":
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
                failures += 1
                self._log_stream_failure(failures, "connection error", e)
                await asyncio.sleep(5)

            except Exception as e:
                failures += 1
                self._log_stream_failure(failures, "error", e)
                await asyncio.sleep(5)

    def get_subscribed_symbols(self) -> set[str]:
        """Get set of currently subscribed symbols"""
        return self.subscribed_symbols.copy()
