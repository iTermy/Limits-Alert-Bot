"""Discord live-update queue simulations.

These tests model many active signals sharing one Discord rate-limit bucket.
They deliberately avoid the real Discord API: the fake messages can pause or
reset connections deterministically without risking a production rate limit.
"""

import asyncio

import pytest

from models import LimitData, SignalData
from price_feeds.alerting.alert_system import AlertSystem


class FakeStreamManager:
    def __init__(self, prices: dict[str, float]):
        self.prices = prices

    async def get_latest_price(self, instrument: str) -> dict[str, float]:
        price = self.prices[instrument]
        return {"bid": price, "ask": price, "spread": 0.0}


class SharedBucket:
    """Record concurrency and optionally hold the first Discord edit open."""

    def __init__(self, *, block_first: bool = False):
        self.block_first = block_first
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.in_flight = 0
        self.max_in_flight = 0
        self.total_edits = 0

    async def edit(self) -> None:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.total_edits += 1
        try:
            if self.block_first and self.total_edits == 1:
                self.first_started.set()
                await self.release_first.wait()
        finally:
            self.in_flight -= 1


class FakeMessage:
    def __init__(self, message_id: int, bucket: SharedBucket, *, reset_once: bool = False):
        self.id = message_id
        self.bucket = bucket
        self.reset_once = reset_once
        self.attempts = 0
        self.rendered_prices: list[str] = []

    async def edit(self, *, embed) -> None:
        self.attempts += 1
        if self.reset_once:
            self.reset_once = False
            raise ConnectionResetError(10054, "connection reset by peer")

        await self.bucket.edit()
        fields = embed.to_dict().get("fields", [])
        current = next(field["value"] for field in fields if field["name"] == "Current Price")
        self.rendered_prices.append(current)


def _make_alert_system(signal_count: int, *, block_first: bool = False):
    prices = {f"TEST{sid}": 100.0 + sid for sid in range(1, signal_count + 1)}
    stream = FakeStreamManager(prices)
    alerts = AlertSystem(bot=None, stream_manager=stream, alert_config=None)
    alerts._LIVE_UPDATE_RETRY_DELAY = 0
    bucket = SharedBucket(block_first=block_first)
    messages = {}

    for sid in range(1, signal_count + 1):
        signal = SignalData(
            signal_id=sid,
            instrument=f"TEST{sid}",
            direction="long",
            stop_loss=90.0,
            total_limits=1,
            limits=[
                LimitData(
                    id=sid,
                    signal_id=sid,
                    price_level=99.0,
                    sequence_number=1,
                )
            ],
        )
        message = FakeMessage(sid, bucket)
        messages[sid] = message
        alerts.signal_messages[sid] = message
        alerts._register_live_embed(signal, "approaching")

    return alerts, stream, bucket, messages


@pytest.mark.parametrize("signal_count", [5, 10])
def test_refreshes_are_sequential_and_repeated_passes_are_coalesced(signal_count):
    async def scenario():
        alerts, stream, bucket, messages = _make_alert_system(
            signal_count, block_first=True
        )
        alerts._queue_live_updates()
        drain = asyncio.create_task(alerts._drain_live_update_queue())

        await asyncio.wait_for(bucket.first_started.wait(), timeout=1)
        assert bucket.max_in_flight == 1

        # Simulate four more scheduler passes while Discord is throttling the
        # first edit. The queue must remain bounded to one entry per signal.
        for _ in range(4):
            for instrument in stream.prices:
                stream.prices[instrument] += 1.0
            alerts._queue_live_updates()
        assert len(alerts._pending_live_updates) <= signal_count

        bucket.release_first.set()
        await asyncio.wait_for(drain, timeout=2)

        assert bucket.max_in_flight == 1
        # The in-flight signal needs one follow-up; all signals that had not yet
        # rendered consume only their newest state. Four full stale batches are
        # not submitted to Discord.
        assert bucket.total_edits == signal_count + 1
        for sid, message in messages.items():
            expected = f"{stream.prices[f'TEST{sid}']:.2f}"
            assert message.rendered_prices[-1] == expected

    asyncio.run(scenario())


def test_connection_reset_retries_one_latest_snapshot():
    async def scenario():
        alerts, stream, bucket, messages = _make_alert_system(1)
        message = messages[1]
        message.reset_once = True

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert message.attempts == 2
        assert bucket.total_edits == 1
        assert message.rendered_prices == [f"{stream.prices['TEST1']:.2f}"]
        assert not alerts._pending_live_updates

    asyncio.run(scenario())


def test_critical_delivery_retries_until_success():
    async def scenario():
        alerts, _, _, _ = _make_alert_system(1)
        alerts._DELIVERY_RETRY_BASE_DELAY = 0
        attempts = 0

        async def flaky_delivery() -> bool:
            nonlocal attempts
            attempts += 1
            return attempts >= 2

        alerts.queue_delivery_retry("limit_hit:1:1", flaky_delivery)
        task = alerts._delivery_retry_tasks["limit_hit:1:1"]
        await asyncio.wait_for(task, timeout=1)

        assert attempts == 2
        assert not alerts._delivery_retry_tasks

    asyncio.run(scenario())
