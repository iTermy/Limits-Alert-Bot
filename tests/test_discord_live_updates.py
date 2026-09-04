"""Discord live-update queue simulations.

These tests model many active signals sharing one Discord rate-limit bucket.
They deliberately avoid the real Discord API: the fake messages can pause or
reset connections deterministically without risking a production rate limit.
"""

import asyncio
from types import SimpleNamespace

import pytest

from models import LimitData, SignalData
from price_feeds.alerting.alert_system import AlertSystem
from price_feeds.alerting.channel_rate_limiter import ChannelRateLimiter

ALERT_CHANNEL_ID = 4242


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
        self.started_at: list[float] = []

    async def edit(self) -> None:
        self.started_at.append(asyncio.get_running_loop().time())
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
        self.channel = SimpleNamespace(id=ALERT_CHANNEL_ID)
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


class FakeBot:
    def __init__(self):
        self.restart_reasons: list[str] = []

    def request_discord_restart(self, reason: str) -> None:
        self.restart_reasons.append(reason)


def _make_alert_system(signal_count: int, *, block_first: bool = False):
    prices = {f"TEST{sid}": 100.0 + sid for sid in range(1, signal_count + 1)}
    stream = FakeStreamManager(prices)
    alerts = AlertSystem(bot=None, stream_manager=stream, alert_config=None)
    # The channel budget is timing, not scheduling. Tests that measure the
    # scheduler give it unlimited room; the budget tests set their own.
    alerts._channel_limiter = ChannelRateLimiter(capacity=10_000)
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
def test_refreshes_are_sequential_and_busy_passes_are_skipped(signal_count):
    async def scenario():
        alerts, stream, bucket, messages = _make_alert_system(
            signal_count, block_first=True
        )
        alerts._queue_live_updates()
        drain = asyncio.create_task(alerts._drain_live_update_queue())

        await asyncio.wait_for(bucket.first_started.wait(), timeout=1)
        assert bucket.max_in_flight == 1

        # Simulate four more scheduler ticks while Discord is throttling the
        # first edit. A busy pass must not be refilled.
        for _ in range(4):
            for instrument in stream.prices:
                stream.prices[instrument] += 1.0
            alerts._queue_live_updates()
        assert len(alerts._pending_live_updates) == signal_count - 1

        bucket.release_first.set()
        await asyncio.wait_for(drain, timeout=2)

        assert bucket.max_in_flight == 1
        # Each signal is attempted once. The already in-flight signal keeps its
        # original snapshot; signals not yet rendered consume the newest state.
        assert bucket.total_edits == signal_count
        assert messages[1].rendered_prices == ["101.00"]
        for sid, message in list(messages.items())[1:]:
            expected = f"{stream.prices[f'TEST{sid}']:.2f}"
            assert message.rendered_prices[-1] == expected

    asyncio.run(scenario())


def test_connection_reset_waits_until_next_refresh_pass():
    async def scenario():
        alerts, stream, bucket, messages = _make_alert_system(1)
        message = messages[1]
        message.reset_once = True

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert message.attempts == 1
        assert bucket.total_edits == 0
        assert not alerts._pending_live_updates

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert message.attempts == 2
        assert bucket.total_edits == 1
        assert message.rendered_prices == [f"{stream.prices['TEST1']:.2f}"]

    asyncio.run(scenario())


def test_hung_cosmetic_edit_is_bounded_and_does_not_block_following_signal():
    async def scenario():
        alerts, _, bucket, messages = _make_alert_system(2, block_first=True)
        alerts._LIVE_UPDATE_ATTEMPT_TIMEOUT = 0.01

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=1)

        assert messages[1].attempts == 1
        assert messages[1].rendered_prices == []
        assert messages[2].attempts == 1
        assert messages[2].rendered_prices == ["102.00"]
        assert bucket.max_in_flight == 1
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


def test_critical_delivery_timeout_releases_monitor_and_queues_retry():
    async def scenario():
        alerts, _, _, _ = _make_alert_system(1)
        alerts._DELIVERY_ATTEMPT_TIMEOUT = 0.01

        async def hung_delivery() -> bool:
            await asyncio.Event().wait()
            return True

        delivered = await asyncio.wait_for(
            alerts.deliver_critical("stop_loss:1", hung_delivery),
            timeout=0.2,
        )

        assert delivered is False
        assert "stop_loss:1" in alerts._delivery_retry_tasks
        alerts.stop_live_updates()

    asyncio.run(scenario())


def test_consecutive_discord_operation_timeouts_request_restart():
    alerts, _, _, _ = _make_alert_system(1)
    bot = FakeBot()
    alerts.bot = bot

    alerts._record_discord_operation_timeout("first")
    assert bot.restart_reasons == []

    alerts._record_discord_operation_timeout("second")
    assert bot.restart_reasons == ["2 consecutive Discord operation timeouts"]


def test_cosmetic_edits_wait_for_room_in_the_channel_budget():
    async def scenario():
        alerts, _, bucket, _ = _make_alert_system(4)
        alerts._channel_limiter = ChannelRateLimiter(capacity=3, window_seconds=0.3)

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert bucket.total_edits == 4
        # Capacity 3 less one reserved slot: two edits burst, the third waits
        # for the first to age out of the window.
        first, _, third = bucket.started_at[0], bucket.started_at[1], bucket.started_at[2]
        assert third - first >= alerts._channel_limiter.window_seconds * 0.9

    asyncio.run(scenario())


def test_idle_channel_takes_the_burst_without_waiting():
    async def scenario():
        alerts, _, bucket, _ = _make_alert_system(3)
        alerts._channel_limiter = ChannelRateLimiter(capacity=8, window_seconds=5)

        loop = asyncio.get_running_loop()
        started = loop.time()
        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert bucket.total_edits == 3
        assert loop.time() - started < 0.5

    asyncio.run(scenario())


def test_event_traffic_spends_the_budget_the_refresh_loop_waits_on():
    async def scenario():
        alerts, _, bucket, _ = _make_alert_system(1)
        alerts._channel_limiter = ChannelRateLimiter(capacity=2, window_seconds=0.3)
        # An event edit and its ping just went out on this channel.
        alerts._channel_limiter.record(ALERT_CHANNEL_ID)

        loop = asyncio.get_running_loop()
        started = loop.time()
        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=2)

        assert bucket.total_edits == 1
        assert loop.time() - started >= alerts._channel_limiter.window_seconds * 0.9

    asyncio.run(scenario())


def test_unchanged_embeds_do_not_wait_for_an_edit_slot():
    async def scenario():
        alerts, _, bucket, _ = _make_alert_system(3)
        alerts._channel_limiter = ChannelRateLimiter(capacity=2, window_seconds=0.5)

        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=5)
        assert bucket.total_edits == 3

        # Prices are static in the fake stream, so every embed of the second
        # pass renders identically. Skipped edits must not spend the budget.
        loop = asyncio.get_running_loop()
        started = loop.time()
        alerts._queue_live_updates()
        await asyncio.wait_for(alerts._drain_live_update_queue(), timeout=5)

        assert bucket.total_edits == 3
        assert loop.time() - started < alerts._channel_limiter.window_seconds

    asyncio.run(scenario())
