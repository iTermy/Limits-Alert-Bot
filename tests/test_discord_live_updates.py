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


class FakeChannel:
    def __init__(self, channel_id: int):
        self.id = channel_id


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        bucket: SharedBucket,
        *,
        channel_id: int = 1,
        reset_once: bool = False,
    ):
        self.id = message_id
        self.channel = FakeChannel(channel_id)
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


class GatedMessage:
    """Blocks inside ``edit`` until released, recording that it started."""

    def __init__(self, message_id: int, channel_id: int, release: asyncio.Event):
        self.id = message_id
        self.channel = FakeChannel(channel_id)
        self.release = release
        self.started = asyncio.Event()

    async def edit(self, *, embed) -> None:
        self.started.set()
        await self.release.wait()


class FakeBot:
    def __init__(self):
        self.restart_reasons: list[str] = []

    def request_discord_restart(self, reason: str) -> None:
        self.restart_reasons.append(reason)


def _make_signal(sid: int) -> SignalData:
    return SignalData(
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


def _make_alert_system(
    signal_count: int,
    *,
    block_first: bool = False,
    channel_ids: dict[int, int] | None = None,
    message_factory=None,
):
    prices = {f"TEST{sid}": 100.0 + sid for sid in range(1, signal_count + 1)}
    stream = FakeStreamManager(prices)
    alerts = AlertSystem(bot=None, stream_manager=stream, alert_config=None)
    alerts._LIVE_UPDATE_RETRY_DELAY = 0
    bucket = SharedBucket(block_first=block_first)
    messages = {}

    for sid in range(1, signal_count + 1):
        channel_id = (channel_ids or {}).get(sid, 1)
        if message_factory:
            message = message_factory(sid, channel_id)
        else:
            message = FakeMessage(sid, bucket, channel_id=channel_id)
        messages[sid] = message
        alerts.signal_messages[sid] = message
        alerts._register_live_embed(_make_signal(sid), "approaching")

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
        assert not alerts._pending_live_updates

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


def test_channels_run_in_parallel_but_serialize_within_a_channel():
    """Discord buckets edits per channel, so that is the unit of serialization.

    Two signals in each of three channels: all three channels must have an edit
    in flight at once, and neither channel may start its second signal until its
    first one lands.
    """

    async def scenario():
        release = asyncio.Event()
        channel_ids = {1: 10, 2: 10, 3: 20, 4: 20, 5: 30, 6: 30}
        alerts, _, _, messages = _make_alert_system(
            6,
            channel_ids=channel_ids,
            message_factory=lambda sid, channel_id: GatedMessage(sid, channel_id, release),
        )

        alerts._queue_live_updates()
        drain = asyncio.create_task(alerts._drain_live_update_queue())

        first_in_each_channel = [messages[1], messages[3], messages[5]]
        await asyncio.wait_for(
            asyncio.gather(*(m.started.wait() for m in first_in_each_channel)),
            timeout=1,
        )

        # The second signal in each channel is queued behind the held edit.
        assert not messages[2].started.is_set()
        assert not messages[4].started.is_set()
        assert not messages[6].started.is_set()

        release.set()
        await asyncio.wait_for(drain, timeout=2)

        assert all(message.started.is_set() for message in messages.values())

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
