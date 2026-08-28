"""A status write that fails must not silently retire the level it was recording.

Signal 3983 (2026-08-28) is the case these cover: six limits and the stop loss
were all crossed on the live feed, the bot recorded none of them, and the DB kept
advertising the signal as active with every limit pending for the next hour — long
enough for the execution bot to trade a signal that should already have stopped
out. In-memory state now moves only after the write lands, and the periodic
refresh reconciles limits it never used to touch.
"""

import asyncio
from datetime import datetime, timedelta, timezone

from models.signal import LimitData, SignalData
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor


class _StubAlertSystem:
    def __init__(self):
        self._live_embeds = {}
        self.hit_alerts = []
        self.sl_alerts = []

    async def send_limit_hit_alert(self, signal, limit, current_price, **kwargs):
        self.hit_alerts.append(limit.sequence_number)
        return True

    async def send_stop_loss_alert(self, signal, current_price):
        self.sl_alerts.append(current_price)
        return True


class _StubSignalDB:
    """Fails the first `failures` writes, then succeeds."""

    def __init__(self, failures=0, raises=False):
        self.failures = failures
        self.raises = raises
        self.attempts = 0

    def _next(self):
        self.attempts += 1
        if self.attempts <= self.failures:
            if self.raises:
                raise RuntimeError("pool timeout")
            return False
        return True

    async def process_limit_hit(self, limit_id, actual_price):
        return {"signal_id": 1} if self._next() else {"signal_id": None}

    async def manually_set_signal_status(self, signal_id, status, **kwargs):
        return self._next()


class _StubTracker:
    def evict_signal(self, signal_id):
        pass

    async def refresh_hit_limits(self, signal_id):
        pass

    async def finalize_with_price(self, signal_id, price, reason):
        pass

    async def finalize(self, signal_id, price, reason):
        pass


class _StubStreamManager:
    async def unsubscribe_symbol(self, symbol):
        pass


class _StubBot:
    news_manager = None
    guilds = []


def _signal(status="active", limits_hit=0):
    return SignalData(
        signal_id=1,
        instrument="GBPUSD",
        direction="long",
        status=status,
        type="standard",
        stop_loss=1.35301,
        total_limits=2,
        limits_hit=limits_hit,
        limits=[
            LimitData(id=10, signal_id=1, price_level=1.35561, sequence_number=1),
            LimitData(id=11, signal_id=1, price_level=1.35531, sequence_number=2),
        ],
    )


def _monitor(signal, signal_db):
    monitor = StreamingPriceMonitor(
        bot=_StubBot(),
        signal_db=signal_db,
        db=None,
        alert_system=_StubAlertSystem(),
        stream_manager=_StubStreamManager(),
        alert_config=None,
        tp_config=None,
        tp_monitor=_StubTracker(),
        nm_config=None,
        nm_monitor=_StubTracker(),
        trailing_monitor=_StubTracker(),
        excursion_monitor=_StubTracker(),
        live_price_writer=None,
        health_monitor=None,
    )
    monitor._react_async = lambda signal, emoji: None
    monitor.active_signals[1] = signal
    return monitor


def _hit(monitor, signal, limit, price=1.35400):
    asyncio.run(
        monitor._handle_limit_hit(signal, limit, price, False, False, 0.0, False)
    )


class TestLimitHitWriteFailure:
    def test_failed_write_leaves_the_limit_pending(self):
        signal = _signal()
        monitor = _monitor(signal, _StubSignalDB(failures=1))

        _hit(monitor, signal, signal.limits[0])

        assert signal.limits[0].status == "pending"
        assert signal.limits[0].hit_alert_sent is False
        assert signal.pending_limits == signal.limits
        assert monitor.alert_system.hit_alerts == []

    def test_a_raising_write_is_treated_the_same(self):
        signal = _signal()
        monitor = _monitor(signal, _StubSignalDB(failures=1, raises=True))

        _hit(monitor, signal, signal.limits[0])

        assert signal.limits[0].status == "pending"
        assert monitor.alert_system.hit_alerts == []

    def test_the_next_tick_retries_once_the_backoff_expires(self):
        signal = _signal()
        monitor = _monitor(signal, _StubSignalDB(failures=1))

        _hit(monitor, signal, signal.limits[0])
        monitor._write_retry_after.clear()
        _hit(monitor, signal, signal.limits[0])

        assert signal.limits[0].status == "hit"
        assert signal.limits[0].hit_alert_sent is True
        assert signal.status == "hit"
        assert monitor.alert_system.hit_alerts == [1]

    def test_the_backoff_holds_a_failing_write_off_the_tick_path(self):
        signal = _signal()
        signal_db = _StubSignalDB(failures=1)
        monitor = _monitor(signal, signal_db)

        _hit(monitor, signal, signal.limits[0])
        _hit(monitor, signal, signal.limits[0])

        assert signal_db.attempts == 1

    def test_a_landed_write_alerts_and_clears_the_backoff(self):
        signal = _signal()
        monitor = _monitor(signal, _StubSignalDB())

        _hit(monitor, signal, signal.limits[0])

        assert signal.limits[0].status == "hit"
        assert monitor.alert_system.hit_alerts == [1]
        assert monitor._write_retry_after == {}


class TestStopLossWriteFailure:
    def _check(self, monitor, signal, price=1.35200):
        asyncio.run(monitor._check_stop_loss(signal, price, "long", False, False))

    def test_failed_write_leaves_the_stop_live(self):
        signal = _signal(status="hit", limits_hit=1)
        monitor = _monitor(signal, _StubSignalDB(failures=1))

        self._check(monitor, signal)

        assert signal.sl_alert_sent is False
        assert monitor.alert_system.sl_alerts == []

    def test_the_stop_fires_on_a_later_tick(self):
        signal = _signal(status="hit", limits_hit=1)
        monitor = _monitor(signal, _StubSignalDB(failures=1))

        self._check(monitor, signal)
        monitor._write_retry_after.clear()
        self._check(monitor, signal)

        assert signal.sl_alert_sent is True
        assert monitor.alert_system.sl_alerts == [1.35200]


class TestLimitReconciliation:
    def _reconcile(self, tracked, pending_ids, snapshot_at=None):
        fresh = SignalData(
            signal_id=1,
            instrument="GBPUSD",
            direction="long",
            limits=[lim for lim in tracked.limits if lim.id in pending_ids],
        )
        monitor = _monitor(tracked, _StubSignalDB())
        monitor._reconcile_limits(
            tracked, fresh, snapshot_at or datetime.now(timezone.utc)
        )

    def test_a_limit_the_db_still_calls_pending_is_restored(self):
        tracked = _signal()
        tracked.limits[0].status = "hit"
        tracked.limits[0].hit_alert_sent = True
        tracked.limits[0].hit_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        tracked.limits[0].hit_price = 1.35400

        self._reconcile(tracked, {10, 11})

        assert tracked.limits[0].status == "pending"
        assert tracked.limits[0].hit_alert_sent is False
        assert tracked.limits[0].hit_time is None
        assert tracked.limits[0].hit_price is None
        assert len(tracked.pending_limits) == 2

    def test_a_genuinely_hit_limit_is_left_alone(self):
        tracked = _signal()
        tracked.limits[0].status = "hit"
        tracked.limits[0].hit_alert_sent = True

        self._reconcile(tracked, {11})

        assert tracked.limits[0].status == "hit"
        assert tracked.limits[0].hit_alert_sent is True

    def test_a_fill_newer_than_the_snapshot_is_left_alone(self):
        snapshot_at = datetime.now(timezone.utc)
        tracked = _signal()
        tracked.limits[0].status = "hit"
        tracked.limits[0].hit_alert_sent = True
        tracked.limits[0].hit_time = snapshot_at + timedelta(seconds=1)

        self._reconcile(tracked, {10, 11}, snapshot_at=snapshot_at)

        assert tracked.limits[0].status == "hit"

    def test_a_cancelled_limit_the_db_still_calls_pending_is_restored(self):
        tracked = _signal()
        tracked.limits[1].status = "cancelled"

        self._reconcile(tracked, {10, 11})

        assert tracked.limits[1].status == "pending"
