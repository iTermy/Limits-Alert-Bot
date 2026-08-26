"""Swing signals hold through news windows.

Every other type is cancelled when a news window covers its instrument: the open
position is closed outright, a fresh fill during the window is cancelled instead
of marked, and the approaching alert is suppressed because that fill would be
cancelled anyway. A swing is meant to sit through the event, so none of the three
gates apply to it.
"""

import asyncio

import pytest

from models.signal import LimitData, SignalData
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor


class _NewsEvent:
    category = "usd"

    def __str__(self):
        return "USD — NFP"


class _NewsManager:
    def __init__(self, active):
        self.active = active

    def is_news_active_for(self, instrument):
        return _NewsEvent() if self.active else None


class _StubBot:
    def __init__(self, news_active):
        self.news_manager = _NewsManager(news_active)


class _StubTracker:
    """Stands in for the tp / nm / trailing / excursion monitors."""

    def __init__(self):
        self.approaches = []

    async def check_signal(self, signal, current_bid):
        return False

    async def mark_entry(self, signal):
        pass

    async def update(self, signal, bid, ask=None):
        return False

    async def update_approach(self, signal, current_price):
        pass

    async def start_approach(self, signal, current_price):
        self.approaches.append(current_price)


class _AlertSystem:
    def __init__(self):
        self.hits = []
        self.approaching = []

    async def send_limit_hit_alert(self, signal, limit, current_price, **kwargs):
        self.hits.append(current_price)
        return True

    async def send_approaching_alert(self, signal, limit, current_price, distance, **kwargs):
        self.approaching.append(current_price)
        return True


class _AlertConfig:
    def get_approaching_distance(self, symbol, current_price=None):
        return 10.0

    def format_distance_for_display(self, symbol, distance, current_price):
        return f"${distance:.2f}"


def _monitor(news_active=True):
    monitor = StreamingPriceMonitor(
        bot=_StubBot(news_active),
        signal_db=None,
        db=None,
        alert_system=_AlertSystem(),
        stream_manager=None,
        alert_config=_AlertConfig(),
        tp_config=None,
        tp_monitor=_StubTracker(),
        nm_config=None,
        nm_monitor=_StubTracker(),
        trailing_monitor=_StubTracker(),
        excursion_monitor=_StubTracker(),
        live_price_writer=None,
        health_monitor=None,
    )
    monitor.guarded = []
    monitor._react_async = lambda signal, emoji: None

    async def _guard(signal, current_price, reason, event=None):
        monitor.guarded.append(reason)

    async def _process_hit(signal, limit, current_price):
        pass

    async def _mark_approaching(limit_id):
        pass

    monitor._cancel_signal_during_guard = _guard
    monitor._process_limit_hit = _process_hit
    monitor._mark_approaching_sent = _mark_approaching
    return monitor


def _signal(signal_type, status="active"):
    signal = SignalData(
        signal_id=1,
        instrument="XAUUSD",
        direction="long",
        status=status,
        type=signal_type,
        stop_loss=4900.0,
        total_limits=1,
        limits_hit=1 if status == "hit" else 0,
        limits=[
            LimitData(
                id=10,
                signal_id=1,
                price_level=5000.0,
                sequence_number=1,
                status="hit" if status == "hit" else "pending",
            )
        ],
    )
    signal.asset_class = "metals"
    return signal


class TestOpenPositionDuringNews:
    """A HIT signal is cancelled the moment a window opens over it."""

    def _drive(self, monitor, signal):
        price = {"bid": 5040.0, "ask": 5040.5, "spread": 0.5}
        asyncio.run(monitor._check_signal(signal, price, False, False, False))

    @pytest.mark.parametrize("signal_type", ["standard", "scalp", "toll", "pa"])
    def test_other_types_are_cancelled(self, signal_type):
        monitor = _monitor()
        self._drive(monitor, _signal(signal_type, status="hit"))
        assert monitor.guarded == ["news"]

    def test_swing_rides_it_out(self):
        monitor = _monitor()
        self._drive(monitor, _signal("swing", status="hit"))
        assert monitor.guarded == []


class TestFillDuringNews:
    """A limit touched inside the window is a fresh entry into the event."""

    def _drive(self, monitor, signal):
        asyncio.run(
            monitor._handle_limit_hit(
                signal, signal.limits[0], 5000.0, False, False, 0.5, False
            )
        )

    def test_other_types_are_cancelled(self):
        monitor = _monitor()
        self._drive(monitor, _signal("standard"))
        assert monitor.guarded == ["news"]
        assert monitor.alert_system.hits == []

    def test_swing_is_filled_normally(self):
        monitor = _monitor()
        signal = _signal("swing")
        self._drive(monitor, signal)
        assert monitor.guarded == []
        assert monitor.alert_system.hits == [5000.0]
        assert signal.limits[0].status == "hit"


class TestApproachingDuringNews:
    def _drive(self, monitor, signal):
        asyncio.run(
            monitor._handle_approaching(
                signal, signal.limits[0], 5005.0, 5.0, False, False, 0.5, False
            )
        )

    def test_other_types_stay_quiet(self):
        monitor = _monitor()
        self._drive(monitor, _signal("standard"))
        assert monitor.alert_system.approaching == []

    def test_swing_still_alerts(self):
        monitor = _monitor()
        signal = _signal("swing")
        self._drive(monitor, signal)
        assert monitor.alert_system.approaching == [5005.0]
        assert signal.limits[0].approaching_alert_sent is True

    def test_other_types_alert_once_the_window_closes(self):
        monitor = _monitor(news_active=False)
        self._drive(monitor, _signal("standard"))
        assert monitor.alert_system.approaching == [5005.0]
