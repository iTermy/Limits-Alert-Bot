"""Spread-hour tests.

17:00-18:00 ET is spread hour every day but Saturday. Sunday used to be exempt
along with Saturday, so the week's 17:00 reopen — the widest, thinnest book of
the week — was treated as ordinary trading and auto-TP fired on the gap.
"""

import asyncio
from datetime import datetime

import pytest
import pytz

from models.signal import LimitData, SignalData
from price_feeds.monitors import streaming_monitor
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor

EST = pytz.timezone("America/New_York")

# 2026-08-10 is a Monday, so the offsets below land on known weekdays.
MONDAY = 10
WEDNESDAY = 12
FRIDAY = 14
SATURDAY = 15
SUNDAY = 16


class _StubBot:
    news_manager = None


class _StubTracker:
    """Stands in for the tp / nm / trailing / excursion monitors."""

    def __init__(self, tp_triggered=False):
        self.tp_triggered = tp_triggered
        self.checked = []
        self.updated = []

    async def check_signal(self, signal, current_bid):
        self.checked.append(current_bid)
        return self.tp_triggered

    async def mark_entry(self, signal):
        pass

    async def update(self, signal, bid, ask=None):
        self.updated.append(bid)
        return False


def _monitor():
    return StreamingPriceMonitor(
        bot=_StubBot(),
        signal_db=None,
        db=None,
        alert_system=None,
        stream_manager=None,
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


@pytest.fixture
def at(monkeypatch):
    """Freeze the monitor's clock at a given 2026-08 day/hour in ET."""

    def _set(day, hour, minute=0):
        moment = EST.localize(datetime(2026, 8, day, hour, minute))

        class _Clock:
            @staticmethod
            def now(tz=None):
                return moment.astimezone(tz) if tz else moment

        monkeypatch.setattr(streaming_monitor, "datetime", _Clock)

    return _set


@pytest.mark.parametrize(
    "day,hour,minute,expected,label",
    [
        (SUNDAY, 17, 0, True, "Sunday reopen — was fair game before the fix"),
        (SUNDAY, 17, 59, True, "Sunday, last minute of the window"),
        (SUNDAY, 18, 0, False, "Sunday, window over"),
        (SUNDAY, 16, 59, False, "Sunday, before the reopen"),
        (SATURDAY, 17, 30, False, "Saturday — nothing quotes at all"),
        (MONDAY, 17, 30, True, "weekday spread hour"),
        (WEDNESDAY, 16, 59, False, "a minute before the window"),
        (WEDNESDAY, 18, 0, False, "the minute the window ends"),
        (FRIDAY, 17, 30, True, "Friday close"),
    ],
)
def test_spread_hour_window(at, day, hour, minute, expected, label):
    at(day, hour, minute)
    assert _monitor()._is_spread_hour() is expected, label


def _hit_signal(asset_class="metals"):
    signal = SignalData(
        signal_id=1,
        instrument="XAUUSD" if asset_class == "metals" else "BTCUSDT",
        direction="long",
        status="hit",
        type="pa",
        stop_loss=4900.0,
        total_limits=1,
        limits_hit=1,
        limits=[
            LimitData(id=10, signal_id=1, price_level=5000.0, sequence_number=1, status="hit")
        ],
    )
    signal.asset_class = asset_class
    return signal


def _drive(monitor, signal, is_spread_hour):
    price = {"bid": 5040.0, "ask": 5040.5, "spread": 0.5}
    asyncio.run(monitor._check_signal(signal, price, is_spread_hour, False, False))


class TestAutoTPDuringSpreadHour:
    def test_non_crypto_position_is_left_alone(self):
        monitor = _monitor()
        _drive(monitor, _hit_signal(), is_spread_hour=True)
        assert monitor.tp_monitor.checked == []
        assert monitor.excursion_monitor.updated == []

    def test_crypto_keeps_being_managed(self):
        monitor = _monitor()
        _drive(monitor, _hit_signal("crypto"), is_spread_hour=True)
        assert monitor.tp_monitor.checked == [5040.0]

    def test_outside_the_window_nothing_changes(self):
        monitor = _monitor()
        _drive(monitor, _hit_signal(), is_spread_hour=False)
        assert monitor.tp_monitor.checked == [5040.0]
        assert monitor.excursion_monitor.updated == [5040.0]


class TestShadowTrailingDuringSpreadHour:
    def test_non_crypto_shadow_sits_the_window_out(self):
        monitor = _monitor()
        signal = _hit_signal()
        signal.shadow_only = True
        _drive(monitor, signal, is_spread_hour=True)
        assert monitor.trailing_monitor.updated == []

    def test_shadow_runs_outside_the_window(self):
        monitor = _monitor()
        signal = _hit_signal()
        signal.shadow_only = True
        _drive(monitor, signal, is_spread_hour=False)
        assert monitor.trailing_monitor.updated == [5040.0]
