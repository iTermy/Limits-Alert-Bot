"""News mode pauses clients without changing alert-bot signal processing."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.news_manager import NewsEvent, NewsManager
from models.signal import LimitData, SignalData
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor


def _news_manager(*, client_only=True):
    manager = NewsManager()
    manager._events = [
        NewsEvent(
            category="ALL",
            news_time=datetime.now(timezone.utc),
            window_minutes=10,
            created_by="test",
            client_only=client_only,
        )
    ]
    return manager


class _Alerts:
    def __init__(self):
        self.hit_calls = []
        self.approach_calls = []

    async def deliver_critical(self, _key, operation, *args, **kwargs):
        return await operation(*args, **kwargs)

    async def send_limit_hit_alert(self, signal, limit, current_price, **kwargs):
        self.hit_calls.append((signal.signal_id, limit.id, current_price, kwargs))
        return True

    async def send_approaching_alert(
        self, signal, limit, current_price, distance_formatted, **kwargs
    ):
        self.approach_calls.append(
            (signal.signal_id, limit.id, current_price, distance_formatted, kwargs)
        )
        return True


class _AlertConfig:
    def get_approaching_distance(self, _symbol, current_price=None):
        return 2.0

    def format_distance_for_display(self, _symbol, distance, _current_price):
        return str(distance)


class _Tracker:
    def __init__(self):
        self.check_calls = []
        self.entry_calls = []
        self.start_calls = []
        self.update_calls = []

    async def check_signal(self, signal, current_bid):
        self.check_calls.append((signal.signal_id, current_bid))
        return False

    async def mark_entry(self, signal):
        self.entry_calls.append(signal.signal_id)

    async def start_approach(self, signal, current_price):
        self.start_calls.append((signal.signal_id, current_price))

    async def update(self, *args):
        self.update_calls.append(args)
        return False


def _monitor(*, client_only=True):
    alerts = _Alerts()
    tp = _Tracker()
    excursion = _Tracker()
    monitor = StreamingPriceMonitor(
        bot=SimpleNamespace(news_manager=_news_manager(client_only=client_only)),
        signal_db=None,
        db=None,
        alert_system=alerts,
        stream_manager=None,
        alert_config=_AlertConfig(),
        tp_config=None,
        tp_monitor=tp,
        nm_config=None,
        nm_monitor=_Tracker(),
        trailing_monitor=_Tracker(),
        excursion_monitor=excursion,
        live_price_writer=None,
        health_monitor=None,
    )
    monitor._cancel_signal_during_guard = AsyncMock()
    monitor._react_async = lambda *_args: None
    return monitor, alerts, tp, excursion


def test_dry_run_is_visible_to_clients_but_not_the_alert_bot():
    manager = _news_manager(client_only=True)

    assert manager._compute_news_mode_value() == "ALL"
    assert manager.is_news_active_for("EURUSD") is not None
    assert manager.is_alert_bot_news_active_for("EURUSD") is None


def test_normal_news_still_guards_the_alert_bot():
    manager = _news_manager(client_only=False)

    assert manager.is_news_active_for("EURUSD") is not None
    assert manager.is_alert_bot_news_active_for("EURUSD") is not None


def test_limit_hit_is_processed_during_active_news():
    monitor, alerts, _, _ = _monitor()
    monitor._process_limit_hit = AsyncMock()
    signal = SignalData(
        signal_id=1,
        instrument="EURUSD",
        direction="long",
        status="active",
        limits=[LimitData(id=10, signal_id=1, price_level=1.1, sequence_number=1)],
    )
    limit = signal.limits[0]

    asyncio.run(
        monitor._handle_limit_hit(
            signal,
            limit,
            current_price=1.1,
            is_spread_hour=False,
            is_late_market=False,
            spread=0.0,
            spread_buffer_enabled=False,
        )
    )

    assert limit.status == "hit"
    assert alerts.hit_calls
    monitor._process_limit_hit.assert_awaited_once()
    monitor._cancel_signal_during_guard.assert_not_awaited()


def test_approaching_alert_is_sent_during_active_news():
    monitor, alerts, _, excursion = _monitor()
    monitor._mark_approaching_sent = AsyncMock()
    signal = SignalData(signal_id=2, instrument="EURUSD", direction="long")
    limit = LimitData(id=20, signal_id=2, price_level=1.1, sequence_number=1)

    asyncio.run(
        monitor._handle_approaching(
            signal,
            limit,
            current_price=1.2,
            distance=0.1,
            is_spread_hour=False,
            is_late_market=False,
            spread=0.0,
            spread_buffer_enabled=False,
        )
    )

    assert alerts.approach_calls
    monitor._mark_approaching_sent.assert_awaited_once_with(limit.id)
    assert excursion.start_calls == [(signal.signal_id, 1.2)]


def test_open_hit_signal_keeps_running_during_active_news():
    monitor, _, tp, excursion = _monitor()
    signal = SignalData(
        signal_id=3,
        instrument="XAUUSD",
        direction="long",
        status="hit",
        type="standard",
    )

    asyncio.run(
        monitor._check_signal(
            signal,
            {"bid": 2000.0, "ask": 2000.5},
            is_spread_hour=False,
            is_late_market=False,
            spread_buffer_enabled=False,
        )
    )

    monitor._cancel_signal_during_guard.assert_not_awaited()
    assert tp.check_calls == [(signal.signal_id, 2000.0)]
    assert excursion.entry_calls == [signal.signal_id]


def test_normal_news_still_cancels_a_limit_hit():
    monitor, alerts, _, _ = _monitor(client_only=False)
    monitor._process_limit_hit = AsyncMock()
    signal = SignalData(signal_id=4, instrument="EURUSD", direction="long")
    limit = LimitData(id=40, signal_id=4, price_level=1.1, sequence_number=1)

    asyncio.run(
        monitor._handle_limit_hit(
            signal,
            limit,
            current_price=1.1,
            is_spread_hour=False,
            is_late_market=False,
            spread=0.0,
            spread_buffer_enabled=False,
        )
    )

    monitor._cancel_signal_during_guard.assert_awaited_once()
    monitor._process_limit_hit.assert_not_awaited()
    assert alerts.hit_calls == []
