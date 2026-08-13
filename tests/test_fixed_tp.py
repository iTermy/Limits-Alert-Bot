"""AutoTPMonitor tests for signals carrying their own take-profit price.

A signal with `take_profit` set exits at that price and ignores the configured
TP threshold entirely; every other signal keeps the threshold behaviour.
"""

import asyncio

from models.signal import LimitData, SignalData
from price_feeds.config.tp_config import TPConfig
from price_feeds.monitors.tp_monitor import AutoTPMonitor


class _StubSignalDB:
    """Records the status write the monitor makes when TP fires."""

    def __init__(self, signal):
        self.signal = signal
        self.profit_calls = []

    async def get_hit_limits_for_signal(self, signal_id):
        return [lim for lim in self.signal.limits if lim.status == "hit"]

    async def get_signal_with_limits(self, signal_id):
        return self.signal

    async def manually_set_signal_status(self, signal_id, status, reason=None, **kwargs):
        self.profit_calls.append({"status": status, "tp_price": kwargs.get("tp_price")})
        return True


def _build(direction, entry, take_profit=None, tp_value=5.0, tmp_path=None):
    signal = SignalData(
        signal_id=1,
        instrument="XAUUSD",
        direction=direction,
        status="hit",
        type="pa",
        stop_loss=entry - 20 if direction == "long" else entry + 20,
        take_profit=take_profit,
        total_limits=1,
        limits_hit=1,
        limits=[
            LimitData(id=10, signal_id=1, price_level=entry, sequence_number=1, status="hit")
        ],
    )
    tp_config = TPConfig(config_path=str(tmp_path / "tp.json"))
    tp_config.set_override("XAUUSD", tp_value, "dollars", signal_type="pa")
    signal_db = _StubSignalDB(signal)
    monitor = AutoTPMonitor(tp_config, signal_db, db=None, alert_system=None)
    asyncio.run(monitor.refresh_hit_limits(1))
    return monitor, signal, signal_db


class TestFixedTakeProfit:
    def test_long_fires_only_at_the_target(self, tmp_path):
        monitor, signal, signal_db = _build("long", 5000.0, take_profit=5100.0, tmp_path=tmp_path)

        assert asyncio.run(monitor.check_signal(signal, current_bid=5099.9)) is False
        assert asyncio.run(monitor.check_signal(signal, current_bid=5100.0)) is True
        assert signal_db.profit_calls == [{"status": "profit", "tp_price": 5100.0}]

    def test_short_fires_only_at_the_target(self, tmp_path):
        monitor, signal, signal_db = _build("short", 5000.0, take_profit=4900.0, tmp_path=tmp_path)

        assert asyncio.run(monitor.check_signal(signal, current_bid=4900.1)) is False
        assert asyncio.run(monitor.check_signal(signal, current_bid=4900.0)) is True
        assert signal_db.profit_calls[0]["tp_price"] == 4900.0

    def test_threshold_does_not_close_a_fixed_tp_signal(self, tmp_path):
        # $50 into a $5-threshold trade: a threshold signal would have closed
        # long ago, but this one runs to its stated target.
        monitor, signal, signal_db = _build(
            "long", 5000.0, take_profit=5100.0, tp_value=5.0, tmp_path=tmp_path
        )

        assert asyncio.run(monitor.check_signal(signal, current_bid=5050.0)) is False
        assert signal_db.profit_calls == []


class TestThresholdSignalsUnchanged:
    def test_threshold_still_fires_without_a_take_profit(self, tmp_path):
        monitor, signal, signal_db = _build(
            "long", 5000.0, take_profit=None, tp_value=5.0, tmp_path=tmp_path
        )

        assert asyncio.run(monitor.check_signal(signal, current_bid=5004.0)) is False
        assert asyncio.run(monitor.check_signal(signal, current_bid=5005.0)) is True
        assert signal_db.profit_calls[0]["status"] == "profit"
