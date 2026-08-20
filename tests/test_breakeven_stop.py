"""Breakeven-stop tests.

A signal armed with "set be" closes flat when price reverses to the mean of its
filled limits, instead of riding down to the stop loss.
"""

import asyncio

from models.signal import LimitData, SignalData, breakeven_price
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor


class _StubAlertSystem:
    def __init__(self):
        self._live_embeds = {}
        self.calls = []

    async def send_breakeven_stop_alert(self, signal, current_price, be_price):
        self.calls.append({"price": current_price, "be_price": be_price})
        return True


class _StubSignalDB:
    def __init__(self, ok=True):
        self.ok = ok
        self.status_calls = []

    async def manually_set_signal_status(self, signal_id, status, reason=None, **kwargs):
        self.status_calls.append({"status": status, "reason": reason, **kwargs})
        return self.ok


class _StubTracker:
    """Stands in for the tp / nm / trailing / excursion monitors."""

    def __init__(self):
        self.finalized = []

    def evict_signal(self, signal_id):
        pass

    async def finalize_with_price(self, signal_id, price, reason):
        self.finalized.append((price, reason))

    async def finalize(self, signal_id, price, reason):
        self.finalized.append((price, reason))


class _StubStreamManager:
    async def unsubscribe_symbol(self, symbol):
        pass


def _build(direction, entries, armed=True, db_ok=True):
    """A HIT signal on XAUUSD with `entries` filled, plus a monitor to drive it."""
    signal = SignalData(
        signal_id=1,
        instrument="XAUUSD",
        direction=direction,
        status="hit",
        type="pa",
        stop_loss=min(entries) - 20 if direction == "long" else max(entries) + 20,
        be_stop_armed_at="2026-08-13T12:00:00+00:00" if armed else None,
        total_limits=len(entries),
        limits_hit=len(entries),
        limits=[
            LimitData(id=10 + i, signal_id=1, price_level=e, sequence_number=i + 1, status="hit")
            for i, e in enumerate(entries)
        ],
    )
    alert_system = _StubAlertSystem()
    signal_db = _StubSignalDB(ok=db_ok)
    monitor = StreamingPriceMonitor(
        bot=None,
        signal_db=signal_db,
        db=None,
        alert_system=alert_system,
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
    monitor.active_signals[1] = signal
    return monitor, signal, signal_db, alert_system


def _check(monitor, signal, bid, is_spread_hour=False):
    return asyncio.run(monitor._check_breakeven_stop(signal, bid, is_spread_hour))


class TestBreakevenPrice:
    def test_single_fill_is_its_own_breakeven(self):
        assert breakeven_price([LimitData(price_level=5000.0, status="hit")]) == 5000.0

    def test_multiple_fills_average(self):
        limits = [
            LimitData(price_level=2000.0, status="hit"),
            LimitData(price_level=1990.0, status="hit"),
        ]
        assert breakeven_price(limits) == 1995.0

    def test_no_fills_has_no_breakeven(self):
        assert breakeven_price([]) is None


class TestBreakevenStopTrigger:
    def test_long_closes_flat_on_a_reversal(self):
        monitor, signal, signal_db, alerts = _build("long", [5000.0])

        # $5 in profit — the floor is not in play.
        assert _check(monitor, signal, 5005.0) is False
        assert signal_db.status_calls == []

        # Back to entry: close at breakeven rather than run to the stop loss.
        assert _check(monitor, signal, 5000.0) is True
        assert signal_db.status_calls[0]["status"] == "breakeven"
        assert signal_db.status_calls[0]["closed_reason"] == "automatic"
        assert alerts.calls == [{"price": 5000.0, "be_price": 5000.0}]
        assert signal.status == "breakeven"

    def test_short_closes_flat_on_a_reversal(self):
        monitor, signal, signal_db, alerts = _build("short", [5000.0])

        assert _check(monitor, signal, 4995.0) is False
        assert _check(monitor, signal, 5000.0) is True
        assert signal_db.status_calls[0]["status"] == "breakeven"

    def test_breakeven_is_the_mean_of_every_fill(self):
        monitor, signal, signal_db, alerts = _build("long", [2000.0, 1990.0])

        # 1996 still nets +2 across the two fills, so the floor holds.
        assert _check(monitor, signal, 1996.0) is False
        # 1995 is exactly flat: -5 on the first fill, +5 on the second.
        assert _check(monitor, signal, 1995.0) is True
        assert alerts.calls[0]["be_price"] == 1995.0

    def test_exit_price_reaches_the_trackers(self):
        monitor, signal, _, _ = _build("long", [5000.0])
        _check(monitor, signal, 4999.0)

        assert monitor.trailing_monitor.finalized == [(4999.0, "be_stop")]
        assert monitor.excursion_monitor.finalized == [(4999.0, "be_stop")]


class TestBreakevenStopGuards:
    def test_unarmed_signal_rides_to_its_stop_loss(self):
        monitor, signal, signal_db, alerts = _build("long", [5000.0], armed=False)

        # Deep in loss and well past entry: without "set be" nothing intervenes.
        assert _check(monitor, signal, 4900.0) is False
        assert signal_db.status_calls == []
        assert signal.status == "hit"

    def test_a_stop_loss_on_the_same_tick_wins(self):
        monitor, signal, signal_db, alerts = _build("long", [5000.0])
        signal.sl_alert_sent = True

        # Price gapped through both levels; the real (worse) SL is already
        # booked, so the breakeven stop must not overwrite it.
        assert _check(monitor, signal, 4900.0) is False
        assert signal_db.status_calls == []

    def test_fires_only_once(self):
        monitor, signal, signal_db, alerts = _build("long", [5000.0])

        assert _check(monitor, signal, 4999.0) is True
        assert _check(monitor, signal, 4998.0) is False
        assert len(signal_db.status_calls) == 1
        assert len(alerts.calls) == 1

    def test_a_signal_with_no_fills_has_nothing_to_protect(self):
        monitor, signal, signal_db, alerts = _build("long", [5000.0])
        signal.limits[0].status = "pending"

        assert _check(monitor, signal, 4000.0) is False
        assert signal_db.status_calls == []

    def test_spread_hour_stands_the_floor_down(self):
        # The bid blows out in the window and a long fires on `bid <= be_price`, so
        # the widened spread alone would close the trade. Same rule as the stop loss.
        monitor, signal, signal_db, alerts = _build("long", [5000.0])

        assert _check(monitor, signal, 4990.0, is_spread_hour=True) is False
        assert signal_db.status_calls == []
        assert signal.status == "hit"

        # The level is re-evaluated on the first tick after the window, on a bid
        # that means something again.
        assert _check(monitor, signal, 4990.0) is True
        assert signal_db.status_calls[0]["status"] == "breakeven"

    def test_crypto_keeps_its_floor_through_spread_hour(self):
        # Crypto books stay tight through the window, so there is no artifact to dodge.
        monitor, signal, signal_db, alerts = _build("long", [60000.0])
        signal.instrument = "BTCUSDT"
        signal.asset_class = "crypto"

        assert _check(monitor, signal, 59900.0, is_spread_hour=True) is True
        assert signal_db.status_calls[0]["status"] == "breakeven"
