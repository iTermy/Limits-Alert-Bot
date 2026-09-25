"""Deleted reply targets and cancellation racing an automatic stop loss."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import pytest

from database.signal_ops import SignalDatabase
from discord_handlers.message_handler import MessageHandler
from models.signal import SignalData
from price_feeds.monitors.streaming_monitor import StreamingPriceMonitor


@pytest.mark.parametrize("missing", [True, False])
def test_signal_reply_fetch_errors(missing):
    handler = MessageHandler.__new__(MessageHandler)
    handler.logger = Mock()
    handler.signal_db = Mock()
    handler.has_bot_success_reaction = AsyncMock()
    error_type = discord.NotFound if missing else discord.Forbidden
    error = error_type(SimpleNamespace(status=404 if missing else 403, reason="test"), "test")
    message = SimpleNamespace(
        author=SimpleNamespace(bot=False),
        reference=SimpleNamespace(message_id=12),
        channel=SimpleNamespace(fetch_message=AsyncMock(side_effect=error)),
    )

    asyncio.run(handler.check_signal_management_reply(message))

    handler.has_bot_success_reaction.assert_not_awaited()
    assert handler.logger.error.called is (not missing)


@pytest.mark.parametrize("old_status", ["cancelled", "profit", "breakeven", "stop_loss"])
def test_automatic_stop_loss_rejects_closed_signal(old_status):
    database = SimpleNamespace(
        fetch_one=AsyncMock(return_value={"status": old_status}), execute=AsyncMock()
    )
    result = asyncio.run(
        SignalDatabase(database).manually_set_signal_status(
            1, "stop_loss", expected_statuses=("active", "hit")
        )
    )
    assert result is False
    database.execute.assert_not_awaited()


@pytest.mark.parametrize("written", [0, 1])
def test_stop_loss_write_result_controls_snapshot(written):
    database = SimpleNamespace(
        fetch_one=AsyncMock(
            return_value={"status": "hit", "limits_hit": 1, "instrument": "XAUUSD"}
        ),
        execute=AsyncMock(return_value=written),
    )
    signals = SignalDatabase(database)
    signals._snapshot_close_prices = AsyncMock()
    result = asyncio.run(
        signals.manually_set_signal_status(1, "stop_loss", expected_statuses=("active", "hit"))
    )
    assert result is bool(written)
    assert signals._snapshot_close_prices.await_count == written
    query, params = database.execute.call_args.args
    assert "status = ANY($8::text[]) AND status = $6" in query
    assert "SELECT id, $6, $2, $4, $7 FROM updated_signal" in query
    assert params[-1] == ["active", "hit"]


@pytest.mark.parametrize("status,accepted", [("cancelled", False), ("hit", False), ("hit", True)])
def test_stop_loss_alert_requires_accepted_transition(status, accepted):
    monitor = StreamingPriceMonitor.__new__(StreamingPriceMonitor)
    monitor._process_stop_loss_hit = AsyncMock(return_value=accepted)
    monitor.alert_system = SimpleNamespace(
        deliver_critical=AsyncMock(), send_stop_loss_alert=Mock()
    )
    monitor._react_async = Mock()
    monitor.stats = {"stop_losses_hit": 0}
    signal = SignalData(
        signal_id=1, instrument="XAUUSD", direction="long", status=status, stop_loss=90
    )

    asyncio.run(monitor._check_stop_loss(signal, 89, "long", False, False))

    assert monitor.alert_system.deliver_critical.await_count == int(accepted)
    assert monitor.stats["stop_losses_hit"] == int(accepted)
    assert signal.sl_alert_sent is accepted
    if status == "cancelled":
        monitor._process_stop_loss_hit.assert_not_awaited()


def test_tracker_failure_after_close_still_allows_alert():
    monitor = StreamingPriceMonitor.__new__(StreamingPriceMonitor)
    monitor.signal_db = SimpleNamespace(manually_set_signal_status=AsyncMock(return_value=True))
    monitor.sync_signal_status_in_memory = Mock()
    monitor.tp_monitor = SimpleNamespace(
        evict_signal=Mock(side_effect=RuntimeError("tracker failed"))
    )
    signal = SimpleNamespace(signal_id=1)

    assert asyncio.run(monitor._process_stop_loss_hit(signal, 89)) is True
    assert monitor.signal_db.manually_set_signal_status.call_args.kwargs["expected_statuses"] == (
        "active",
        "hit",
    )
