"""Exporter tests independent of Discord, MT5, and live database credentials."""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from prometheus_client import generate_latest

from core.metrics import Monitoring


def test_snapshot_resets_after_bot_restart():
    metrics = Monitoring()
    health = SimpleNamespace(running=True, feed_status={"oanda": "down", "binance": "idle"})
    alerts = SimpleNamespace(_pending_live_updates={1: None}, _delivery_retry_tasks={2: None})
    metrics.bot = SimpleNamespace(
        is_ready=lambda: True,
        latency=0.1,
        monitor=SimpleNamespace(
            health_monitor=health, alert_system=alerts, active_signals={1: None}
        ),
    )
    metrics.sample()
    output = generate_latest(metrics.registry).decode()
    assert 'limits_feed_down{feed="oanda"} 1.0' in output
    assert 'limits_feed_down{feed="binance"} 0.0' in output
    assert "limits_tracked_signals 1.0" in output
    metrics.bot = None
    metrics.sample()
    output = generate_latest(metrics.registry).decode()
    assert "limits_discord_ready 0.0" in output
    assert "limits_alert_retries 0.0" in output
    assert "limits_feed_down{feed=" not in output


@pytest.mark.asyncio
async def test_http_scrape():
    metrics = Monitoring()
    app = web.Application()
    app.router.add_get("/metrics", metrics.scrape)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/metrics")
        assert response.status == 200
        assert response.headers["Content-Type"].startswith("text/plain")
        assert "limits_discord_ready 0.0" in await response.text()
        assert (await client.get("/missing")).status == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_database_probe_and_cancellation(monkeypatch, failed):
    fetch = AsyncMock(side_effect=ConnectionError() if failed else None, return_value=1)
    monkeypatch.setitem(
        sys.modules,
        "database",
        SimpleNamespace(db=SimpleNamespace(_pool=SimpleNamespace(fetchval=fetch))),
    )
    metrics = Monitoring()
    metrics.task = asyncio.create_task(metrics.poll())
    for _ in range(100):
        if fetch.await_count:
            break
        await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert fetch.await_count == 1
    assert metrics.gauges["database_up"]._value.get() == int(not failed)
    await metrics.close()
    assert metrics.task.cancelled()


@pytest.mark.asyncio
async def test_disabled_exporter(monkeypatch):
    monkeypatch.setenv("METRICS_ENABLED", "false")
    metrics = Monitoring()
    await metrics.start()
    assert metrics.runner is None
    assert metrics.task is None
    await metrics.close()
