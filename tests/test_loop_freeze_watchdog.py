"""Tests for recovery when the asyncio loop itself is frozen."""

from types import SimpleNamespace

import pytest

from price_feeds.monitors import feed_health_monitor as health_module


def test_freeze_watchdog_relaunches_without_logging(monkeypatch):
    monitor = object.__new__(health_module.FeedHealthMonitor)
    monitor._loop_heartbeat = 0.0
    monitor._loop_watchdog_stop = SimpleNamespace(wait=lambda _timeout: False)
    events = []
    monitor._dump_all_thread_stacks = lambda: events.append("dump")

    monkeypatch.setattr(health_module.time, "monotonic", lambda: 1000.0)
    monkeypatch.setattr(
        health_module, "_relaunch_process", lambda: events.append("relaunch")
    )

    def fake_exit(code):
        events.append(("exit", code))
        raise SystemExit(code)

    monkeypatch.setattr(health_module.os, "_exit", fake_exit)

    with pytest.raises(SystemExit):
        monitor._loop_watchdog_run()

    assert events == ["dump", "relaunch", ("exit", 1)]
