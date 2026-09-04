"""Recovery-path tests for a single dead price feed.

On 2026-08-27 a five-minute DNS outage killed the OANDA reader loop and it
never came back: the health monitor spent its three reconnect attempts inside
the outage and then stopped trying, every later reconnect reported success
because the HTTP session rebuilt fine, the reader loop itself was never
restarted, and the global price-flow watchdog could not see it because the
other three feeds kept ticking. OANDA carries every index, so signals 3935 and
4003 went unmonitored for 28 h until a manual restart. Each test below pins one
of the links in that chain.
"""

import asyncio
import contextlib
from collections import defaultdict
from datetime import datetime, timedelta

import pytest
import pytz

from price_feeds.config.symbol_mapper import SymbolMapper
from price_feeds.feeds import price_stream_manager as psm
from price_feeds.monitors import feed_health_monitor as fhm

FEED = "oanda"
SYMBOL = "NAS100USD"


class FakeFeed:
    """A feed whose stream ends immediately, the way OANDA's does when it has
    been disconnected out from under the reader."""

    def __init__(self, connected=True):
        self.connected = connected
        self.reconnects = 0
        self.streams = 0

    async def stream_prices(self):
        self.streams += 1
        return
        yield  # pragma: no cover — makes this an async generator

    async def reconnect(self):
        self.reconnects += 1
        return self.connected


def make_manager(feed, routed=True):
    """A stream manager with only the fields the reader loop touches."""
    manager = psm.PriceStreamManager.__new__(psm.PriceStreamManager)
    manager.feeds = {FEED: feed}
    manager.feed_status = {FEED: True}
    manager._feed_tasks = {}
    manager.symbol_to_feed = {SYMBOL: FEED} if routed else {}
    manager.stats = {"errors": 0, "reconnections": 0}
    return manager


def make_monitor():
    """A health monitor with only the fields the reconnect path touches."""
    monitor = fhm.FeedHealthMonitor.__new__(fhm.FeedHealthMonitor)
    monitor.last_seen = defaultdict(dict)
    monitor.feed_status = {}
    monitor.first_stale_time = {}
    monitor.reconnect_attempts = defaultdict(int)
    monitor.last_reconnect_time = {}
    monitor.startup_time = datetime.now()
    monitor.est = pytz.timezone("America/New_York")
    monitor.symbol_mapper = SymbolMapper()
    monitor._watchdog_fired = False
    monitor.stats = {
        "checks_performed": 0,
        "stale_detections": 0,
        "reconnections_attempted": 0,
        "reconnections_successful": 0,
        "alerts_sent": 0,
    }
    return monitor


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch):
    """Collapse the production backoffs so the loops run at test speed."""
    monkeypatch.setattr(psm, "RECONNECT_BACKOFF_START_SECONDS", 0)
    monkeypatch.setattr(psm, "RECONNECT_BACKOFF_MAX_SECONDS", 0)
    monkeypatch.setattr(fhm, "RECONNECT_DELAY_SECONDS", 0)


async def run_briefly(coro, seconds=0.25):
    """Run a never-ending loop for a moment, then cancel it."""
    task = asyncio.create_task(coro)
    await asyncio.sleep(seconds)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── The reader task must not be collectable ─────────────────────────────────


@pytest.mark.asyncio
async def test_feed_task_is_referenced():
    """asyncio keeps only a weak reference to a running task, so a reader that
    nobody holds can be garbage-collected mid-flight — a feed disappearing with
    no error logged anywhere."""
    manager = make_manager(FakeFeed())

    manager._start_feed_task(FEED)
    task = manager._feed_tasks[FEED]

    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_reconnect_restarts_a_dead_reader():
    """Every feed's reconnect() disconnects first, which ends the stream
    generator. Rebuilding the session without restarting the reader leaves a
    connected feed that delivers nothing — exactly what the logs showed."""
    feed = FakeFeed()
    manager = make_manager(feed)

    assert FEED not in manager._feed_tasks

    assert await manager.reconnect_feed(FEED) is True

    task = manager._feed_tasks[FEED]
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ── A stream that ends without yielding is an outage, not a no-op ───────────


@pytest.mark.asyncio
async def test_silent_stream_end_triggers_reconnect():
    """The old `else` branch slept a second and retried forever without logging
    or reconnecting, so a feed could sit silent indefinitely."""
    feed = FakeFeed()
    manager = make_manager(feed, routed=True)

    await run_briefly(manager._run_feed_stream(FEED))

    assert feed.reconnects > 0
    assert manager.stats["errors"] > 0


@pytest.mark.asyncio
async def test_unrouted_feed_stays_idle():
    """Before bulk_subscribe runs, a feed legitimately has nothing to stream.
    That must not be counted as a failure or reconnected."""
    feed = FakeFeed()
    manager = make_manager(feed, routed=False)

    await run_briefly(manager._run_feed_stream(FEED))

    assert feed.reconnects == 0
    assert manager.stats["errors"] == 0


# ── A rebuilt session is not a working feed ─────────────────────────────────


class FakeStreamManager:
    def __init__(self, monitor, tick_on_reconnect):
        self.monitor = monitor
        self.tick_on_reconnect = tick_on_reconnect

    async def reconnect_feed(self, feed_name):
        if self.tick_on_reconnect:
            self.monitor.last_seen[feed_name][SYMBOL] = datetime.now()
        return True


@pytest.mark.asyncio
async def test_reconnect_without_ticks_is_not_success(monkeypatch):
    """OANDA logged three successful reconnects while delivering nothing, which
    is why the outage read as handled."""
    monkeypatch.setattr(fhm, "RECONNECT_TICK_TIMEOUT_SECONDS", 0)

    monitor = make_monitor()
    monitor.stream_manager = FakeStreamManager(monitor, tick_on_reconnect=False)

    assert await monitor.attempt_reconnection(FEED) is False
    assert monitor.stats["reconnections_successful"] == 0


@pytest.mark.asyncio
async def test_reconnect_with_ticks_is_success(monkeypatch):
    monkeypatch.setattr(fhm, "RECONNECT_TICK_TIMEOUT_SECONDS", 5)

    monitor = make_monitor()
    monitor.stream_manager = FakeStreamManager(monitor, tick_on_reconnect=True)

    assert await monitor.attempt_reconnection(FEED) is True
    assert monitor.stats["reconnections_successful"] == 1


# ── Retries slow down, but never stop ───────────────────────────────────────


def test_reconnect_is_retried_after_the_attempt_budget():
    """The hard stop at MAX_RECONNECT_ATTEMPTS is what made the outage
    permanent: the budget was spent during a five-minute DNS failure and no
    attempt was ever made again once the network came back."""
    monitor = make_monitor()
    monitor.reconnect_attempts[FEED] = fhm.MAX_RECONNECT_ATTEMPTS

    monitor.last_reconnect_time[FEED] = datetime.now()
    assert monitor._may_attempt_reconnect(FEED) is False

    monitor.last_reconnect_time[FEED] = datetime.now() - timedelta(
        seconds=fhm.RECONNECT_RETRY_INTERVAL_SECONDS + 1
    )
    assert monitor._may_attempt_reconnect(FEED) is True


def test_early_failures_retry_every_cycle():
    monitor = make_monitor()
    monitor.reconnect_attempts[FEED] = 1
    monitor.last_reconnect_time[FEED] = datetime.now()

    assert monitor._may_attempt_reconnect(FEED) is True


# ── One dead feed among four must still trip a watchdog ─────────────────────


@pytest.mark.asyncio
async def test_dead_feed_watchdog_fires(monkeypatch):
    """The price-flow watchdog needs every feed silent, so it never saw OANDA
    die while ICMarkets, Binance and Exness kept the bot looking alive."""
    monitor = make_monitor()
    monitor.startup_time = datetime.now() - timedelta(seconds=fhm.WATCHDOG_GRACE_SECONDS + 1)
    monitor.feed_status = {"icmarkets": "healthy", FEED: "down"}
    monitor.first_stale_time[FEED] = datetime.now() - timedelta(
        seconds=fhm.FEED_DOWN_RESTART_SECONDS + 1
    )

    restarts = []
    monkeypatch.setattr(
        fhm.FeedHealthMonitor,
        "_watchdog_restart",
        lambda self, alert: restarts.append(alert) or asyncio.sleep(0),
    )

    await monitor._check_dead_feed_watchdog(datetime.now())
    await asyncio.sleep(0)

    assert monitor._watchdog_fired is True
    assert len(restarts) == 1
    assert FEED.upper() in restarts[0]


@pytest.mark.asyncio
async def test_dead_feed_watchdog_holds_below_the_threshold():
    monitor = make_monitor()
    monitor.startup_time = datetime.now() - timedelta(seconds=fhm.WATCHDOG_GRACE_SECONDS + 1)
    monitor.feed_status = {FEED: "down"}
    monitor.first_stale_time[FEED] = datetime.now() - timedelta(
        seconds=fhm.FEED_DOWN_RESTART_SECONDS - 60
    )

    await monitor._check_dead_feed_watchdog(datetime.now())

    assert monitor._watchdog_fired is False


# ── A shrinking stale list is not evidence of recovery ──────────────────────


@pytest.mark.asyncio
async def test_down_feed_is_held_down_until_a_real_tick(monkeypatch):
    """A symbol whose market closes stops counting as stale, so a dead feed
    drains its own stale list overnight. That is what cleared OANDA's down
    state at 01:00 on 2026-08-28 while it stayed silent another 18 h."""
    now = datetime.now()
    stale_threshold = timedelta(seconds=fhm.STALE_THRESHOLD_SECONDS)

    monitor = make_monitor()
    monitor.feed_status[FEED] = "down"
    monitor.stream_manager = type("S", (), {"subscribed_symbols": {SYMBOL, "SPX500USD"}})()
    # Both symbols long silent; only one still has an open market, so the stale
    # list is shorter than the symbol list and the feed used to read healthy.
    monitor.last_seen[FEED] = {
        SYMBOL: now - timedelta(hours=8),
        "SPX500USD": now - timedelta(hours=8),
    }
    monkeypatch.setattr(
        fhm.FeedHealthMonitor, "is_market_open", lambda self, ac, at=None: False
    )

    written = []
    monkeypatch.setattr(
        fhm.FeedHealthMonitor,
        "_write_feed_health",
        lambda self, feed, status, secs, seen: written.append(status) or asyncio.sleep(0),
    )
    monkeypatch.setattr(
        fhm.FeedHealthMonitor,
        "_handle_feed_recovery",
        lambda self, feed: pytest.fail("recovered without a tick"),
    )

    await monitor._check_feed(FEED, stale_threshold, now)

    assert monitor.feed_status[FEED] == "down"
    assert written == ["down"]
