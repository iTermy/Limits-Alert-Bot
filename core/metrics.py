"""Bounded operational metrics; no signal IDs, prices, or credentials exported."""

import asyncio
import math
import os
import time
from contextlib import suppress

from aiohttp import web
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)


class Monitoring:
    def __init__(self):
        self.bot = None
        self.registry = CollectorRegistry()
        self.gauges = {}
        for name, help_text in {
            "discord_ready": "Discord client is ready",
            "discord_latency_seconds": "Discord gateway heartbeat latency",
            "event_loop_lag_seconds": "Delay beyond the sampler interval",
            "database_up": "Last SELECT 1 probe succeeded",
            "database_probe_seconds": "Duration of last database probe",
            "tracked_signals": "Signals held by the price monitor",
            "monitor_running": "Price monitor health loop is running",
            "alert_queue_depth": "Pending cosmetic Discord updates",
            "alert_retries": "Pending important Discord delivery retries",
            "uptime_seconds": "Supervisor process uptime",
        }.items():
            self.gauges[name] = Gauge("limits_" + name, help_text, registry=self.registry)
        self.feed = Gauge(
            "limits_feed_down",
            "Feed health reports down (market aware)",
            ["feed"],
            registry=self.registry,
        )
        self.restarts = Counter(
            "limits_bot_restarts", "Supervisor bot restarts", registry=self.registry
        )
        self.started = time.monotonic()
        self.runner = None
        self.task = None

    def sample(self):
        bot = self.bot
        monitor = getattr(bot, "monitor", None)
        health = getattr(monitor, "health_monitor", None)
        alerts = getattr(monitor, "alert_system", None)
        latency = getattr(bot, "latency", float("nan"))
        values = {
            "discord_ready": int(bot is not None and bot.is_ready()),
            "discord_latency_seconds": latency if math.isfinite(latency) else float("nan"),
            "tracked_signals": len(getattr(monitor, "active_signals", {})),
            "monitor_running": int(bool(getattr(health, "running", False))),
            "alert_queue_depth": len(getattr(alerts, "_pending_live_updates", {})),
            "alert_retries": len(getattr(alerts, "_delivery_retry_tasks", {})),
            "uptime_seconds": time.monotonic() - self.started,
        }
        for name, value in values.items():
            self.gauges[name].set(value)
        self.feed.clear()
        for feed, status in getattr(health, "feed_status", {}).items():
            if feed in {"icmarkets", "oanda", "binance", "exness"}:
                self.feed.labels(feed).set(int(status == "down"))

    async def scrape(self, request):
        self.sample()
        return web.Response(
            body=generate_latest(self.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    async def poll(self):
        from database import db

        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            try:
                # Probe the existing pool; monitoring must never create connections.
                if db._pool is None:
                    raise ConnectionError("Pool unavailable")
                await asyncio.wait_for(db._pool.fetchval("SELECT 1"), timeout=3)
                self.gauges["database_up"].set(1)
            except Exception:
                self.gauges["database_up"].set(0)
            self.gauges["database_probe_seconds"].set(loop.time() - started)
            due = loop.time() + 5
            await asyncio.sleep(5)
            self.gauges["event_loop_lag_seconds"].set(max(0, loop.time() - due))

    async def start(self):
        if os.getenv("METRICS_ENABLED", "true").lower() in {"false", "0", "no"}:
            return
        app = web.Application()
        app.router.add_get("/metrics", self.scrape)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        try:
            await web.TCPSite(
                self.runner,
                os.getenv("METRICS_HOST", "0.0.0.0"),
                int(os.getenv("METRICS_PORT", "9108")),
            ).start()
        except Exception:
            await self.runner.cleanup()
            raise
        self.task = asyncio.create_task(self.poll())

    async def close(self):
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        if self.runner:
            await self.runner.cleanup()


monitoring = Monitoring()
