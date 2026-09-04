"""Stress the live-update scheduler against a simulated Discord channel bucket.

Reproduces, without a token / database / price feed, the two symptoms seen in
production logs:

* a 429 storm on ``PATCH /channels/<alert channel>/messages/<embed>`` while N
  live embeds share one alert channel, and the long
  ``Live refresh pass complete: ... in 25.2s`` passes that follow;
* ``heartbeat blocked for more than 10 seconds`` when a log sink writes from the
  asyncio thread. ``--blocking-log direct`` restores that pre-queue behaviour;
  ``--blocking-log queued`` blocks the same sink behind today's queue handlers,
  so the difference is measurable as event-loop lag.

Usage:
    python benchmarks/live_update_stress.py --signals 12 --duration 120
    python benchmarks/live_update_stress.py --blocking-log direct --block-seconds 12
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import queue
import random
import sys
import time
from collections import deque
from logging.handlers import QueueListener
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models import LimitData, SignalData
from price_feeds.alerting.alert_system import AlertSystem
from price_feeds.alerting.channel_rate_limiter import ChannelRateLimiter
from utils.logger import _DroppingQueueHandler

# Mirrors the alert channel in config/channels.json; only used to shape log lines.
ALERT_CHANNEL_ID = 1512891614501011578
FIRST_MESSAGE_ID = 1545195965437317191

discord_http_log = logging.getLogger("discord.http")


class ChannelBucket:
    """One shared Discord channel bucket, throttling like discord.py does.

    Discord's per-channel message-edit bucket is 5 requests per 5 seconds. When
    it is exhausted discord.py logs the 429 and sleeps for Retry-After rather
    than raising, which is why production logs show warnings but no exceptions.
    """

    def __init__(self, capacity: int, window: float, latency: float):
        self.capacity = capacity
        self.window = window
        self.latency = latency
        self.delivered: deque = deque()
        self.edits = 0
        self.rate_limited = 0
        self.wait_seconds = 0.0

    async def edit(self, message_id: int) -> None:
        while True:
            now = time.monotonic()
            while self.delivered and now - self.delivered[0] >= self.window:
                self.delivered.popleft()
            if len(self.delivered) < self.capacity:
                break
            retry_after = self.window - (now - self.delivered[0]) + random.uniform(0.1, 0.9)
            self.rate_limited += 1
            self.wait_seconds += retry_after
            discord_http_log.warning(
                "We are being rate limited. PATCH "
                "https://discord.com/api/v10/channels/%s/messages/%s "
                "responded with 429. Retrying in %.2f seconds.",
                ALERT_CHANNEL_ID,
                message_id,
                retry_after,
            )
            await asyncio.sleep(retry_after)

        await asyncio.sleep(self.latency)
        self.delivered.append(time.monotonic())
        self.edits += 1


class FakeMessage:
    def __init__(self, message_id: int, bucket: ChannelBucket):
        self.id = message_id
        self.channel = SimpleNamespace(id=ALERT_CHANNEL_ID)
        self.bucket = bucket
        self.edits = 0

    async def edit(self, *, embed) -> None:
        self.edits += 1
        await self.bucket.edit(self.id)


class DriftingPriceStream:
    """Prices move every read so no refresh is skipped by the embed signature."""

    def __init__(self, instruments: list):
        self.prices = {name: 100.0 + index for index, name in enumerate(instruments)}

    async def get_latest_price(self, instrument: str) -> dict:
        self.prices[instrument] += random.uniform(-0.35, 0.35)
        price = self.prices[instrument]
        return {"bid": price - 0.02, "ask": price, "spread": 0.02}


class LoopLagMonitor:
    """Measure event-loop stalls the way discord.py's heartbeat notices them."""

    TICK = 0.2

    def __init__(self, warn_after: float = 10.0):
        self.warn_after = warn_after
        self.max_lag = 0.0
        self.stalls = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            before = time.monotonic()
            await asyncio.sleep(self.TICK)
            lag = time.monotonic() - before - self.TICK
            self.max_lag = max(self.max_lag, lag)
            if lag >= self.warn_after:
                self.stalls += 1
                print(f"  event loop blocked for {lag:.1f}s (a heartbeat would be missed)")


class BlockingStream:
    """A console that stops accepting writes - a selected Windows console, or a
    redirected pipe nobody drains."""

    def __init__(self, block_seconds: float):
        self.block_seconds = block_seconds
        self.writes = 0

    def write(self, message: str) -> int:
        self.writes += 1
        time.sleep(self.block_seconds)
        return len(message)

    def flush(self) -> None:
        return None


def install_blocking_log_sink(block_seconds: float, mode: str) -> BlockingStream:
    """Attach a log sink that stops accepting writes.

    ``direct`` puts it straight on the root logger - the shape the sinks had
    before they moved onto listener threads, where a stuck console freezes the
    asyncio thread. ``queued`` puts the same sink behind the queue handler the
    bot uses today, so the stall stays on the listener thread.
    """
    stream = BlockingStream(block_seconds)
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.WARNING)

    if mode == "direct":
        logging.getLogger().addHandler(handler)
        return stream

    sink_queue: queue.Queue = queue.Queue(maxsize=1000)
    queue_handler = _DroppingQueueHandler(sink_queue)
    queue_handler.setLevel(logging.WARNING)
    QueueListener(sink_queue, handler, respect_handler_level=True).start()
    logging.getLogger().addHandler(queue_handler)
    return stream


def build_alert_system(signal_count: int, bucket: ChannelBucket) -> AlertSystem:
    instruments = [f"STRESS{index}" for index in range(1, signal_count + 1)]
    stream = DriftingPriceStream(instruments)
    alerts = AlertSystem(bot=None, stream_manager=stream, alert_config=None)

    for index, instrument in enumerate(instruments, start=1):
        signal = SignalData(
            signal_id=index,
            instrument=instrument,
            direction="long",
            stop_loss=80.0,
            total_limits=1,
            limits=[
                LimitData(
                    id=index,
                    signal_id=index,
                    price_level=stream.prices[instrument] - 1.0,
                    sequence_number=1,
                )
            ],
        )
        alerts.signal_messages[index] = FakeMessage(FIRST_MESSAGE_ID + index, bucket)
        alerts._register_live_embed(signal, "approaching")

    return alerts


async def fire_criticals(alerts, count: int, at_seconds: float, started: float) -> list:
    """Fire N event edits mid-pass, the way _upsert_signal_message does.

    Criticals hold _priority_edits_active for their duration (which stands the
    cosmetic pass down), take the per-signal lock, and record their spend against
    the channel budget without ever waiting on it.
    """
    loop = asyncio.get_running_loop()
    await asyncio.sleep(max(at_seconds - (loop.time() - started), 0))

    latencies = []
    for signal_id in range(1, count + 1):
        queued = loop.time()
        alerts._priority_edits_active += 1
        try:
            message = alerts.signal_messages[signal_id]
            async with alerts._get_message_lock(signal_id):
                await message.edit(embed=None)
            alerts._channel_limiter.record(message.channel.id)
        finally:
            alerts._priority_edits_active -= 1
        latencies.append(loop.time() - queued)
        print(f"  CRITICAL signal {signal_id} delivered in {latencies[-1]:.2f}s")
    return latencies


async def run(args: argparse.Namespace) -> None:
    bucket = ChannelBucket(args.capacity, args.window, args.latency)
    alerts = build_alert_system(args.signals, bucket)
    alerts.LIVE_UPDATE_INTERVAL = args.interval
    if args.unpaced:
        alerts._channel_limiter = ChannelRateLimiter(capacity=10_000)

    lag = LoopLagMonitor()
    lag.start()
    started = asyncio.get_running_loop().time()
    alerts.start_live_updates()

    critical_latencies = []
    if args.criticals:
        critical_task = asyncio.create_task(
            fire_criticals(alerts, args.criticals, args.critical_at, started)
        )
        await asyncio.sleep(args.duration)
        critical_latencies = await critical_task
    else:
        await asyncio.sleep(args.duration)

    alerts.stop_live_updates()
    await lag.stop()

    print("\n--- stress result ---")
    print(f"signals            : {args.signals}")
    print(f"duration           : {args.duration}s at a {args.interval}s refresh interval")
    budget = alerts._channel_limiter
    print(f"channel budget     : {budget.capacity} per {budget.window_seconds}s")
    print(f"edits delivered    : {bucket.edits}")
    print(f"429 responses      : {bucket.rate_limited}")
    print(f"time spent waiting : {bucket.wait_seconds:.1f}s")
    print(f"max event-loop lag : {lag.max_lag:.2f}s ({lag.stalls} stalls over 10s)")
    if critical_latencies:
        worst = max(critical_latencies)
        mean = sum(critical_latencies) / len(critical_latencies)
        print(f"critical events    : {len(critical_latencies)} fired at t={args.critical_at}s")
        print(f"critical latency   : mean {mean:.2f}s, worst {worst:.2f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=int, default=12, help="live embeds sharing one channel")
    parser.add_argument("--duration", type=float, default=120, help="seconds to run")
    parser.add_argument("--interval", type=float, default=30, help="LIVE_UPDATE_INTERVAL override")
    parser.add_argument("--capacity", type=int, default=5, help="edits allowed per window")
    parser.add_argument("--window", type=float, default=5.0, help="rate-limit window, seconds")
    parser.add_argument("--latency", type=float, default=0.25, help="simulated API latency")
    parser.add_argument(
        "--criticals", type=int, default=0, help="event edits (cancel/SL/TP) to fire mid-run"
    )
    parser.add_argument(
        "--critical-at", type=float, default=18.0, help="when the event edits fire, seconds"
    )
    parser.add_argument(
        "--unpaced",
        action="store_true",
        help="give the channel budget unlimited room, restoring the pre-fix behaviour",
    )
    parser.add_argument(
        "--blocking-log",
        choices=("direct", "queued"),
        help=(
            "install a log sink whose writes block: 'direct' reproduces the pre-fix "
            "freeze, 'queued' keeps the stall off the asyncio thread"
        ),
    )
    parser.add_argument("--block-seconds", type=float, default=12.0)
    args = parser.parse_args()

    if args.blocking_log:
        install_blocking_log_sink(args.block_seconds, args.blocking_log)
        print(
            f"Blocking {args.blocking_log} log sink installed: "
            f"every WARNING stalls its thread {args.block_seconds}s"
        )

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
