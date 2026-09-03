"""Compare the legacy and coalesced Discord live-update schedulers.

This is a deterministic workload model, not a request generator for Discord.
One simulated second is scaled down to a few milliseconds. Both schedulers use
the same FIFO Discord channel bucket, price ticks, outage, and critical event.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
from collections import deque
from dataclasses import asdict, dataclass


@dataclass
class Request:
    kind: str
    queued_at: float
    payload_at: float
    future: asyncio.Future


@dataclass
class Result:
    scheduler: str
    signals: int
    cosmetic_requests: int
    max_app_cosmetic_in_flight: int
    max_discord_queue_depth: int
    rate_limit_waits: int
    rate_limit_wait_seconds: float
    network_stalls: int
    mean_payload_age_seconds: float
    p95_payload_age_seconds: float
    max_payload_age_seconds: float
    critical_delivery_latency_seconds: float


class Clock:
    def __init__(self, scale: float):
        self.scale = scale
        self.started = asyncio.get_running_loop().time()

    def now(self) -> float:
        return (asyncio.get_running_loop().time() - self.started) / self.scale

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(seconds * self.scale, 0))


class DiscordChannelBucket:
    """FIFO shared-channel bucket with one external-network outage."""

    def __init__(
        self,
        clock: Clock,
        *,
        capacity: int = 5,
        window_seconds: float = 5,
        outage_start: float = 27,
        outage_end: float = 55,
    ):
        self.clock = clock
        self.capacity = capacity
        self.window_seconds = window_seconds
        self.outage_start = outage_start
        self.outage_end = outage_end
        self.queue: asyncio.Queue[Request] = asyncio.Queue()
        self.delivered_at: deque[float] = deque()
        self.payload_ages: list[float] = []
        self.cosmetic_requests = 0
        self.max_queue_depth = 0
        self.rate_limit_waits = 0
        self.rate_limit_wait_seconds = 0.0
        self.network_stalls = 0
        self.critical_latency = math.nan
        self._worker = asyncio.create_task(self._run())

    async def send(self, kind: str, payload_at: float) -> None:
        future = asyncio.get_running_loop().create_future()
        request = Request(kind, self.clock.now(), payload_at, future)
        await self.queue.put(request)
        self.max_queue_depth = max(self.max_queue_depth, self.queue.qsize())
        if kind == "cosmetic":
            self.cosmetic_requests += 1
        await future

    async def close(self) -> None:
        await self.queue.join()
        self._worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._worker

    async def _run(self) -> None:
        while True:
            request = await self.queue.get()
            try:
                stalled = False
                while True:
                    now = self.clock.now()
                    if self.outage_start <= now < self.outage_end:
                        if not stalled:
                            self.network_stalls += 1
                            stalled = True
                        await self.clock.sleep(self.outage_end - now)
                        continue

                    while self.delivered_at and now - self.delivered_at[0] >= self.window_seconds:
                        self.delivered_at.popleft()
                    if len(self.delivered_at) < self.capacity:
                        break
                    self.rate_limit_waits += 1
                    wait = self.window_seconds - (now - self.delivered_at[0])
                    self.rate_limit_wait_seconds += max(wait, 0)
                    await self.clock.sleep(wait)

                # Yield without advancing simulated time; API latency is not the
                # variable under test, while queue and Retry-After time are.
                await asyncio.sleep(0)
                delivered = self.clock.now()
                self.delivered_at.append(delivered)
                if request.kind == "cosmetic":
                    self.payload_ages.append(delivered - request.payload_at)
                else:
                    self.critical_latency = delivered - request.queued_at
                request.future.set_result(None)
            finally:
                self.queue.task_done()


class AppConcurrency:
    def __init__(self):
        self.current = 0
        self.maximum = 0

    async def send(self, bucket: DiscordChannelBucket, payload_at: float) -> None:
        self.current += 1
        self.maximum = max(self.maximum, self.current)
        try:
            await bucket.send("cosmetic", payload_at)
        finally:
            self.current -= 1


async def run_legacy(signal_count: int, clock: Clock, bucket: DiscordChannelBucket) -> int:
    """Legacy: wait 15 s, render a full batch, submit five concurrently."""
    concurrency = AppConcurrency()
    semaphore = asyncio.Semaphore(5)

    async def refresh_one() -> None:
        async with semaphore:
            # Legacy payloads are rendered before entering discord.py's queue.
            payload_at = clock.now()
            await concurrency.send(bucket, payload_at)

    async def scheduler() -> None:
        while True:
            await clock.sleep(15)
            if clock.now() >= 90:
                return
            await asyncio.gather(*(refresh_one() for _ in range(signal_count)))

    async def critical_event() -> None:
        await clock.sleep(40)
        await bucket.send("critical", clock.now())

    await asyncio.gather(scheduler(), critical_event())
    return concurrency.maximum


async def run_coalesced(signal_count: int, clock: Clock, bucket: DiscordChannelBucket) -> int:
    """Current: one sequential snapshot pass followed by a 30 s cooldown."""
    concurrency = AppConcurrency()
    priority_active = False

    async def scheduler() -> None:
        while True:
            await clock.sleep(30)
            if clock.now() >= 90:
                return
            for _ in range(signal_count):
                if priority_active:
                    # The production worker drops the rest of a cosmetic pass
                    # when a hit/SL/TP event needs the shared HTTP bucket.
                    break
                # Current payload is rendered only when its turn arrives.
                await concurrency.send(bucket, clock.now())

    async def critical_event() -> None:
        nonlocal priority_active
        await clock.sleep(40)
        priority_active = True
        try:
            await bucket.send("critical", clock.now())
        finally:
            priority_active = False

    await asyncio.gather(scheduler(), critical_event())
    return concurrency.maximum


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(math.ceil(len(ordered) * fraction) - 1, len(ordered) - 1)
    return ordered[max(index, 0)]


async def run_case(scheduler: str, signal_count: int, scale: float) -> Result:
    clock = Clock(scale)
    bucket = DiscordChannelBucket(clock)
    if scheduler == "legacy":
        max_in_flight = await run_legacy(signal_count, clock, bucket)
    else:
        max_in_flight = await run_coalesced(signal_count, clock, bucket)
    await bucket.close()

    ages = bucket.payload_ages
    return Result(
        scheduler=scheduler,
        signals=signal_count,
        cosmetic_requests=bucket.cosmetic_requests,
        max_app_cosmetic_in_flight=max_in_flight,
        max_discord_queue_depth=bucket.max_queue_depth,
        rate_limit_waits=bucket.rate_limit_waits,
        rate_limit_wait_seconds=round(bucket.rate_limit_wait_seconds, 2),
        network_stalls=bucket.network_stalls,
        mean_payload_age_seconds=round(sum(ages) / len(ages), 2),
        p95_payload_age_seconds=round(percentile(ages, 0.95), 2),
        max_payload_age_seconds=round(max(ages), 2),
        critical_delivery_latency_seconds=round(bucket.critical_latency, 2),
    )


async def benchmark(scale: float) -> list[Result]:
    results = []
    for signal_count in (5, 10):
        for scheduler in ("legacy", "coalesced"):
            results.append(await run_case(scheduler, signal_count, scale))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float, default=0.005)
    args = parser.parse_args()
    results = asyncio.run(benchmark(args.scale))
    print(json.dumps([asdict(result) for result in results], indent=2))


if __name__ == "__main__":
    main()
