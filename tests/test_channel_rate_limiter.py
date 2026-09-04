"""Sliding-window channel budget used by the alert system."""

import asyncio

from price_feeds.alerting.channel_rate_limiter import ChannelRateLimiter

CHANNEL = 111
OTHER_CHANNEL = 222


def test_idle_channel_grants_a_full_burst_without_waiting():
    async def scenario():
        limiter = ChannelRateLimiter(capacity=5, window_seconds=5)
        waits = [await limiter.acquire(CHANNEL) for _ in range(5)]
        assert waits == [0.0] * 5

    asyncio.run(scenario())


def test_sixth_request_waits_for_the_oldest_to_age_out():
    async def scenario():
        limiter = ChannelRateLimiter(capacity=3, window_seconds=0.2)
        for _ in range(3):
            await limiter.acquire(CHANNEL)

        waited = await limiter.acquire(CHANNEL)
        assert waited > 0

        # Once the window has rolled over, the budget is available again.
        await asyncio.sleep(0.25)
        assert await limiter.acquire(CHANNEL) == 0.0

    asyncio.run(scenario())


def test_reserved_slots_are_left_for_traffic_that_cannot_wait():
    async def scenario():
        limiter = ChannelRateLimiter(capacity=5, window_seconds=0.3)
        for _ in range(4):
            assert await limiter.acquire(CHANNEL, reserve=1) == 0.0

        # The fifth slot is reserved, so a cosmetic caller waits for it even
        # though the channel budget itself still has room.
        assert await limiter.acquire(CHANNEL, reserve=1) > 0

    asyncio.run(scenario())


def test_recorded_traffic_spends_the_same_budget():
    async def scenario():
        limiter = ChannelRateLimiter(capacity=2, window_seconds=0.2)
        limiter.record(CHANNEL)
        limiter.record(CHANNEL)

        assert await limiter.acquire(CHANNEL) > 0

    asyncio.run(scenario())


def test_channels_hold_independent_budgets():
    async def scenario():
        limiter = ChannelRateLimiter(capacity=2, window_seconds=5)
        for _ in range(2):
            await limiter.acquire(CHANNEL)

        assert await limiter.acquire(OTHER_CHANNEL) == 0.0

    asyncio.run(scenario())
