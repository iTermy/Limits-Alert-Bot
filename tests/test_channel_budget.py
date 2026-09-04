"""Per-channel Discord write budget: pacing, learning, and header parsing."""

import asyncio
import time
from types import SimpleNamespace

from price_feeds.alerting.channel_budget import (
    MAX_WINDOW_SECONDS,
    MIN_EFFECTIVE_LIMIT,
    ChannelBudget,
)
from utils.discord_http_trace import ChannelWrite, _channel_message_write

CHANNEL = 555


def _url(path: str) -> SimpleNamespace:
    return SimpleNamespace(path=path)


def _write(limit=5, remaining=2, reset_after=2.0, retry_after=0.0, channel_id=CHANNEL):
    return ChannelWrite(
        channel_id=channel_id,
        limit=limit,
        remaining=remaining,
        reset_after=reset_after,
        retry_after=retry_after,
    )


def _fresh(limit=5, reset_after=5.0, channel_id=CHANNEL):
    """A response read at the start of a new window — Reset-After is the period."""
    return _write(
        limit=limit, remaining=limit - 1, reset_after=reset_after, channel_id=channel_id
    )


class TestAllowance:
    def test_allowance_is_capacity_minus_the_event_reserve(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        assert budget.cosmetic_allowance(CHANNEL) == 3

    def test_allowance_never_falls_below_one(self):
        """A reserve wider than the bucket must not deadlock cosmetic work."""
        budget = ChannelBudget(default_limit=2, default_window=5.0, event_reserve=9)
        assert budget.cosmetic_allowance(CHANNEL) == 1


class TestPacing:
    def test_writes_within_the_allowance_do_not_wait(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)

        async def scenario():
            return [await budget.acquire(CHANNEL) for _ in range(3)]

        assert asyncio.run(scenario()) == [0.0, 0.0, 0.0]

    def test_the_write_past_the_allowance_waits_for_the_window(self):
        budget = ChannelBudget(default_limit=3, default_window=0.1, event_reserve=0)

        async def scenario():
            for _ in range(3):
                await budget.acquire(CHANNEL)
            started = time.monotonic()
            waited = await budget.acquire(CHANNEL)
            return waited, time.monotonic() - started

        waited, elapsed = asyncio.run(scenario())
        assert waited > 0
        assert elapsed >= 0.05

    def test_event_writes_are_charged_to_the_same_window(self):
        """Event traffic does not wait, but it does shrink the next sweep."""
        budget = ChannelBudget(default_limit=5, default_window=0.1, event_reserve=2)
        budget.record(CHANNEL, count=3)

        async def scenario():
            return await budget.acquire(CHANNEL)

        assert asyncio.run(scenario()) > 0

    def test_channels_have_independent_budgets(self):
        budget = ChannelBudget(default_limit=1, default_window=5.0, event_reserve=0)
        budget.record(CHANNEL)

        async def scenario():
            # A different channel is a different bucket, so it is unaffected.
            return await budget.acquire(CHANNEL + 1)

        assert asyncio.run(scenario()) == 0.0

    def test_a_window_widening_mid_wait_does_not_spin(self):
        """observe() fires on every Discord response, including mid-wait.

        Reading the window once before the loop meant a widening left the sleep
        computed from the stale (smaller) value: non-positive, so the loop
        `continue`d without ever awaiting. That is a tight spin with no yield —
        it starves the whole event loop, not just this waiter.

        The iteration cap makes that failure loud instead of a hang.
        """
        budget = ChannelBudget(default_limit=1, default_window=0.02, event_reserve=0)
        prunes = 0
        original_prune = budget._prune

        def counting_prune(channel_id):
            nonlocal prunes
            prunes += 1
            if prunes > 50:
                raise RuntimeError("acquire() is spinning without awaiting")
            original_prune(channel_id)

        budget._prune = counting_prune

        async def scenario():
            await budget.acquire(CHANNEL)
            waiter = asyncio.create_task(budget.acquire(CHANNEL))
            # Let the waiter reach its sleep, then widen the bucket under it.
            await asyncio.sleep(0)
            budget.observe(_fresh(limit=1, reset_after=0.15))
            return await waiter

        waited = asyncio.run(scenario())
        assert waited > 0

    def test_slots_free_up_once_the_window_passes(self):
        budget = ChannelBudget(default_limit=1, default_window=0.05, event_reserve=0)

        async def scenario():
            await budget.acquire(CHANNEL)
            await asyncio.sleep(0.08)
            return await budget.acquire(CHANNEL)

        assert asyncio.run(scenario()) == 0.0

    def test_a_rate_limited_channel_holds_cosmetic_work_off(self):
        """Sending during a 429's Retry-After just queues behind discord.py."""
        budget = ChannelBudget(default_limit=5, default_window=0.05, event_reserve=0)
        budget.observe(_write(retry_after=0.1, reset_after=0.05))

        async def scenario():
            return await budget.acquire(CHANNEL)

        assert asyncio.run(scenario()) > 0


class TestLearning:
    def test_a_fresh_bucket_reading_replaces_the_default(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_fresh(limit=10, reset_after=8.0))

        assert budget.limit_for(CHANNEL) == 10
        assert budget.window_for(CHANNEL) == 8.0

    def test_mid_window_reset_after_is_not_mistaken_for_the_period(self):
        """Reset-After is the remainder; only a fresh bucket states the period.

        Every Reset-After the bot saw in production was a remainder (1.0–2.7 s
        on a 5 s bucket). Taking the max over them shrank the window to 2.7 s
        and put a 429 on every sweep.
        """
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_write(remaining=3, reset_after=1.2))
        budget.observe(_write(remaining=2, reset_after=2.7))
        budget.observe(_write(remaining=1, reset_after=0.4))

        assert budget.window_for(CHANNEL) == 5.0

    def test_the_window_is_never_learned_below_the_default(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_fresh(reset_after=1.0))

        assert budget.window_for(CHANNEL) == 5.0

    def test_the_tightest_reported_limit_wins(self):
        """Sending and editing are separate buckets folded into one allowance.

        Last-write-wins let the looser route set the pace, and the channel then
        overran the tighter one on every sweep.
        """
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        budget.observe(_fresh(limit=10))
        budget.observe(_fresh(limit=5))
        budget.observe(_fresh(limit=10))

        assert budget.limit_for(CHANNEL) == 5

    def test_an_absurd_window_is_clamped(self):
        budget = ChannelBudget()
        budget.observe(_fresh(reset_after=3600.0))

        assert budget.window_for(CHANNEL) == MAX_WINDOW_SECONDS

    def test_nonsense_observations_are_ignored(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_fresh(limit=0))
        budget.observe(_write(reset_after=0.0))

        assert budget.limit_for(CHANNEL) == 5
        assert budget.window_for(CHANNEL) == 5.0


class TestRateLimitPenalty:
    def test_a_429_steps_the_channel_allowance_down(self):
        """`scope: shared` 429s arrive with slots left, so only the 429 tells us."""
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        budget.observe(_write(remaining=2, retry_after=3.0))

        assert budget.limit_for(CHANNEL) == 4
        assert budget.cosmetic_allowance(CHANNEL) == 2

    def test_a_burst_of_429s_costs_one_step_not_five(self):
        """One overrun produces a run of rejections describing the same overrun."""
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        for _ in range(5):
            budget.observe(_write(remaining=2, retry_after=3.0))

        assert budget.limit_for(CHANNEL) == 4

    def test_the_allowance_never_drops_below_the_floor(self):
        budget = ChannelBudget(default_limit=5, default_window=0.01, event_reserve=2)
        for _ in range(20):
            budget.observe(_write(remaining=2, reset_after=0.01, retry_after=0.001))
            time.sleep(0.02)

        assert budget.limit_for(CHANNEL) == MIN_EFFECTIVE_LIMIT
        assert budget.cosmetic_allowance(CHANNEL) == 1

    def test_a_rejected_write_is_charged_to_the_window(self):
        """discord.py retries it, so one logical write costs the channel two."""
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=0)
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

        assert len(budget._writes[CHANNEL]) == 1

    def test_a_quiet_spell_gives_a_slot_back(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        budget.observe(_write(remaining=2, retry_after=3.0))
        assert budget.limit_for(CHANNEL) == 4

        # Pretend the step-down happened long enough ago to be worth retesting.
        budget._penalty_changed_at[CHANNEL] -= 600.0

        assert budget.limit_for(CHANNEL) == 5


class TestRefreshInterval:
    def test_one_sweep_of_a_full_allowance_costs_nothing(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        assert budget.refresh_interval_for(CHANNEL, 3) == 0.0

    def test_the_interval_stretches_as_embeds_accumulate(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)

        # 3 slots per 5 s window: 10 embeds need 3 extra windows after the first.
        assert budget.refresh_interval_for(CHANNEL, 10) == 15.0

    def test_no_embeds_is_no_wait(self):
        budget = ChannelBudget()
        assert budget.refresh_interval_for(CHANNEL, 0) == 0.0


class TestHeaderParsing:
    def test_a_message_edit_route_is_parsed(self):
        parsed = _channel_message_write(
            _url("/api/v10/channels/777/messages/888"),
            {
                "X-RateLimit-Limit": "5",
                "X-RateLimit-Remaining": "4",
                "X-RateLimit-Reset-After": "4.2",
            },
            200,
        )
        assert parsed == ChannelWrite(777, 5, 4, 4.2)
        assert parsed.bucket_was_fresh
        assert not parsed.rate_limited

    def test_a_message_send_route_is_parsed(self):
        parsed = _channel_message_write(
            _url("/api/v10/channels/777/messages"),
            {
                "X-RateLimit-Limit": "5",
                "X-RateLimit-Remaining": "1",
                "X-RateLimit-Reset-After": "1.0",
            },
            200,
        )
        assert parsed == ChannelWrite(777, 5, 1, 1.0)
        assert not parsed.bucket_was_fresh

    def test_a_429_carries_its_retry_after(self):
        parsed = _channel_message_write(
            _url("/api/v10/channels/777/messages/888"),
            {
                "X-RateLimit-Limit": "5",
                "X-RateLimit-Remaining": "2",
                "X-RateLimit-Reset-After": "2.47",
                "Retry-After": "3",
            },
            429,
        )
        assert parsed.rate_limited
        assert parsed.retry_after == 3.0

    def test_unrelated_routes_are_ignored(self):
        headers = {
            "X-RateLimit-Limit": "5",
            "X-RateLimit-Remaining": "4",
            "X-RateLimit-Reset-After": "1.0",
        }
        # Reactions and channel edits are different buckets; the channel route
        # itself carries no message allowance.
        assert _channel_message_write(_url("/api/v10/channels/777"), headers, 200) is None
        assert (
            _channel_message_write(
                _url("/api/v10/channels/777/messages/888/reactions/x/@me"), headers, 200
            )
            is None
        )

    def test_missing_headers_yield_nothing(self):
        assert (
            _channel_message_write(_url("/api/v10/channels/777/messages/888"), {}, 200)
            is None
        )

    def test_malformed_headers_yield_nothing(self):
        parsed = _channel_message_write(
            _url("/api/v10/channels/777/messages/888"),
            {
                "X-RateLimit-Limit": "lots",
                "X-RateLimit-Remaining": "some",
                "X-RateLimit-Reset-After": "soon",
            },
            200,
        )
        assert parsed is None
