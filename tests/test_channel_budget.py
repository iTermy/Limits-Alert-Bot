"""Per-channel Discord write budget: pacing, learning, and header parsing."""

import asyncio
import time
from types import SimpleNamespace

from price_feeds.alerting.channel_budget import MAX_WINDOW_SECONDS, ChannelBudget
from utils.discord_http_trace import _channel_message_budget

CHANNEL = 555


def _url(path: str) -> SimpleNamespace:
    return SimpleNamespace(path=path)


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
            budget.observe(CHANNEL, limit=1, reset_after=0.15)
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


class TestLearning:
    def test_observed_headers_replace_the_default(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(CHANNEL, limit=10, reset_after=3.0)

        assert budget.limit_for(CHANNEL) == 10
        assert budget.window_for(CHANNEL) == 3.0

    def test_the_window_converges_on_the_largest_reset_after(self):
        """Mid-bucket responses report only the remainder, so keep the max."""
        budget = ChannelBudget()
        budget.observe(CHANNEL, limit=5, reset_after=1.2)
        budget.observe(CHANNEL, limit=5, reset_after=4.8)
        budget.observe(CHANNEL, limit=5, reset_after=0.4)

        assert budget.window_for(CHANNEL) == 4.8

    def test_an_absurd_window_is_clamped(self):
        budget = ChannelBudget()
        budget.observe(CHANNEL, limit=5, reset_after=3600.0)

        assert budget.window_for(CHANNEL) == MAX_WINDOW_SECONDS

    def test_nonsense_observations_are_ignored(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(CHANNEL, limit=0, reset_after=5.0)
        budget.observe(CHANNEL, limit=5, reset_after=0.0)

        assert budget.limit_for(CHANNEL) == 5
        assert budget.window_for(CHANNEL) == 5.0


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
        parsed = _channel_message_budget(
            _url("/api/v10/channels/777/messages/888"),
            {"X-RateLimit-Limit": "5", "X-RateLimit-Reset-After": "4.2"},
        )
        assert parsed == (777, 5, 4.2)

    def test_a_message_send_route_is_parsed(self):
        parsed = _channel_message_budget(
            _url("/api/v10/channels/777/messages"),
            {"X-RateLimit-Limit": "5", "X-RateLimit-Reset-After": "1.0"},
        )
        assert parsed == (777, 5, 1.0)

    def test_unrelated_routes_are_ignored(self):
        headers = {"X-RateLimit-Limit": "5", "X-RateLimit-Reset-After": "1.0"}
        # Reactions and channel edits are different buckets; the channel route
        # itself carries no message allowance.
        assert _channel_message_budget(_url("/api/v10/channels/777"), headers) is None
        assert (
            _channel_message_budget(
                _url("/api/v10/channels/777/messages/888/reactions/x/@me"), headers
            )
            is None
        )

    def test_missing_headers_yield_nothing(self):
        assert (
            _channel_message_budget(_url("/api/v10/channels/777/messages/888"), {})
            is None
        )

    def test_malformed_headers_yield_nothing(self):
        parsed = _channel_message_budget(
            _url("/api/v10/channels/777/messages/888"),
            {"X-RateLimit-Limit": "lots", "X-RateLimit-Reset-After": "soon"},
        )
        assert parsed is None
