"""Per-channel Discord write budget: pacing, learning, and header parsing."""

import asyncio
import time
from types import SimpleNamespace

from price_feeds.alerting.channel_budget import (
    INITIAL_PROVEN_LIMIT,
    MAX_WINDOW_SECONDS,
    MIN_EFFECTIVE_LIMIT,
    PROBE_INTERVAL_SECONDS,
    PROBE_MAX_INTERVAL_SECONDS,
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
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget.observe(_fresh(limit=9))
        budget._proven[CHANNEL] = 5

        assert budget.cosmetic_allowance(CHANNEL) == 3

    def test_allowance_never_falls_below_one(self):
        """A reserve wider than the bucket must not deadlock cosmetic work."""
        budget = ChannelBudget(default_limit=2, default_window=5.0, event_reserve=9)
        assert budget.cosmetic_allowance(CHANNEL) == 1

    def test_an_unknown_channel_starts_below_what_the_bucket_usually_offers(self):
        """Cold start is conservative — see INITIAL_PROVEN_LIMIT.

        A restart with 86 signals hydrated their embeds and swept them all at
        the advertised five, into a channel that took two.
        """
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        assert budget.limit_for(CHANNEL) == INITIAL_PROVEN_LIMIT


class TestPacing:
    def test_writes_within_the_allowance_do_not_wait(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=0)

        async def scenario():
            return [await budget.acquire(CHANNEL) for _ in range(INITIAL_PROVEN_LIMIT)]

        assert asyncio.run(scenario()) == [0.0] * INITIAL_PROVEN_LIMIT

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
    def test_a_fresh_bucket_reading_replaces_the_default_window(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_fresh(limit=10, reset_after=8.0))

        assert budget.window_for(CHANNEL) == 8.0

    def test_an_advertised_limit_caps_the_allowance_but_does_not_grant_it(self):
        """The header says what the channel offers, not what it tolerates.

        `scope: shared` rejections arrive with slots still nominally remaining,
        so trusting the advertised figure is what produced the 2026-09-04
        restart storm.
        """
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget.observe(_fresh(limit=10))

        assert budget.limit_for(CHANNEL) == INITIAL_PROVEN_LIMIT

    def test_a_tight_advertised_limit_still_caps_a_higher_proven_one(self):
        budget = ChannelBudget(default_limit=5, default_window=5.0)
        budget._proven[CHANNEL] = 8
        budget.observe(_fresh(limit=4))

        assert budget.limit_for(CHANNEL) == 4

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
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget._proven[CHANNEL] = 9
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

        assert budget.limit_for(CHANNEL) == INITIAL_PROVEN_LIMIT
        assert budget.window_for(CHANNEL) == 5.0


class TestRateLimitPenalty:
    def test_a_429_steps_the_channel_allowance_down(self):
        """`scope: shared` 429s arrive with slots left, so only the 429 tells us."""
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

        assert budget.limit_for(CHANNEL) == 4
        assert budget.cosmetic_allowance(CHANNEL) == 2

    def test_a_burst_of_429s_costs_one_step_not_five(self):
        """One overrun produces a run of rejections describing the same overrun."""
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget._proven[CHANNEL] = 5
        for _ in range(5):
            budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

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

    def test_a_quiet_spell_gives_one_slot_back(self):
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))
        assert budget.limit_for(CHANNEL) == 4

        # Pretend the step-down happened long enough ago to be worth retesting.
        budget._proven_changed_at[CHANNEL] -= budget._probe_interval_for(CHANNEL) + 1

        assert budget.limit_for(CHANNEL) == 5

    def test_a_quiet_spell_does_not_restore_everything_at_once(self):
        """The old scheme decayed the whole penalty away and re-stormed.

        A 429 is evidence about the channel, not about the minute it arrived
        in, so recovery is a one-slot probe that has to be re-earned each time.
        """
        budget = ChannelBudget(default_limit=9, default_window=0.01, event_reserve=2)
        budget._proven[CHANNEL] = 6
        for _ in range(3):
            budget.observe(_write(limit=9, remaining=2, reset_after=0.01, retry_after=0.001))
            time.sleep(0.02)
        assert budget.limit_for(CHANNEL) == 3

        budget._proven_changed_at[CHANNEL] -= budget._probe_interval_for(CHANNEL) + 1
        assert budget.limit_for(CHANNEL) == 4

    def test_the_probe_stops_at_what_the_channel_advertises(self):
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=0)
        budget.observe(_fresh(limit=4))
        budget._proven[CHANNEL] = 4
        budget._proven_changed_at[CHANNEL] = budget._now() - (PROBE_MAX_INTERVAL_SECONDS + 1)

        assert budget.limit_for(CHANNEL) == 4

    def test_each_rejection_makes_the_next_probe_wait_longer(self):
        """A channel at its true ceiling must not re-find the wall every 5 min.

        Probing up on a fixed interval traded a 429 every interval for headroom
        that was not there.
        """
        budget = ChannelBudget(default_limit=9, default_window=0.01, event_reserve=2)
        budget._proven[CHANNEL] = 6
        assert budget._probe_interval_for(CHANNEL) == PROBE_INTERVAL_SECONDS

        intervals = []
        for _ in range(3):
            budget.observe(_write(limit=9, remaining=2, reset_after=0.01, retry_after=0.001))
            intervals.append(budget._probe_interval_for(CHANNEL))
            time.sleep(0.02)

        assert intervals == [
            PROBE_INTERVAL_SECONDS * 2,
            PROBE_INTERVAL_SECONDS * 4,
            PROBE_INTERVAL_SECONDS * 8,
        ]

    def test_a_429_the_probe_itself_provoked_is_not_swallowed(self):
        """The probe clock and the one-step-per-window guard are separate.

        Sharing one timestamp meant a probe raised the limit and thereby looked
        like a recent step-down, so the rejection it directly caused was
        ignored and the channel parked on a value it had already been refused
        at.
        """
        budget = ChannelBudget(default_limit=5, default_window=5.0, event_reserve=2)
        budget._advertised[CHANNEL] = 5
        budget._proven[CHANNEL] = 3
        budget._proven_changed_at[CHANNEL] = budget._now() - PROBE_MAX_INTERVAL_SECONDS * 2

        assert budget.limit_for(CHANNEL) == 4  # the probe fires

        budget.observe(_write(limit=5, remaining=1, retry_after=3.0))
        assert budget.limit_for(CHANNEL) == 3

    def test_the_probe_backoff_is_capped(self):
        budget = ChannelBudget(default_limit=99, default_window=0.01, event_reserve=0)
        budget._rejections[CHANNEL] = 50

        assert budget._probe_interval_for(CHANNEL) == PROBE_MAX_INTERVAL_SECONDS


class TestRefreshInterval:
    def _budget_proving(self, limit: int) -> ChannelBudget:
        budget = ChannelBudget(default_limit=9, default_window=5.0, event_reserve=2)
        budget._proven[CHANNEL] = limit
        return budget

    def test_one_sweep_of_a_full_allowance_costs_nothing(self):
        assert self._budget_proving(5).refresh_interval_for(CHANNEL, 3) == 0.0

    def test_the_interval_stretches_as_embeds_accumulate(self):
        # 3 slots per 5 s window: 10 embeds need 3 extra windows after the first.
        assert self._budget_proving(5).refresh_interval_for(CHANNEL, 10) == 15.0

    def test_no_embeds_is_no_wait(self):
        budget = ChannelBudget()
        assert budget.refresh_interval_for(CHANNEL, 0) == 0.0


class TestPersistence:
    """What a channel proved must outlive the process that learned it.

    The budget used to be pure in-memory, so every restart began at the
    optimistic default and rediscovered the real allowance the only way it
    can — by generating 429s. That is what a restart with a full alert channel
    did on 2026-09-04, and why cleaning the channel looked like a fix: fewer
    embeds simply kept the first sweep under the wrong number.
    """

    def _budget(self, tmp_path, **kwargs):
        return ChannelBudget(state_path=tmp_path / "channel_budget.json", **kwargs)

    def test_a_stepped_down_limit_survives_a_restart(self, tmp_path):
        budget = self._budget(tmp_path, default_limit=9, default_window=5.0)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))
        assert budget.limit_for(CHANNEL) == 4

        restarted = self._budget(tmp_path, default_limit=9, default_window=5.0)
        assert restarted.limit_for(CHANNEL) == 4

    def test_a_learned_window_survives_a_restart(self, tmp_path):
        budget = self._budget(tmp_path, default_limit=9, default_window=5.0)
        budget.observe(_fresh(limit=9, reset_after=8.0))

        restarted = self._budget(tmp_path, default_limit=9, default_window=5.0)
        assert restarted.window_for(CHANNEL) == 8.0

    def test_a_restart_still_re_earns_headroom_slowly(self, tmp_path):
        """Restoring the figure must not also restore a right to probe at once."""
        budget = self._budget(tmp_path, default_limit=9, default_window=5.0)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

        restarted = self._budget(tmp_path, default_limit=9, default_window=5.0)
        assert restarted.limit_for(CHANNEL) == 4
        assert restarted.limit_for(CHANNEL) == 4

    def test_the_probe_backoff_survives_a_restart(self, tmp_path):
        """Otherwise a restart loop re-probes aggressively every time."""
        budget = self._budget(tmp_path, default_limit=9, default_window=5.0)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

        restarted = self._budget(tmp_path, default_limit=9, default_window=5.0)
        assert restarted._probe_interval_for(CHANNEL) == PROBE_INTERVAL_SECONDS * 2

    def test_an_unreadable_state_file_is_not_fatal(self, tmp_path):
        path = tmp_path / "channel_budget.json"
        path.write_text("{ this is not json", encoding="utf-8")

        budget = ChannelBudget(state_path=path, default_limit=5)
        assert budget.limit_for(CHANNEL) == INITIAL_PROVEN_LIMIT

    def test_no_state_path_writes_nothing(self, tmp_path):
        budget = ChannelBudget(default_limit=9, default_window=5.0)
        budget._proven[CHANNEL] = 5
        budget.observe(_write(limit=9, remaining=2, retry_after=3.0))

        assert list(tmp_path.iterdir()) == []


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
