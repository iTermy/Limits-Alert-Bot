"""Per-channel Discord request budget, learned from response headers.

Discord buckets message writes on the **channel**, not the message, and one
bucket covers `POST`, `PATCH` and `DELETE` on `/channels/{id}/messages`. So a
signal embed edit, the ping reply that follows it, the deletion of the previous
ping, a news notice and an archive move all draw from the same allowance.

discord.py already honours whatever the headers say, which means the failure
mode here is not a 429 — it is latency. Queue more edits than a channel drains
and every one of them waits, including the stop-loss ping a trader is watching
for. This budget exists to keep the bot from generating that backlog: cosmetic
price refreshes wait for room, event traffic never does.

The numbers are **observed, not hard coded**. Discord's documentation is explicit
that per-route limits are dynamic and must be read from the response, so
`observe()` learns each channel's real bucket and the defaults below are only a
conservative starting point for the first few requests.
"""

import asyncio
import time
from collections import defaultdict, deque

from utils.discord_http_trace import ChannelWrite
from utils.logger import get_logger

logger = get_logger("channel_budget")

# Starting point only, replaced per channel by the first observed headers. Five
# writes per five seconds is the bucket Discord has long served on message
# routes; assuming it costs one slow first pass if wrong, whereas assuming
# something generous costs a backlog.
DEFAULT_LIMIT = 5
DEFAULT_WINDOW_SECONDS = 5.0

# Slots withheld from cosmetic refreshes so an event that arrives mid-pass finds
# room instead of queueing behind price snapshots. Event traffic still spends
# from the window, so a burst of hits pushes the next refresh back rather than
# stacking on top of it.
EVENT_RESERVE = 2

# A learned window is clamped to this. A single odd Reset-After (a proxy, a
# clock skew, a shared sub-limit) must not be able to stall a channel for
# minutes.
MAX_WINDOW_SECONDS = 30.0

# A 429 is the only unambiguous statement that a channel's real allowance is
# below the bucket it advertises: Discord returns `X-RateLimit-Scope: shared`
# rejections with slots still nominally remaining, so no header predicts them.
# The budget steps itself down when one arrives and probes back up after a
# quiet spell, rather than throttling the channel for the life of the process.
PENALTY_RECOVERY_SECONDS = 300.0
MIN_EFFECTIVE_LIMIT = 2


class ChannelBudget:
    """Track recent writes per channel and hold cosmetic work to what fits."""

    def __init__(
        self,
        default_limit: int = DEFAULT_LIMIT,
        default_window: float = DEFAULT_WINDOW_SECONDS,
        event_reserve: int = EVENT_RESERVE,
    ):
        self._default_limit = default_limit
        self._default_window = default_window
        self._event_reserve = event_reserve

        self._writes: dict[int, deque[float]] = defaultdict(deque)
        self._limits: dict[int, int] = {}
        self._windows: dict[int, float] = {}
        self._penalties: dict[int, int] = {}
        self._penalty_changed_at: dict[int, float] = {}
        self._blocked_until: dict[int, float] = {}

    # ── Learning ─────────────────────────────────────────────────────────────

    def observe(self, write: ChannelWrite) -> None:
        """Record what one Discord response said about a channel's message route."""
        if write.limit <= 0 or write.reset_after <= 0:
            return

        self._learn_limit(write)
        self._learn_window(write)

        if write.rate_limited:
            self._penalize(write)

    def _learn_limit(self, write: ChannelWrite) -> None:
        """Keep the tightest limit any of a channel's message routes reported.

        Sending and editing are separate Discord buckets that this budget
        deliberately folds into one channel allowance, so they can report
        different numbers. Last-write-wins let the looser of the two set the
        pace and the channel then overran the tighter one on every sweep.
        """
        known = self._limits.get(write.channel_id)
        if known is not None and known <= write.limit:
            return

        self._limits[write.channel_id] = write.limit
        logger.debug(
            "Channel %s message budget: %d per %.1fs",
            write.channel_id,
            write.limit,
            self.window_for(write.channel_id),
        )

    def _learn_window(self, write: ChannelWrite) -> None:
        """Widen the window, but only on evidence that actually states a period.

        `Reset-After` is the time *remaining*, so it only equals the bucket's
        period on the first request of a fresh window. The bot writes in
        bursts, so almost every response is mid-window: taking the max over
        those remainders converged on ~2.7 s for a 5 s bucket and let every
        sweep fire four writes into a channel that takes three.

        A 429's Retry-After is the other honest reading — it is how long
        Discord will keep refusing — so it can only widen the window too. The
        default is a floor for the same reason the estimate is only ever
        widened: being slower than necessary costs a price snapshot a second of
        staleness, being faster costs a 429 storm that delays real alerts.
        """
        candidates = [self._default_window, self.window_for(write.channel_id)]
        if write.bucket_was_fresh:
            candidates.append(write.reset_after)
        if write.rate_limited:
            candidates.append(write.retry_after)

        self._windows[write.channel_id] = min(max(candidates), MAX_WINDOW_SECONDS)

    def _penalize(self, write: ChannelWrite) -> None:
        """Charge a rejected write and step the channel's allowance down."""
        channel_id = write.channel_id
        now = self._now()

        # discord.py retries the rejected request, so one logical write cost the
        # channel two. Charge the one the budget would otherwise never see.
        self._writes[channel_id].append(now)
        self._blocked_until[channel_id] = max(
            self._blocked_until.get(channel_id, 0.0),
            now + min(write.retry_after, MAX_WINDOW_SECONDS),
        )

        # One step per window: a single overrun produces a run of 429s and they
        # all describe the same overrun.
        if now - self._penalty_changed_at.get(channel_id, -MAX_WINDOW_SECONDS) < self.window_for(
            channel_id
        ):
            return
        if self.limit_for(channel_id) <= MIN_EFFECTIVE_LIMIT:
            return

        self._penalties[channel_id] = self._penalties.get(channel_id, 0) + 1
        self._penalty_changed_at[channel_id] = now
        logger.info(
            "Channel %s rate limited; write budget reduced to %d per %.0fs",
            channel_id,
            self.limit_for(channel_id),
            self.window_for(channel_id),
        )

    def _relax_penalty(self, channel_id: int) -> None:
        """Give one stepped-down slot back after a quiet spell.

        Without this a single bad minute would throttle a channel until
        restart. With it the budget probes upward again and re-learns the hard
        way if the channel still cannot take it.
        """
        penalty = self._penalties.get(channel_id, 0)
        if not penalty:
            return

        now = self._now()
        if now - self._penalty_changed_at.get(channel_id, now) < PENALTY_RECOVERY_SECONDS:
            return

        self._penalties[channel_id] = penalty - 1
        self._penalty_changed_at[channel_id] = now

    def limit_for(self, channel_id: int) -> int:
        self._relax_penalty(channel_id)
        learned = self._limits.get(channel_id, self._default_limit)
        return max(learned - self._penalties.get(channel_id, 0), 1)

    def window_for(self, channel_id: int) -> float:
        return self._windows.get(channel_id, self._default_window)

    def cosmetic_allowance(self, channel_id: int) -> int:
        """How many of a channel's slots price refreshes may use per window."""
        return max(self.limit_for(channel_id) - self._event_reserve, 1)

    # ── Spending ─────────────────────────────────────────────────────────────

    def record(self, channel_id: int, count: int = 1) -> None:
        """Count writes the bot has just made and will not wait for.

        Event edits, pings, news notices and archive moves all go through here:
        they must not be delayed, but they do consume the channel's allowance,
        so the next cosmetic refresh sees a smaller window.
        """
        self._prune(channel_id)
        now = self._now()
        for _ in range(count):
            self._writes[channel_id].append(now)

    async def acquire(self, channel_id: int) -> float:
        """Wait until a cosmetic write fits, count it, and return the wait.

        Sleeps in whole gaps rather than polling: the oldest write inside the
        window is the one whose expiry frees the next slot.
        """
        waited = 0.0

        while True:
            # Re-read every pass: observe() fires on every Discord response and
            # can widen the window under us. Computing the wait from a stale one
            # yields a non-positive sleep and spins the loop.
            allowance = self.cosmetic_allowance(channel_id)
            window = self.window_for(channel_id)

            # A channel Discord is actively refusing has no room for cosmetic
            # work, whatever the local tally says. Sending anyway just queues
            # behind the retry discord.py is already sitting on.
            blocked_for = self._blocked_until.get(channel_id, 0.0) - self._now()
            if blocked_for > 0:
                waited += blocked_for
                await asyncio.sleep(blocked_for)
                continue

            self._prune(channel_id)
            writes = self._writes[channel_id]
            if len(writes) < allowance:
                writes.append(self._now())
                return waited

            # _prune has dropped everything older than the window, so the oldest
            # survivor normally has time left before it frees a slot. Yield the
            # loop regardless so a zero-length window cannot spin.
            wait = max(window - (self._now() - writes[0]), 0.0)
            waited += wait
            await asyncio.sleep(wait)

    def _prune(self, channel_id: int) -> None:
        writes = self._writes[channel_id]
        cutoff = self._now() - self.window_for(channel_id)
        while writes and writes[0] <= cutoff:
            writes.popleft()

    @staticmethod
    def _now() -> float:
        # Monotonic wall time rather than the loop clock: record() is called
        # from every Discord write path and must not depend on a running loop.
        return time.monotonic()

    # ── Introspection ────────────────────────────────────────────────────────

    def refresh_interval_for(self, channel_id: int, embed_count: int) -> float:
        """Seconds a full sweep of `embed_count` embeds takes in one channel.

        This is what "the interval stretches under load" means in practice, and
        what `!health` reports.
        """
        if embed_count <= 0:
            return 0.0
        allowance = self.cosmetic_allowance(channel_id)
        window = self.window_for(channel_id)
        return ((embed_count - 1) // allowance) * window
