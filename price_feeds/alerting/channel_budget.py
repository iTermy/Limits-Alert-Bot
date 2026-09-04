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

    # ── Learning ─────────────────────────────────────────────────────────────

    def observe(self, channel_id: int, limit: int, reset_after: float) -> None:
        """Record the bucket Discord reported for a channel's message route.

        The window is the **largest** Reset-After seen: a response mid-bucket
        reports only the remainder, so the maximum converges on the true period.
        """
        if limit <= 0 or reset_after <= 0:
            return

        window = min(reset_after, MAX_WINDOW_SECONDS)
        known_limit = self._limits.get(channel_id)
        known_window = self._windows.get(channel_id, 0.0)

        self._limits[channel_id] = limit
        self._windows[channel_id] = max(known_window, window)

        if known_limit != limit:
            logger.debug(
                "Channel %s message budget: %d per %.1fs",
                channel_id,
                limit,
                self._windows[channel_id],
            )

    def limit_for(self, channel_id: int) -> int:
        return self._limits.get(channel_id, self._default_limit)

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
