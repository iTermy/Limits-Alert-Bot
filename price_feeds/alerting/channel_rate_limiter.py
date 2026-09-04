"""Sliding-window budget for the message operations the bot makes per channel."""

import asyncio
from collections import defaultdict, deque

from utils.logger import get_logger

logger = get_logger("channel_rate_limiter")

# Discord budgets message sends and edits per channel rather than per message.
# The exact numbers are not documented and the server remains the authority --
# discord.py still enforces whatever the response headers say. This window only
# keeps the bot from walking into that limit in the first place.
CHANNEL_OPERATION_CAPACITY = 5
CHANNEL_OPERATION_WINDOW_SECONDS = 5.0


class ChannelRateLimiter:
    """Track what the bot has recently sent to each channel, and wait for room.

    Two entry points, because the bot's two kinds of traffic want opposite
    treatment. Cosmetic work calls `acquire` and waits its turn. Event traffic
    (hit / stop-loss / TP edits and their pings) calls `record` and never waits:
    a trader waiting on a stop-loss alert should not queue behind a price
    refresh. Recording still costs the cosmetic loop budget, so a burst of event
    edits pushes the next refresh back instead of stacking on top of it.
    """

    def __init__(
        self,
        capacity: int = CHANNEL_OPERATION_CAPACITY,
        window_seconds: float = CHANNEL_OPERATION_WINDOW_SECONDS,
    ):
        self.capacity = capacity
        self.window_seconds = window_seconds
        self._sent: dict[int, deque] = defaultdict(deque)

    def record(self, channel_id: int) -> None:
        """Count one request the bot has just made to this channel."""
        self._prune(channel_id)
        self._sent[channel_id].append(self._now())

    async def acquire(self, channel_id: int, reserve: int = 0) -> float:
        """Wait until the channel has room, count the request, return the wait.

        `reserve` holds slots back for traffic that cannot wait, so a caller can
        spend the budget down to a floor rather than to zero.
        """
        budget = max(self.capacity - reserve, 1)
        waited = 0.0

        while True:
            self._prune(channel_id)
            sent = self._sent[channel_id]
            if len(sent) < budget:
                sent.append(self._now())
                return waited

            # _prune has just dropped everything older than the window, so the
            # oldest survivor always has time left on it.
            wait = self.window_seconds - (self._now() - sent[0])
            waited += wait
            await asyncio.sleep(wait)

    def _prune(self, channel_id: int) -> None:
        sent = self._sent[channel_id]
        cutoff = self._now() - self.window_seconds
        while sent and sent[0] <= cutoff:
            sent.popleft()

    @staticmethod
    def _now() -> float:
        return asyncio.get_running_loop().time()
