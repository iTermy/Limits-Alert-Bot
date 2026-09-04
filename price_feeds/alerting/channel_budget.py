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

The header, though, is an upper bound rather than a target. Discord rejects
channel message writes with `X-RateLimit-Scope: shared` and slots still
nominally remaining, so a channel routinely tolerates less than it advertises.
The budget therefore tracks two numbers: what the headers *advertise*, and what
the channel has actually *proven* it takes. The proven figure starts low, walks
up while the channel stays quiet, drops the moment a 429 arrives, and is
persisted — a restart that re-learned it from scratch would re-run the same
discovery storm every time.
"""

import asyncio
import json
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional

from utils.discord_http_trace import ChannelWrite
from utils.logger import get_logger

logger = get_logger("channel_budget")

# Starting point only, replaced per channel by the first observed headers. Five
# writes per five seconds is the bucket Discord has long served on message
# routes; assuming it costs one slow first pass if wrong, whereas assuming
# something generous costs a backlog.
DEFAULT_LIMIT = 5
DEFAULT_WINDOW_SECONDS = 5.0

# What an unknown channel is assumed to take until it earns more. This is the
# cold-start figure, and it is deliberately below DEFAULT_LIMIT: on 2026-09-04
# the bot restarted with 86 signals, hydrated their embeds, and the first
# cosmetic sweep fired at the advertised 5 into a channel that turned out to
# take 2 — six 429s in thirty seconds, every one of them delaying real alerts
# while discord.py sat on the retries. A slow first sweep costs a price
# snapshot some staleness; an optimistic one costs the channel.
INITIAL_PROVEN_LIMIT = 3

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
# below the bucket it advertises. The budget steps the proven figure down when
# one arrives and probes it back up one slot per quiet spell, rather than
# throttling the channel for the life of the process.
#
# The wait between probes doubles with each rejection the channel has handed
# out. A busy channel sitting at its true ceiling would otherwise re-discover
# the same wall every interval forever — probe up, get rejected, step down,
# repeat — which is a 429 every five minutes in exchange for headroom that is
# not there. Backing off keeps the search but stops paying for it repeatedly.
PROBE_INTERVAL_SECONDS = 300.0
PROBE_MAX_INTERVAL_SECONDS = 3600.0
MIN_EFFECTIVE_LIMIT = 2


class ChannelBudget:
    """Track recent writes per channel and hold cosmetic work to what fits."""

    def __init__(
        self,
        default_limit: int = DEFAULT_LIMIT,
        default_window: float = DEFAULT_WINDOW_SECONDS,
        event_reserve: int = EVENT_RESERVE,
        state_path: Optional[Path] = None,
    ):
        self._default_limit = default_limit
        self._default_window = default_window
        self._event_reserve = event_reserve
        self._state_path = state_path

        self._writes: dict[int, deque[float]] = defaultdict(deque)
        self._advertised: dict[int, int] = {}
        self._windows: dict[int, float] = {}
        self._proven: dict[int, int] = {}
        # When the proven figure last moved in either direction — the probe
        # clock. Kept separate from _stepped_down_at: a probe that raises the
        # limit must not also look like a recent step-down, or the 429 the probe
        # itself provokes is swallowed and the channel parks on a value it has
        # already been rejected at.
        self._proven_changed_at: dict[int, float] = {}
        self._stepped_down_at: dict[int, float] = {}
        self._rejections: dict[int, int] = {}
        self._blocked_until: dict[int, float] = {}

        self._load_state()

    # ── Learning ─────────────────────────────────────────────────────────────

    def observe(self, write: ChannelWrite) -> None:
        """Record what one Discord response said about a channel's message route."""
        if write.limit <= 0 or write.reset_after <= 0:
            return

        self._learn_advertised(write)
        self._learn_window(write)

        if write.rate_limited:
            self._penalize(write)

    def _learn_advertised(self, write: ChannelWrite) -> None:
        """Keep the tightest limit any of a channel's message routes reported.

        Sending and editing are separate Discord buckets that this budget
        deliberately folds into one channel allowance, so they can report
        different numbers. Last-write-wins let the looser of the two set the
        pace and the channel then overran the tighter one on every sweep.

        This is only ever a ceiling on the proven figure — a channel that
        advertises five slots is not thereby known to take five.
        """
        known = self._advertised.get(write.channel_id)
        if known is not None and known <= write.limit:
            return

        self._advertised[write.channel_id] = write.limit
        logger.debug(
            "Channel %s advertises %d writes per %.1fs",
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

        window = min(max(candidates), MAX_WINDOW_SECONDS)
        if window != self._windows.get(write.channel_id):
            self._windows[write.channel_id] = window
            self._save_state()

    def _penalize(self, write: ChannelWrite) -> None:
        """Charge a rejected write and step the channel's proven figure down."""
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
        if now - self._stepped_down_at.get(channel_id, -MAX_WINDOW_SECONDS) < self.window_for(
            channel_id
        ):
            return

        current = self.limit_for(channel_id)
        if current <= MIN_EFFECTIVE_LIMIT:
            return

        self._rejections[channel_id] = self._rejections.get(channel_id, 0) + 1
        self._stepped_down_at[channel_id] = now
        self._set_proven(channel_id, current - 1)
        logger.info(
            "Channel %s rate limited; write budget reduced to %d per %.0fs "
            "(next probe in %.0fs)",
            channel_id,
            self.limit_for(channel_id),
            self.window_for(channel_id),
            self._probe_interval_for(channel_id),
        )

    def _probe_interval_for(self, channel_id: int) -> float:
        """How long a channel must stay quiet before it is offered a slot back."""
        rejections = self._rejections.get(channel_id, 0)
        return min(PROBE_INTERVAL_SECONDS * (2**rejections), PROBE_MAX_INTERVAL_SECONDS)

    def _probe_upward(self, channel_id: int) -> None:
        """Give one slot back after a quiet spell, up to what the channel advertises.

        A 429 is evidence about the channel, not about the minute it arrived in,
        so the step-down is kept rather than decayed away — the old scheme
        returned to the advertised figure after three quiet spells and then
        re-discovered the same overrun. Probing upward one slot at a time keeps
        a bad minute from throttling the channel forever while still making the
        budget re-earn anything it lost.
        """
        proven = self._proven.get(channel_id, INITIAL_PROVEN_LIMIT)
        ceiling = self._advertised.get(channel_id, self._default_limit)
        if proven >= ceiling:
            return

        now = self._now()
        last_change = self._proven_changed_at.get(channel_id)
        if last_change is None:
            # First sight of this channel: start its clock rather than handing
            # it a slot it has not been quiet for yet.
            self._proven_changed_at[channel_id] = now
            return
        if now - last_change < self._probe_interval_for(channel_id):
            return

        self._set_proven(channel_id, proven + 1)

    def _set_proven(self, channel_id: int, limit: int) -> None:
        self._proven[channel_id] = max(limit, MIN_EFFECTIVE_LIMIT)
        self._proven_changed_at[channel_id] = self._now()
        self._save_state()

    def limit_for(self, channel_id: int) -> int:
        """Slots per window this channel is currently believed to take.

        The smaller of what it advertises and what it has proven: a header can
        only ever cap the allowance, never raise it past what the channel has
        actually tolerated.
        """
        self._probe_upward(channel_id)
        advertised = self._advertised.get(channel_id, self._default_limit)
        proven = self._proven.get(channel_id, INITIAL_PROVEN_LIMIT)
        # The floor applies to what the channel has proven, not to what it
        # offers: a channel that genuinely serves fewer slots than the floor is
        # still capped by its own header.
        return min(advertised, max(proven, MIN_EFFECTIVE_LIMIT))

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
        """Wait until a deferrable write fits, count it, and return the wait.

        Price snapshots use this, and so does everything else nobody is waiting
        on: startup embed rebuilds, archive moves, the static info embeds. All
        of them arrive in batches, and a batch that does not wait empties the
        channel's allowance before the first alert of the day.

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

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load_state(self) -> None:
        """Restore what each channel proved it takes on previous runs.

        Without this every restart begins at the optimistic default and
        rediscovers the real allowance the only way it can — by generating
        429s. That is precisely what a restart with a full alert channel did on
        2026-09-04, and it is why cleaning the channel appeared to fix the
        problem: fewer embeds simply kept the first sweep under the wrong
        number.
        """
        if not self._state_path or not self._state_path.exists():
            return

        try:
            with open(self._state_path, encoding="utf-8") as f:
                state = json.load(f)
        except (OSError, ValueError) as e:
            logger.warning("Could not read %s: %s", self._state_path.name, e)
            return

        for channel_id, entry in state.items():
            self._proven[int(channel_id)] = max(int(entry["proven"]), MIN_EFFECTIVE_LIMIT)
            self._windows[int(channel_id)] = min(float(entry["window"]), MAX_WINDOW_SECONDS)
            self._rejections[int(channel_id)] = int(entry.get("rejections", 0))

        logger.debug("Restored write budget for %d channel(s)", len(self._proven))

    def _save_state(self) -> None:
        """Persist the learned figures. Called only when one actually changes."""
        if not self._state_path:
            return

        state = {
            str(channel_id): {
                "proven": self._proven.get(channel_id, INITIAL_PROVEN_LIMIT),
                "window": self.window_for(channel_id),
                "rejections": self._rejections.get(channel_id, 0),
            }
            for channel_id in self._proven.keys() | self._windows.keys()
        }

        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except OSError as e:
            logger.warning("Could not write %s: %s", self._state_path.name, e)

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
