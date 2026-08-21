"""Risky-trading disabled windows.

Risky-gold signals are paused during fixed daily windows (UTC) when the market
is thin and prone to whipsaws. During a window any risky signal that would be
hit — or that near-misses — is cancelled rather than tracked, mirroring the
news / spread-hour guards.

This module owns two things:
  - ``is_risky_trading_disabled`` / ``current_window_end`` — pure time helpers
    used by the streaming monitor to gate risky signals per tick.
  - ``RiskyWindowAnnouncer`` — a small background loop that posts a "disabled"
    embed to the risky-gold alert channel when a window opens and edits it to
    "resumed" when the window closes, in the style of the news / volatility
    guard announcements.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timezone
from datetime import time as dtime
from pathlib import Path

import discord
import pytz

from utils.logger import get_logger

logger = get_logger("risky_window")

_EST_TZ = pytz.timezone("America/New_York")

# Persisted reference to the currently-posted "disabled" embed, so a restart
# mid-window reuses the existing embed instead of orphaning it.
_STATE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "risky_window_state.json"

# Daily windows (UTC) during which risky-gold trading is disabled. None of these
# cross midnight, so a simple ``start <= now < end`` check per window suffices.
RISKY_DISABLED_WINDOWS: list[tuple[dtime, dtime]] = [
    (dtime(22, 0), dtime(23, 10)),
    (dtime(1, 0), dtime(2, 0)),
    (dtime(12, 0), dtime(14, 0)),
]

# Weekend market closure (EST): the gold market shuts Friday 17:00 EST and
# reopens Sunday 18:00 EST. Risky windows do not apply while it is closed, so
# the announcer stays silent over the weekend.
_WEEKEND_CLOSE_TIME = dtime(17, 0)  # Friday
_WEEKEND_REOPEN_TIME = dtime(18, 0)  # Sunday

# How often the announcer re-checks the current window state.
_CHECK_INTERVAL_SECONDS = 20

# Edited "resumed" embeds are removed after this delay to keep the channel tidy.
_ENDED_DELETE_AFTER_SECONDS = 300


def _is_weekend_closed(now: datetime) -> bool:
    """True when the gold market is shut for the weekend (EST).

    Closed from Friday 17:00 EST through Sunday 18:00 EST.
    """
    est = now.astimezone(_EST_TZ)
    weekday = est.weekday()  # Mon=0 .. Sun=6
    if weekday == 5:  # Saturday — closed all day
        return True
    if weekday == 4 and est.time() >= _WEEKEND_CLOSE_TIME:  # Friday after close
        return True
    if weekday == 6 and est.time() < _WEEKEND_REOPEN_TIME:  # Sunday before reopen
        return True
    return False


def is_risky_trading_disabled(now: datetime | None = None) -> bool:
    """True when the current UTC time falls inside a risky-disabled window.

    Weekend closures take precedence: while the market is shut no window applies.
    """
    now = now or datetime.now(timezone.utc)
    if _is_weekend_closed(now):
        return False
    t = now.time()
    return any(start <= t < end for start, end in RISKY_DISABLED_WINDOWS)


def current_window_end(now: datetime | None = None) -> datetime | None:
    """UTC datetime the active window ends, or None if not currently disabled."""
    now = now or datetime.now(timezone.utc)
    if _is_weekend_closed(now):
        return None
    t = now.time()
    for start, end in RISKY_DISABLED_WINDOWS:
        if start <= t < end:
            return now.replace(
                hour=end.hour, minute=end.minute, second=0, microsecond=0
            )
    return None


def _windows_label() -> str:
    """Human-readable list of the configured windows, e.g. '22:00–23:10 UTC'."""
    return ", ".join(
        f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')}" for s, e in RISKY_DISABLED_WINDOWS
    )


def _save_state(message_id: int, channel_id: int, window_end: datetime | None) -> None:
    """Persist the posted disabled-embed reference so a restart can reattach."""
    data = {
        "message_id": message_id,
        "channel_id": channel_id,
        "window_end": window_end.isoformat() if window_end else None,
    }
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to save risky-window state: {e}")


def _load_state() -> dict | None:
    """Read the persisted disabled-embed reference, or None if absent/unreadable."""
    if not _STATE_PATH.exists():
        return None
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read risky-window state: {e}")
        return None


def _clear_state() -> None:
    """Drop the persisted disabled-embed reference."""
    try:
        _STATE_PATH.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Failed to clear risky-window state: {e}")


class RiskyWindowAnnouncer:
    """Announces the start/end of each risky-disabled window in the risky channel."""

    def __init__(self, bot, channel: discord.abc.Messageable):
        self.bot = bot
        self.channel = channel
        self._active: bool | None = None  # None until first evaluation
        self._message: discord.Message | None = None
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self.channel is None:
            logger.warning("Risky window announcer has no channel — not starting")
            return
        self._task = asyncio.ensure_future(self._loop())
        logger.debug("Risky window announcer started (windows: %s UTC)", _windows_label())

    def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        await self.bot.wait_until_ready()
        await self._recover_persisted_message()
        while True:
            try:
                await self._evaluate()
            except Exception as e:
                logger.error(f"Risky window announcer error: {e}", exc_info=True)
            await asyncio.sleep(_CHECK_INTERVAL_SECONDS)

    async def _recover_persisted_message(self) -> None:
        """Reattach to a persisted "disabled" embed after a restart.

        If a window is still active and the persisted embed belongs to it, reuse
        it so no duplicate is posted. Otherwise the embed is stale (its window
        ended while the bot was down) — delete it and start fresh.
        """
        state = _load_state()
        message_id = state.get("message_id") if state else None
        if not message_id:
            return
        try:
            msg = await self.channel.fetch_message(message_id)
        except discord.NotFound:
            _clear_state()
            return
        except Exception as e:
            logger.warning(f"Could not fetch persisted risky-window message: {e}")
            return

        end = current_window_end()
        if end is not None and end.isoformat() == state.get("window_end"):
            self._message = msg
            self._active = True
            logger.info("Recovered active risky-disabled embed after restart")
            return

        # Stale embed from an ended (or different) window — remove it and reset.
        with contextlib.suppress(Exception):
            await msg.delete()
        _clear_state()

    async def _evaluate(self) -> None:
        disabled = is_risky_trading_disabled()
        if disabled == self._active:
            return
        # Skip announcing "resumed" on the very first evaluation (calm at startup).
        was_active = self._active
        self._active = disabled
        if disabled:
            await self._send_disabled()
        elif was_active:
            await self._send_resumed()

    async def _send_disabled(self) -> None:
        end = current_window_end()
        end_line = f"Active until <t:{int(end.timestamp())}:t>.\n" if end else ""
        embed = discord.Embed(
            title="🚫 Risky Trades Disabled",
            description=(
                f"Risky-gold trading is paused during a scheduled window.\n"
                f"{end_line}"
                f"Any risky signal hit or near-missed during this window is auto-cancelled."
            ),
            color=0xE74C3C,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Disabled windows (UTC): {_windows_label()}")
        try:
            self._message = await self.channel.send(embed=embed)
            _save_state(self._message.id, self.channel.id, end)
        except Exception as e:
            logger.error(f"Failed to send risky-disabled embed: {e}")

    async def _send_resumed(self) -> None:
        msg = self._message
        self._message = None
        _clear_state()
        resumed_ts = int(datetime.now(timezone.utc).timestamp())
        embed = discord.Embed(
            title="✅ Risky Trades Resumed",
            description=(
                f"The scheduled pause has ended.\n**Resumed at <t:{resumed_ts}:t>**"
            ),
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="This message will be deleted in 5 minutes")
        if msg is None:
            try:
                await self.channel.send(embed=embed)
            except Exception as e:
                logger.warning(f"Could not send risky-resumed embed: {e}")
            return
        try:
            await msg.edit(embed=embed)
        except Exception as e:
            logger.warning(f"Could not edit risky window embed to resumed: {e}")
            return

        async def _delete_later():
            await asyncio.sleep(_ENDED_DELETE_AFTER_SECONDS)
            with contextlib.suppress(Exception):
                await msg.delete()

        asyncio.ensure_future(_delete_later())
