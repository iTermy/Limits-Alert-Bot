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
from datetime import datetime, timezone
from datetime import time as dtime
from typing import List, Optional, Tuple

import discord

from utils.logger import get_logger

logger = get_logger("risky_window")

# Daily windows (UTC) during which risky-gold trading is disabled. None of these
# cross midnight, so a simple ``start <= now < end`` check per window suffices.
RISKY_DISABLED_WINDOWS: List[Tuple[dtime, dtime]] = [
    (dtime(22, 0), dtime(23, 10)),
    (dtime(1, 0), dtime(2, 0)),
    (dtime(12, 0), dtime(14, 0)),
]

# How often the announcer re-checks the current window state.
_CHECK_INTERVAL_SECONDS = 20

# Edited "resumed" embeds are removed after this delay to keep the channel tidy.
_ENDED_DELETE_AFTER_SECONDS = 300


def is_risky_trading_disabled(now: Optional[datetime] = None) -> bool:
    """True when the current UTC time falls inside a risky-disabled window."""
    now = now or datetime.now(timezone.utc)
    t = now.time()
    return any(start <= t < end for start, end in RISKY_DISABLED_WINDOWS)


def current_window_end(now: Optional[datetime] = None) -> Optional[datetime]:
    """UTC datetime the active window ends, or None if not currently disabled."""
    now = now or datetime.now(timezone.utc)
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


class RiskyWindowAnnouncer:
    """Announces the start/end of each risky-disabled window in the risky channel."""

    def __init__(self, bot, channel: discord.abc.Messageable):
        self.bot = bot
        self.channel = channel
        self._active: Optional[bool] = None  # None until first evaluation
        self._message: Optional[discord.Message] = None
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self.channel is None:
            logger.info("Risky window announcer has no channel — not starting")
            return
        self._task = asyncio.ensure_future(self._loop())
        logger.info("Risky window announcer started (windows: %s UTC)", _windows_label())

    def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self._evaluate()
            except Exception as e:
                logger.error(f"Risky window announcer error: {e}", exc_info=True)
            await asyncio.sleep(_CHECK_INTERVAL_SECONDS)

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
        except Exception as e:
            logger.error(f"Failed to send risky-disabled embed: {e}")

    async def _send_resumed(self) -> None:
        msg = self._message
        self._message = None
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
            try:
                await msg.delete()
            except Exception:
                pass

        asyncio.ensure_future(_delete_later())
