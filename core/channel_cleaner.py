"""
channel_cleaner.py — Weekly alert-channel purge

Runs a background task that fires every minute and checks whether it is
Friday at 18:00 local time.  When the window is hit (within a 1-minute
tolerance), all messages posted during the past 7 days in every alert
channel are bulk-deleted.

Alert channels purged:
  • alert_channel
  • pa-alert-channel
  • toll-alert-channel
  • general-tolls-alert

The task is intentionally idempotent: a "last_purge_date" guard prevents
it from running more than once per Friday even if the bot restarts mid-
window.
"""

import asyncio
import discord
from datetime import datetime, timedelta, timezone
from discord.ext import tasks
from utils.logger import get_logger


# ── tuneable constants ────────────────────────────────────────────────────────
PURGE_WEEKDAY   = 4        # 0=Mon … 4=Fri
PURGE_HOUR      = 18       # 18:00 local time
PURGE_MINUTE    = 0
PURGE_TOLERANCE = 1        # minutes either side that still counts as "on time"
MESSAGE_AGE_DAYS = 7       # delete messages younger than this


class ChannelCleaner:
    """Purges alert channels every Friday at 18:00."""

    def __init__(self, bot):
        self.bot  = bot
        self.logger = get_logger("channel_cleaner")
        self._last_purge_date = None   # date of the most-recent successful purge

        self._check_loop.start()

    # ── background loop ───────────────────────────────────────────────────────

    @tasks.loop(minutes=1)
    async def _check_loop(self):
        try:
            now = datetime.now()   # local time (server timezone)

            # Only act on Friday
            if now.weekday() != PURGE_WEEKDAY:
                return

            today = now.date()

            # Don't purge twice on the same Friday
            if self._last_purge_date == today:
                return

            # Check we are inside the target minute window
            target = now.replace(hour=PURGE_HOUR, minute=PURGE_MINUTE,
                                 second=0, microsecond=0)
            delta_minutes = abs((now - target).total_seconds() / 60)
            if delta_minutes > PURGE_TOLERANCE:
                return

            # ── all guards passed — run the purge ────────────────────────────
            self.logger.info("Friday 18:00 reached — starting weekly alert-channel purge")
            self._last_purge_date = today
            await self._purge_alert_channels()

        except Exception as exc:
            self.logger.error(f"channel_cleaner loop error: {exc}", exc_info=True)

    @_check_loop.before_loop
    async def _before_loop(self):
        await self.bot.wait_until_ready()

    # ── purge logic ───────────────────────────────────────────────────────────

    async def _purge_alert_channels(self):
        """Delete messages < MESSAGE_AGE_DAYS old from every alert channel."""
        cfg = getattr(self.bot, "channels_config", {}) or {}

        # Build the list of alert channel IDs from channels.json keys
        alert_channel_keys = [
            "alert_channel",
            "pa-alert-channel",
            "toll-alert-channel",
            "general-tolls-alert",
        ]

        channel_ids = []
        for key in alert_channel_keys:
            raw = cfg.get(key)
            if raw:
                try:
                    channel_ids.append(int(raw))
                except ValueError:
                    self.logger.warning(f"Invalid channel ID for key '{key}': {raw}")

        if not channel_ids:
            self.logger.warning("No alert channels found in config — nothing to purge")
            return

        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MESSAGE_AGE_DAYS)

        for ch_id in channel_ids:
            channel = self.bot.get_channel(ch_id)
            if channel is None:
                self.logger.warning(f"Channel {ch_id} not found in cache — skipping")
                continue

            await self._purge_channel(channel, cutoff)

    async def _purge_channel(self, channel: discord.TextChannel, cutoff: datetime):
        """Bulk-delete messages newer than *cutoff* in *channel*."""
        channel_name = getattr(channel, "name", str(channel.id))
        total_deleted = 0
        total_skipped = 0   # messages older than 14 days (Discord API limit)

        try:
            # collect messages to delete (Discord bulk-delete cap: 14 days old)
            to_delete = []
            async for msg in channel.history(limit=None, after=cutoff):
                age = datetime.now(tz=timezone.utc) - msg.created_at
                if age.days < 14:
                    to_delete.append(msg)
                else:
                    total_skipped += 1

            if not to_delete:
                self.logger.info(
                    f"#{channel_name}: no messages in the past {MESSAGE_AGE_DAYS} days"
                )
                return

            # bulk_delete accepts up to 100 messages per call
            CHUNK = 100
            for i in range(0, len(to_delete), CHUNK):
                chunk = to_delete[i : i + CHUNK]
                try:
                    await channel.delete_messages(chunk)
                    total_deleted += len(chunk)
                    await asyncio.sleep(1)   # small pause between bulk-delete calls
                except discord.HTTPException as exc:
                    self.logger.error(
                        f"#{channel_name}: bulk delete failed for chunk "
                        f"{i}–{i+len(chunk)}: {exc}"
                    )

            self.logger.info(
                f"#{channel_name}: deleted {total_deleted} messages "
                f"({total_skipped} older than 14 days skipped — "
                f"Discord API limit prevents bulk-deleting those)"
            )

        except discord.Forbidden:
            self.logger.error(
                f"#{channel_name}: missing 'Manage Messages' permission — "
                f"cannot purge this channel"
            )
        except Exception as exc:
            self.logger.error(
                f"#{channel_name}: unexpected error during purge: {exc}",
                exc_info=True,
            )

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def stop(self):
        """Cancel the background loop (called from bot.close())."""
        self._check_loop.cancel()