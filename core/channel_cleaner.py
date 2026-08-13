"""
channel_cleaner.py — Weekly alert + monitored-channel purge

Runs a background task that fires every minute and checks whether it is
Friday at 18:00 local time.  When the window is hit (within a 1-minute
tolerance):

  • Alert channels: every message posted during the past 7 days is
    bulk-deleted (no preservation).
  • Monitored channels (except `price-action-trades`): every message
    posted during the past 7 days that is NOT tied to an ACTIVE or HIT
    signal is bulk-deleted (orphans + cancelled / closed signals).

The task is intentionally idempotent: a "last_purge_date" guard prevents
it from running more than once per Friday even if the bot restarts mid-
window.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import discord
from discord.ext import tasks

from price_feeds.alerting.info_embeds import info_embed_message_ids
from utils.logger import get_logger

# ── tuneable constants ────────────────────────────────────────────────────────
PURGE_WEEKDAY = 4  # 0=Mon … 4=Fri
PURGE_HOUR = 18  # 18:00 local time
PURGE_MINUTE = 0
PURGE_TOLERANCE = 1  # minutes either side that still counts as "on time"
MESSAGE_AGE_DAYS = 7  # delete messages younger than this

ALERT_CHANNEL_KEYS = [
    "alert_channel",
    "pa-alert-channel",
    "toll-alert-channel",
    "general-tolls-alert",
    "legends-trade-alert",
    "risky-gold-alert",
]

# Monitored channels whose signal messages are preserved by the weekly purge.
EXEMPT_MONITORED_NAMES = {"price-action-trades"}


class ChannelCleaner:
    """Purges alert and monitored channels every Friday at 18:00."""

    def __init__(self, bot):
        self.bot = bot
        self.logger = get_logger("channel_cleaner")
        self._last_purge_date = None  # date of the most-recent successful purge

        self._check_loop.start()

    # ── background loop ───────────────────────────────────────────────────────

    @tasks.loop(minutes=1)
    async def _check_loop(self):
        try:
            now = datetime.now()  # local time (server timezone)

            # Only act on Friday
            if now.weekday() != PURGE_WEEKDAY:
                return

            today = now.date()

            # Don't purge twice on the same Friday
            if self._last_purge_date == today:
                return

            # Check we are inside the target minute window
            target = now.replace(hour=PURGE_HOUR, minute=PURGE_MINUTE, second=0, microsecond=0)
            delta_minutes = abs((now - target).total_seconds() / 60)
            if delta_minutes > PURGE_TOLERANCE:
                return

            # ── all guards passed — run the purge ────────────────────────────
            self.logger.info("Friday 18:00 reached — starting weekly purge")
            self._last_purge_date = today
            await self._purge_alert_channels()
            await self._purge_monitored_channels()

        except Exception as exc:
            self.logger.error(f"channel_cleaner loop error: {exc}", exc_info=True)

    @_check_loop.before_loop
    async def _before_loop(self):
        await self.bot.wait_until_ready()

    # ── alert-channel purge ───────────────────────────────────────────────────

    async def _purge_alert_channels(self):
        """Delete every message < MESSAGE_AGE_DAYS old from every alert channel,
        preserving the permanent informational embeds."""
        cfg = getattr(self.bot, "channels_config", {}) or {}

        channel_ids = []
        for key in ALERT_CHANNEL_KEYS:
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
        preserve = info_embed_message_ids()

        for ch_id in channel_ids:
            channel = self.bot.get_channel(ch_id)
            if channel is None:
                self.logger.warning(f"Channel {ch_id} not found in cache — skipping")
                continue

            await self._purge_channel_preserving(channel, cutoff, preserve)

    # ── monitored-channel purge ───────────────────────────────────────────────

    async def _purge_monitored_channels(self):
        """Delete orphans and non-active-signal messages from monitored channels."""
        cfg = getattr(self.bot, "channels_config", {}) or {}
        monitored = cfg.get("monitored_channels", {}) or {}
        signal_db = getattr(self.bot, "signal_db", None)
        if signal_db is None:
            self.logger.warning("signal_db not available — skipping monitored purge")
            return

        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MESSAGE_AGE_DAYS)
        notice_ids = info_embed_message_ids()

        for name, raw_id in monitored.items():
            if name.lower() in EXEMPT_MONITORED_NAMES:
                continue
            try:
                ch_id = int(raw_id)
            except (TypeError, ValueError):
                # placeholders like "REPLACE_WITH_CHANNEL_ID"
                continue
            channel = self.bot.get_channel(ch_id)
            if channel is None:
                self.logger.warning(
                    f"Monitored channel {name} ({ch_id}) not in cache — skipping"
                )
                continue

            try:
                preserve = await signal_db.get_active_message_ids_for_channel(ch_id)
            except Exception as exc:
                self.logger.error(
                    f"#{name}: could not load active signal IDs, skipping purge: {exc}",
                    exc_info=True,
                )
                continue

            await self._purge_channel_preserving(channel, cutoff, preserve | notice_ids)

    # ── channel-level helpers ─────────────────────────────────────────────────

    async def _purge_channel_preserving(
        self,
        channel: discord.TextChannel,
        cutoff: datetime,
        preserve_ids: set[str],
    ):
        """Bulk-delete messages newer than *cutoff* in *channel*, skipping
        messages whose ID is in *preserve_ids* (e.g. active/hit signal posts)."""
        channel_name = getattr(channel, "name", str(channel.id))
        total_deleted = 0
        total_skipped_old = 0  # messages older than 14 days (Discord API limit)
        total_preserved = 0

        try:
            to_delete = []
            async for msg in channel.history(limit=None, after=cutoff):
                if str(msg.id) in preserve_ids:
                    total_preserved += 1
                    continue
                age = datetime.now(tz=timezone.utc) - msg.created_at
                if age.days < 14:
                    to_delete.append(msg)
                else:
                    total_skipped_old += 1

            if not to_delete:
                self.logger.info(
                    f"#{channel_name}: no eligible messages "
                    f"({total_preserved} preserved, {total_skipped_old} too old)"
                )
                return

            CHUNK = 100
            for i in range(0, len(to_delete), CHUNK):
                chunk = to_delete[i : i + CHUNK]
                try:
                    await channel.delete_messages(chunk)
                    total_deleted += len(chunk)
                    await asyncio.sleep(1)
                except discord.HTTPException as exc:
                    self.logger.error(
                        f"#{channel_name}: bulk delete failed for chunk {i}–{i + len(chunk)}: {exc}"
                    )

            self.logger.info(
                f"#{channel_name}: deleted {total_deleted}, "
                f"preserved {total_preserved}, "
                f"skipped {total_skipped_old} older than 14 days"
            )

        except discord.Forbidden:
            self.logger.error(
                f"#{channel_name}: missing 'Manage Messages' permission — cannot purge this channel"
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
