"""
Archive manager for the alert system.

Handles end-state embed lifecycle: scheduling moves to finished-signals or
profit channels after a delay, cancelling pending moves on reactivation,
and cleaning up original signal messages in auto-purge channels.
"""

import asyncio
import logging
from typing import Dict, Optional

import discord

from price_feeds.embed_builders import (
    _build_profit_archive_embed,
    _build_signal_embed,
    _set_archive_footer,
)

logger = logging.getLogger(__name__)

_END_STATES = {
    "profit",
    "auto_tp",
    "stop_loss",
    "cancelled",
    "near_miss_cancelled",
    "spread_hour_cancelled",
    "expired",
    "breakeven",
}

_PROFIT_EVENTS = {"profit", "auto_tp"}

END_STATE_DELETE_MINUTES = 15


def is_end_state(event: str) -> bool:
    return event in _END_STATES


class ArchiveManager:
    """Manages the delayed move of end-state signal embeds to archive channels."""

    def __init__(
        self,
        bot,
        signal_messages: Dict[int, discord.Message],
        signal_ping_messages: Dict[int, discord.Message],
        signal_finished_messages: Dict[int, discord.Message],
        alert_messages: dict,
        auto_purge_channel_ids: set,
        role_mention: str,
        track_alert_message_fn,
        finished_channel_id,
        profit_channel_id,
    ):
        self.bot = bot
        self.signal_messages = signal_messages
        self.signal_ping_messages = signal_ping_messages
        self.signal_finished_messages = signal_finished_messages
        self.alert_messages = alert_messages
        self.auto_purge_channel_ids = auto_purge_channel_ids
        self.role_mention = role_mention
        self._track_alert_message = track_alert_message_fn
        self._finished_channel_id = finished_channel_id
        self._profit_channel_id = profit_channel_id
        self._deletion_tasks: Dict[int, asyncio.Task] = {}

    def cancel_pending_move(self, signal_id: int):
        """Cancel any pending move-to-finished task for a signal (e.g. on reactivation)."""
        task = self._deletion_tasks.pop(signal_id, None)
        if task and not task.done():
            task.cancel()
            logger.debug(f"Cancelled pending move-to-finished task for signal {signal_id}")

    def cancel_all(self):
        """Cancel all pending archive-move tasks. Called during shutdown."""
        for signal_id, task in list(self._deletion_tasks.items()):
            if not task.done():
                task.cancel()
        self._deletion_tasks.clear()
        logger.info("Cancelled all pending end-state archive-move tasks")

    def _get_finished_channel(self) -> Optional[discord.TextChannel]:
        if not self._finished_channel_id or not self.bot:
            return None
        return self.bot.get_channel(int(self._finished_channel_id))

    def _get_profit_channel(self) -> Optional[discord.TextChannel]:
        if not self._profit_channel_id or not self.bot:
            return None
        return self.bot.get_channel(int(self._profit_channel_id))

    async def maybe_delete_original_message(self, signal: Dict, signal_id: int) -> None:
        """
        Delete the original signal message if its channel is in auto_purge_channel_ids.
        Safe to call on any signal — silently skips exempt channels and manual signals.
        """
        src_channel_id = str(signal.get("channel_id", ""))
        src_message_id = str(signal.get("message_id", ""))
        if (
            src_channel_id not in self.auto_purge_channel_ids
            or not src_message_id
            or src_message_id.startswith("manual_")
        ):
            return
        try:
            src_channel = self.bot.get_channel(int(src_channel_id)) if self.bot else None
            if not src_channel:
                return
            try:
                src_msg = await src_channel.fetch_message(int(src_message_id))
                await src_msg.delete()
                logger.info(
                    f"Deleted original signal message {src_message_id} for signal {signal_id}"
                )
            except discord.NotFound:
                pass
            except discord.Forbidden:
                logger.warning(
                    f"No permission to delete original signal message {src_message_id} for signal {signal_id}"
                )
            except Exception as e:
                logger.warning(f"Could not delete original signal message {src_message_id}: {e}")
        except Exception as e:
            logger.warning(f"Original signal message cleanup failed for signal {signal_id}: {e}")

    def schedule_end_state_move(self, signal_id: int, event: str = ""):
        """
        Schedule the persistent embed to be moved out of the alert channel after
        END_STATE_DELETE_MINUTES minutes.

        Routing:
          - profit / auto_tp  -> profit_channel
          - everything else   -> finished_signals channel
        """
        self.cancel_pending_move(signal_id)
        is_profit = event in _PROFIT_EVENTS

        async def _move_after_delay():
            try:
                await asyncio.sleep(END_STATE_DELETE_MINUTES * 60)
            except asyncio.CancelledError:
                return

            ping_msg = self.signal_ping_messages.pop(signal_id, None)
            if ping_msg:
                try:
                    await ping_msg.delete()
                except Exception:
                    pass

            embed_msg = self.signal_messages.get(signal_id)
            if not embed_msg:
                self._deletion_tasks.pop(signal_id, None)
                return

            if is_profit:
                dest_channel = self._get_profit_channel()
                archive_label = "📁 Profit Archived"
                dest_name = "profit channel"
            else:
                dest_channel = self._get_finished_channel()
                archive_label = "📁 Archived"
                dest_name = "finished-signals channel"

            finished_msg = None
            if dest_channel:
                try:
                    sig_data = None
                    if self.bot and self.bot.signal_db:
                        try:
                            sig_data = await self.bot.signal_db.get_signal_with_limits(signal_id)
                        except Exception as _fetch_err:
                            logger.warning(
                                f"Could not fetch signal {signal_id} from DB for archive: {_fetch_err}"
                            )

                    if is_profit:
                        new_embed = _build_profit_archive_embed(sig_data, signal_id, self.bot)
                    else:
                        new_embed = None
                        if sig_data:
                            try:
                                db_status = sig_data.get("status", "")
                                cancel_type_db = sig_data.get("closed_reason") or ""
                                _status_to_event = {
                                    "profit": "profit",
                                    "auto_tp": "auto_tp",
                                    "stop_loss": "stop_loss",
                                    "cancelled": "cancelled",
                                    "expired": "expired",
                                    "breakeven": "breakeven",
                                }
                                rebuild_event = _status_to_event.get(db_status, event)
                                if rebuild_event == "cancelled":
                                    if cancel_type_db == "near_miss":
                                        rebuild_event = "near_miss_cancelled"
                                    elif cancel_type_db == "spread_hour":
                                        rebuild_event = "spread_hour_cancelled"

                                guild_id_val = sig_data.get("guild_id")
                                if not guild_id_val and self.bot and self.bot.guilds:
                                    guild_id_val = self.bot.guilds[0].id

                                new_embed = _build_signal_embed(
                                    signal=sig_data,
                                    limits=sig_data.get("limits", []),
                                    event=rebuild_event,
                                    guild_id=guild_id_val,
                                    bot=self.bot,
                                )
                                _set_archive_footer(new_embed)
                            except Exception as _rebuild_err:
                                logger.warning(
                                    f"Could not rebuild embed for signal {signal_id} from DB: {_rebuild_err}"
                                )

                        if new_embed is None:
                            existing_embed = embed_msg.embeds[0] if embed_msg.embeds else None
                            if existing_embed:
                                new_embed = existing_embed.copy()
                                _set_archive_footer(new_embed)
                            else:
                                new_embed = discord.Embed(
                                    description="Signal reached a final state.",
                                    color=0x808080,
                                )

                    if not is_profit:
                        try:
                            await dest_channel.send(self.role_mention)
                        except Exception as _ping_err:
                            logger.warning(
                                f"Could not send role ping to {dest_name} for signal {signal_id}: {_ping_err}"
                            )

                    finished_msg = await dest_channel.send(embed=new_embed)
                    self.signal_finished_messages[signal_id] = finished_msg
                    self._track_alert_message(finished_msg.id, signal_id)
                    logger.info(
                        f"Moved signal {signal_id} embed to {dest_name} (msg {finished_msg.id})"
                    )
                except Exception as e:
                    logger.error(f"Failed to send embed to {dest_name} for signal {signal_id}: {e}")

                try:
                    await embed_msg.delete()
                    logger.info(
                        f"Deleted alert-channel embed for signal {signal_id} after move to {dest_name}"
                    )
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(f"Failed to delete alert embed for signal {signal_id}: {e}")
            else:
                try:
                    await embed_msg.delete()
                    logger.info(
                        f"Deleted end-state embed for signal {signal_id} "
                        f"(no {dest_name} configured)"
                    )
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(f"Failed to delete embed for signal {signal_id}: {e}")

            self.signal_messages.pop(signal_id, None)
            if embed_msg:
                self.alert_messages.pop(str(embed_msg.id), None)
            self._deletion_tasks.pop(signal_id, None)

            if self.bot and self.bot.signal_db:
                try:
                    async with self.bot.signal_db.db.get_connection() as conn:
                        if finished_msg is not None:
                            await conn.execute(
                                "UPDATE signals "
                                "SET alert_message_id = NULL, alert_channel_id = NULL, ping_message_id = NULL, "
                                "    finished_message_id = $1, finished_channel_id = $2 "
                                "WHERE id = $3",
                                int(finished_msg.id),
                                int(dest_channel.id),
                                int(signal_id),
                            )
                        else:
                            await conn.execute(
                                "UPDATE signals "
                                "SET alert_message_id = NULL, alert_channel_id = NULL, ping_message_id = NULL "
                                "WHERE id = $1",
                                int(signal_id),
                            )
                except Exception as e:
                    logger.warning(
                        f"Could not update persisted alert IDs after archive for signal {signal_id}: {e}"
                    )

            try:
                if self.bot and self.bot.signal_db:
                    sig_data = await self.bot.signal_db.get_signal_with_limits(signal_id)
                    if sig_data:
                        await self.maybe_delete_original_message(sig_data, signal_id)
            except Exception as e:
                logger.warning(
                    f"Original signal message cleanup failed for signal {signal_id}: {e}"
                )

        task = asyncio.ensure_future(_move_after_delay())
        self._deletion_tasks[signal_id] = task
        logger.info(
            f"Scheduled archive move for signal {signal_id} (event='{event}') "
            f"in {END_STATE_DELETE_MINUTES} minutes -> "
            f"{'profit channel' if is_profit else 'finished-signals channel'}"
        )

    async def move_standalone_after_delay(
        self, signal: Dict, signal_id: int, message: discord.Message, embed: discord.Embed
    ) -> None:
        """Move a standalone news-cancel message to finished-signals after the delay."""
        try:
            await asyncio.sleep(END_STATE_DELETE_MINUTES * 60)
        except asyncio.CancelledError:
            return

        finished_channel = self._get_finished_channel()
        if finished_channel:
            try:
                archived_embed = embed.copy()
                _set_archive_footer(archived_embed)
                await finished_channel.send(embed=archived_embed)
                logger.info(
                    f"Moved standalone news-cancel embed for signal {signal_id} to finished-signals"
                )
            except Exception as _mv:
                logger.warning(
                    f"Could not move standalone news-cancel embed for signal {signal_id}: {_mv}"
                )

        try:
            await message.delete()
            logger.info(f"Deleted standalone news-cancel message for signal {signal_id}")
        except Exception:
            pass

        await self.maybe_delete_original_message(signal, signal_id)
