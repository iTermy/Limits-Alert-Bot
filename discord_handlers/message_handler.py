"""
Message Handler
"""

import asyncio
import re
from datetime import datetime
from typing import Optional

import discord
import pytz

from core.parser import RejectedSignal, parse_signal
from database import db
from price_feeds.embed_builders import _build_signal_embed, _set_archive_footer
from price_feeds.streaming_monitor import react_to_original_signal
from price_feeds.tp_config import TPConfig
from utils.formatting import format_price, get_channel_name as _get_channel_name
from utils.logger import get_logger
from utils.permissions import is_signal_manager

logger = get_logger("message_handler")

# Auto-delete delay for transient bot replies in monitored / alert channels.
_REPLY_DELETE_AFTER = 15.0

# DM sent when a non-manager tries to manage a signal via reply.
_NO_PERMISSION_DM = (
    "You don't have permission to manage signals. "
    "If you'd like access, please ask an admin."
)


class MessageHandler:
    """Handles all message-related events for signal processing"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.signal_db = bot.signal_db
        self.tp_config = TPConfig()
        self.alert_system = None  # Set by monitor when initialized
        logger.info("MessageHandler initialized, alert_system is None initially")

    def is_allowed_channel(self, channel_id: int) -> bool:
        return channel_id in self.bot.allowed_channel_ids

    async def _safe_remove_reaction(self, message: discord.Message, emoji: str) -> None:
        try:
            await message.remove_reaction(emoji, self.bot.user)
        except Exception:
            pass

    async def _safe_delete(self, message: discord.Message) -> None:
        try:
            await message.delete()
        except Exception:
            pass

    async def _deny_signal_management(self, message: discord.Message) -> None:
        """Delete an unauthorized management reply and DM the user why."""
        try:
            await message.author.send(_NO_PERMISSION_DM)
        except discord.Forbidden:
            self.logger.info(
                f"Could not DM {message.author} about denied signal management (DMs closed)"
            )
        except Exception as e:
            self.logger.warning(f"Error DMing user about denied signal management: {e}")
        await self._safe_delete(message)

    async def handle_new_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.channel.id in self.bot.monitored_channels:
            self.logger.info(f"New message in monitored channel: {message.channel.name}")
            await self.process_signal(message)
        await self.check_signal_management_reply(message)
        await self.check_alert_management_reply(message)

    async def _react_to_original_signal(self, signal: dict, action_taken: str):
        """Add a reaction to the original signal message based on the action taken."""
        try:
            message_id = signal.get("message_id")
            channel_id = signal.get("channel_id")
            if not message_id or not channel_id or str(message_id).startswith("manual_"):
                self.logger.debug(
                    "Skipping original message reaction - manual signal or missing IDs"
                )
                return
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    channel = await self.bot.fetch_channel(int(channel_id))
                if not channel:
                    self.logger.warning(f"Could not find channel {channel_id} for original signal")
                    return
                original_message = await channel.fetch_message(int(message_id))
            except discord.NotFound:
                self.logger.warning(f"Original signal message {message_id} not found")
                return
            except discord.Forbidden:
                self.logger.warning(f"No permission to access message {message_id}")
                return
            except Exception as e:
                self.logger.error(f"Error fetching original message: {e}")
                return

            if action_taken == "cancelled":
                await self.safe_add_reaction(original_message, "❌")
            elif action_taken == "marked as HIT":
                await self.safe_add_reaction(original_message, "🎯")
            elif action_taken == "marked as PROFIT":
                await self.safe_add_reaction(original_message, "💰")
            elif action_taken == "marked as BREAKEVEN":
                await self.safe_add_reaction(original_message, "➖")
            elif action_taken == "marked as STOP LOSS":
                await self.safe_add_reaction(original_message, "🛑")
            elif action_taken == "reactivated":
                await self._safe_remove_reaction(original_message, "❌")
                await self.safe_add_reaction(original_message, "♻️")
            self.logger.info(
                f"Added reaction to original signal message {message_id} for action: {action_taken}"
            )
        except Exception as e:
            self.logger.error(f"Error adding reaction to original signal: {e}", exc_info=True)

    async def check_alert_management_reply(self, message: discord.Message):
        """Handle replies to alert embeds — any user may execute commands."""
        if not message.reference or message.author.bot:
            return

        if not self.alert_system:
            if self.bot.services.alert_system:
                self.alert_system = self.bot.services.alert_system
                logger.info(
                    f"Got alert system from services, has {len(self.alert_system.alert_messages)} tracked messages"
                )
            else:
                logger.warning("Alert system not available - monitor may not be initialized")
                return

        try:
            try:
                referenced = await message.channel.fetch_message(message.reference.message_id)
            except discord.NotFound:
                return  # referenced message was deleted; nothing to process
            signal_id = self.alert_system.get_signal_from_alert(str(referenced.id))

            if not signal_id:
                if referenced.author.id == self.bot.user.id and referenced.embeds:
                    embed = referenced.embeds[0]
                    if any(
                        keyword in embed.title.lower()
                        for keyword in ["approaching", "hit", "stop loss"]
                    ):
                        logger.warning(
                            f"Message looks like alert but isn't tracked: {referenced.id}"
                        )
                        await message.reply(
                            "❌ This alert is not tracked. It may have been sent before the bot restarted.",
                            delete_after=_REPLY_DELETE_AFTER,
                        )
                        await self._safe_delete(message)
                return

            if not is_signal_manager(self.bot, message.author):
                await self._deny_signal_management(message)
                return

            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.warning(f"No signal found with ID {signal_id}")
                await message.reply("❌ Signal not found.", delete_after=_REPLY_DELETE_AFTER)
                await self._safe_delete(message)
                return

            await self._handle_reply_command(message, referenced, signal, signal_id)

        except Exception as e:
            logger.error(f"Error in alert management reply: {e}", exc_info=True)
            try:
                await message.reply(
                    "❌ An error occurred processing your command.",
                    delete_after=_REPLY_DELETE_AFTER,
                )
                await self._safe_delete(message)
            except Exception:
                pass

    async def check_signal_management_reply(self, message: discord.Message):
        """Handle replies to original signal messages — signal author or admins only."""
        if not message.reference or message.author.bot:
            return

        try:
            referenced = await message.channel.fetch_message(message.reference.message_id)
            if not await self.has_bot_success_reaction(referenced):
                return

            signal = await self.signal_db.get_signal_by_message_id(str(referenced.id))
            if not signal:
                return

            if not is_signal_manager(self.bot, message.author):
                await self._deny_signal_management(message)
                return

            await self._handle_reply_command(
                message, referenced, signal, signal["id"], from_signal_reply=True
            )

        except Exception as e:
            self.logger.error(f"Error in signal management reply: {e}", exc_info=True)

    async def _handle_reply_command(
        self,
        message: discord.Message,
        referenced: discord.Message,
        signal: dict,
        signal_id: int,
        from_signal_reply: bool = False,
    ) -> None:
        """Unified command processor shared by both alert-reply and signal-reply paths."""
        path = "signal" if from_signal_reply else "alert"
        logger.info(
            f"Processing {path} management command for signal {signal_id}: '{message.content}'"
        )

        command_parts = message.content.lower().strip().split()
        command = command_parts[0] if command_parts else ""

        success = False
        action_taken = None

        try:
            if command in ("cancel", "nm", "cancelled"):
                success = await asyncio.wait_for(
                    self.signal_db.manually_set_signal_status(
                        signal_id,
                        "cancelled",
                        f"Cancelled via {path} reply by {message.author.name}",
                    ),
                    timeout=5.0,
                )
                if success and self.bot.services.monitor:
                    self.bot.services.monitor.sync_signal_status_in_memory(signal_id, "cancelled")
                    await self.bot.services.monitor.finalize_trailing_on_manual_close(signal_id)
                action_taken = "cancelled"

            elif command in ("profit", "win", "tp"):
                # Auto-hit first pending limit if no limits have been hit yet
                if not signal.get("hit_limits"):
                    pending = sorted(
                        signal.get("pending_limits") or [],
                        key=lambda l: l.get("sequence_number", 999),
                    )
                    if pending:
                        try:
                            await db.mark_limit_hit(pending[0]["id"], pending[0]["price_level"])
                            if self.bot.services.monitor:
                                self.bot.services.monitor._mutate_limit_hit_in_memory(
                                    signal_id, pending[0]["id"], pending[0]["price_level"]
                                )
                            signal = (
                                await self.signal_db.get_signal_with_limits(signal_id) or signal
                            )
                        except Exception as _he:
                            logger.warning(
                                f"Could not auto-hit limit for signal {signal_id} on profit reply: {_he}"
                            )
                success = await asyncio.wait_for(
                    self.signal_db.manually_set_signal_status(
                        signal_id,
                        "profit",
                        f"Set via {path} reply by {message.author.name}",
                    ),
                    timeout=5.0,
                )
                if success and self.bot.services.monitor:
                    self.bot.services.monitor.sync_signal_status_in_memory(signal_id, "profit")
                    await self.bot.services.monitor.finalize_trailing_on_manual_close(signal_id)
                action_taken = "marked as PROFIT"

            elif command in ("hit",):
                was_cancelled = signal.get("status") == "cancelled"
                transitioned = await asyncio.wait_for(
                    self.signal_db.manually_set_signal_to_hit(
                        signal_id, f"Set via {path} reply by {message.author.name}"
                    ),
                    timeout=5.0,
                )
                if transitioned:
                    if self.bot.services.monitor:
                        monitor = self.bot.services.monitor
                        await self.bot.services.tp_monitor.refresh_hit_limits(signal_id)
                        if signal_id in monitor.active_signals:
                            monitor.active_signals[signal_id]["status"] = "hit"
                            monitor.mark_first_pending_limit_hit_in_memory(signal_id)
                        elif was_cancelled:
                            reloaded = await self.signal_db.get_signal_with_limits(signal_id)
                            if reloaded:
                                reloaded_for_monitor = dict(reloaded)
                                reloaded_for_monitor["signal_id"] = signal_id
                                reloaded_for_monitor["status"] = "hit"
                                monitor.active_signals[signal_id] = reloaded_for_monitor
                                monitor._annotate_asset_class(reloaded_for_monitor)
                                sym = signal.get("instrument")
                                if sym:
                                    monitor.symbol_to_signals.setdefault(sym, [])
                                    if signal_id not in monitor.symbol_to_signals[sym]:
                                        monitor.symbol_to_signals[sym].append(signal_id)
                                    await monitor.stream_manager.bulk_subscribe([sym])
                    success = True
                    action_taken = "marked as HIT"
                else:
                    success = False
                    action_taken = None

            elif command in ("breakeven", "be"):
                success = await asyncio.wait_for(
                    self.signal_db.manually_set_signal_status(
                        signal_id,
                        "breakeven",
                        f"Set via {path} reply by {message.author.name}",
                    ),
                    timeout=5.0,
                )
                if success and self.bot.services.monitor:
                    self.bot.services.monitor.sync_signal_status_in_memory(signal_id, "breakeven")
                    await self.bot.services.monitor.finalize_trailing_on_manual_close(signal_id)
                action_taken = "marked as BREAKEVEN"

            elif command in ("sl", "stop", "stoploss"):
                success = await asyncio.wait_for(
                    self.signal_db.manually_set_signal_status(
                        signal_id,
                        "stop_loss",
                        f"Set via {path} reply by {message.author.name}",
                    ),
                    timeout=5.0,
                )
                if success and self.bot.services.monitor:
                    self.bot.services.monitor.sync_signal_status_in_memory(signal_id, "stop_loss")
                    await self.bot.services.monitor.finalize_trailing_on_manual_close(signal_id)
                action_taken = "marked as STOP LOSS"

            elif command in ("reactivate", "reopen", "active"):
                if signal["status"] not in ("cancelled", "stop_loss"):
                    await message.reply(
                        f"❌ Signal is not reactivatable (current status: {signal['status']})",
                        delete_after=_REPLY_DELETE_AFTER,
                    )
                    await self._safe_delete(message)
                    return

                try:
                    guard = await self.signal_db.check_reactivation_guard(signal_id)
                except Exception as _ge:
                    logger.warning(f"Reactivation guard check failed for signal {signal_id}: {_ge}")
                    guard = None

                if guard and guard["blocked"]:
                    instrument = guard["instrument"]
                    cur = format_price(guard["current_price"], instrument)
                    limit_lines = "\n".join(
                        f"• Limit #{lim['sequence_number']}: "
                        f"{format_price(float(lim['price_level']), instrument)}"
                        for lim in guard["blocked_limits"]
                    )
                    await message.reply(
                        f"❌ Cannot reactivate — price has already moved past pending limits.\n"
                        f"Current price: **{cur}**\n"
                        f"**Limits past:**\n{limit_lines}\n\n"
                        f"An admin can use `!setstatus {signal_id} active --force` to override.",
                        delete_after=_REPLY_DELETE_AFTER,
                    )
                    await self._safe_delete(message)
                    return

                parsed = None
                if from_signal_reply:
                    channel_name = self.get_channel_name(referenced.channel.id)
                    try:
                        parsed = parse_signal(referenced.content, channel_name)
                    except Exception as e:
                        logger.debug(f"Could not parse referenced content for reactivate: {e}")
                elif signal.get("message_id") and signal.get("channel_id"):
                    try:
                        original_channel = self.bot.get_channel(int(signal["channel_id"]))
                        if original_channel is None:
                            original_channel = await self.bot.fetch_channel(
                                int(signal["channel_id"])
                            )
                        original_message = await original_channel.fetch_message(
                            int(signal["message_id"])
                        )
                        channel_name = self.get_channel_name(int(signal["channel_id"]))
                        parsed = parse_signal(original_message.content, channel_name)
                    except Exception as e:
                        logger.info(
                            f"Could not fetch original message for signal {signal_id} "
                            f"— reactivating from DB state: {e}"
                        )
                success = await asyncio.wait_for(
                    self.signal_db.reactivate_cancelled_signal(signal_id, parsed),
                    timeout=5.0,
                )
                if success:
                    action_taken = "reactivated"
                    if self.bot.services.nm_monitor:
                        self.bot.services.nm_monitor.mark_immune(signal_id)
                    # Re-add to the streaming monitor immediately. Without this
                    # the signal is invisible to price ticks until the 30s
                    # periodic refresh picks it back up.
                    if self.bot.services.monitor:
                        await self.bot.services.monitor.refresh_signal_in_memory(signal_id)
                    # Re-fetch so the embed update + downstream code see the
                    # post-reactivation state (status active/hit, limits pending).
                    refreshed = await self.signal_db.get_signal_with_limits(signal_id)
                    if refreshed:
                        signal = refreshed

            else:
                await message.reply(
                    "❓ Unknown command. Valid commands: `cancel`, `profit`, `tp`, `breakeven`, `be`, `sl`, `stop`, `reactivate`",
                    delete_after=_REPLY_DELETE_AFTER,
                )
                await self._safe_delete(message)
                return

        except asyncio.TimeoutError:
            logger.error(f"Operation timed out for command: {command}")
            await message.reply(
                f"❌ {command.title()} operation timed out. Please try again.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return
        except Exception as e:
            logger.error(f"Error processing command '{command}': {e}", exc_info=True)
            await message.reply(
                f"❌ Error processing {command} command.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        if not (success and action_taken):
            await message.reply("❌ Failed to process command.", delete_after=_REPLY_DELETE_AFTER)
            logger.warning(f"Failed to process command '{command}' for signal {signal_id}")
            await self._safe_delete(message)
            return

        # Update reactions on referenced message
        if action_taken == "cancelled":
            await self._safe_remove_reaction(referenced, "✅")
            await referenced.add_reaction("❌")
            if from_signal_reply:
                sig_id = signal["signal_id"]
                has_embed = self.alert_system and sig_id in self.alert_system.signal_messages
                if not has_embed:
                    is_purge_channel = (
                        self.alert_system
                        and self.alert_system.is_auto_purge_channel(referenced.channel.id)
                    )
                    if is_purge_channel:
                        try:
                            await referenced.delete()
                            logger.info(
                                f"Deleted original signal message {referenced.id} "
                                f"(signal {sig_id} cancelled with no alert embed)"
                            )
                        except Exception as _de:
                            logger.warning(
                                f"Could not delete original signal message {referenced.id}: {_de}"
                            )
                    if is_purge_channel and self.alert_system:
                        try:
                            finished_channel = self.alert_system._get_finished_channel()
                            if finished_channel:
                                guild_id_val = signal.get("guild_id")
                                if not guild_id_val and self.bot.guilds:
                                    guild_id_val = self.bot.guilds[0].id
                                _embed_limits = (
                                    signal.get("limits") or signal.get("pending_limits") or []
                                )
                                try:
                                    _full = await self.signal_db.get_signal_with_limits(sig_id)
                                    if _full:
                                        _embed_limits = _full.get("limits") or _embed_limits
                                except Exception as _lfe:
                                    self.logger.warning(
                                        f"Could not fetch limits for cancelled embed (signal {sig_id}): {_lfe}"
                                    )
                                cancel_embed = _build_signal_embed(
                                    signal=signal,
                                    limits=_embed_limits,
                                    event="cancelled",
                                    guild_id=guild_id_val,
                                    bot=self.bot,
                                )
                                _set_archive_footer(cancel_embed)
                                ping_line = (
                                    f"{self.alert_system.role_mention} ❌ **{signal['instrument']}** "
                                    f"{signal['direction'].upper()} — cancelled by sender "
                                    f"(by {message.author.display_name})"
                                )
                                await finished_channel.send(ping_line)
                                cancel_embed_msg = await finished_channel.send(embed=cancel_embed)
                                # Register so the user can reply "reactivate" to this embed
                                # and reactivate_embed() can delete it on reactivation.
                                self.alert_system.track_alert_message(cancel_embed_msg.id, sig_id)
                                self.alert_system.signal_finished_messages[sig_id] = (
                                    cancel_embed_msg
                                )
                                try:
                                    async with self.signal_db.db.get_connection() as conn:
                                        await conn.execute(
                                            "UPDATE signals "
                                            "SET finished_message_id = $1, finished_channel_id = $2 "
                                            "WHERE id = $3",
                                            int(cancel_embed_msg.id),
                                            int(finished_channel.id),
                                            int(sig_id),
                                        )
                                except Exception as _pe:
                                    logger.warning(
                                        f"Could not persist finished embed IDs for signal {sig_id}: {_pe}"
                                    )
                                logger.info(
                                    f"Sent direct cancellation embed to finished-signals "
                                    f"for signal {sig_id} (no prior alert embed)"
                                )
                        except Exception as _fe:
                            logger.warning(
                                f"Could not send cancellation embed to finished-signals "
                                f"for signal {sig_id}: {_fe}"
                            )
        elif action_taken == "marked as HIT":
            await referenced.add_reaction("🎯")
        elif action_taken == "marked as PROFIT":
            await referenced.add_reaction("💰")
        elif action_taken == "marked as BREAKEVEN":
            await referenced.add_reaction("➖")
        elif action_taken == "marked as STOP LOSS":
            await referenced.add_reaction("🛑")
        elif action_taken == "reactivated":
            await self._safe_remove_reaction(referenced, "❌")
            await referenced.add_reaction("✅")
            await referenced.add_reaction("♻️")

        # For alert-reply path, also react on the original signal message
        if not from_signal_reply:
            await self._react_to_original_signal(signal, action_taken)

        # Delete user reply to reduce clutter
        await self._safe_delete(message)

        # Update the persistent embed and send a ping
        if self.alert_system:
            event_map = {
                "cancelled": "cancelled",
                "marked as PROFIT": "profit",
                "marked as HIT": "hit",
                "marked as BREAKEVEN": "breakeven",
                "marked as STOP LOSS": "stop_loss",
                "reactivated": "reactivated",
            }
            embed_event = event_map.get(action_taken)
            if embed_event:
                action_emoji_map = {
                    "profit": "💰",
                    "hit": "🎯",
                    "stop_loss": "🛑",
                    "breakeven": "➖",
                    "cancelled": "❌",
                    "reactivated": "♻️",
                }
                emoji = action_emoji_map.get(embed_event, "✅")
                ping_text = (
                    f"{emoji} **{signal['instrument']}** {signal['direction'].upper()} — "
                    f"manually {action_taken.lower()} (by {message.author.display_name})"
                )
                try:
                    if embed_event == "reactivated":
                        await self.alert_system.reactivate_embed(signal=signal, ping_text=ping_text)
                    else:
                        await self.alert_system.update_signal_message(
                            signal=signal, event=embed_event, ping_text=ping_text
                        )
                except Exception as _ue:
                    logger.warning(f"Could not update signal embed after manual command: {_ue}")

        logger.info(f"Signal {signal_id} {action_taken} via {path} reply by {message.author.name}")

    def _build_save_context(self, parsed) -> dict:
        """
        Save-time analysis stamps: the TP threshold that applies to this signal
        right now, and minutes until the next news event affecting its instrument.
        Best-effort — signal saving must never fail because of this.
        """
        context = {}
        try:
            signal_type = getattr(parsed, "type", "standard")
            tp_config = self.tp_config
            services = getattr(self.bot, "services", None)
            if services is not None and getattr(services, "tp_config", None) is not None:
                tp_config = services.tp_config
            context["tp_threshold_used"] = tp_config.get_tp_value(
                parsed.instrument, signal_type=signal_type
            )
            context["tp_threshold_unit"] = tp_config.get_tp_type(
                parsed.instrument, signal_type=signal_type
            )
        except Exception as e:
            self.logger.warning(f"TP threshold stamp failed for {parsed.instrument}: {e}")
        try:
            news_manager = getattr(self.bot, "news_manager", None)
            if news_manager is not None:
                now = datetime.now(pytz.UTC)
                upcoming = [
                    e.news_time
                    for e in news_manager.get_all_events()
                    if e.news_time > now and e.instrument_affected(parsed.instrument)
                ]
                if upcoming:
                    context["minutes_to_news"] = int(
                        (min(upcoming) - now).total_seconds() // 60
                    )
        except Exception as e:
            self.logger.warning(f"minutes_to_news stamp failed for {parsed.instrument}: {e}")
        return context

    async def process_signal(self, message: discord.Message):
        """Process a potential trading signal with enhanced parsing"""
        try:
            channel_name = self.get_channel_name(message.channel.id)
            parsed = parse_signal(message.content, channel_name)

            if isinstance(parsed, RejectedSignal):
                # Signal looks valid but is malformed (e.g. out-of-order limits = typo).
                # React ❌ so the user knows to fix and re-edit the message.
                await self.safe_add_reaction(message, "❌")
                self.logger.info(
                    f"Signal rejected as malformed (likely typo) in message {message.id}: "
                    f"{parsed.reason}"
                )
                return

            if parsed:
                context = self._build_save_context(parsed)
                success, signal_id = await self.signal_db.save_signal(
                    parsed, str(message.id), str(message.channel.id), context=context
                )

                if success:
                    await self.safe_add_reaction(message, "✅")
                    self.logger.info(
                        f"Signal #{signal_id} processed: {parsed.instrument} {parsed.direction}"
                    )

                    if parsed.limits and signal_id:
                        min_limit = min(parsed.limits)
                        max_limit = max(parsed.limits)
                        try:
                            overlapping = await self.signal_db.get_overlapping_signals(
                                parsed.instrument, min_limit, max_limit, signal_id
                            )
                            if overlapping:
                                asyncio.create_task(
                                    self._handle_overlap_prompt(message, signal_id, overlapping)
                                )
                        except Exception as _oe:
                            self.logger.warning(f"Overlap check failed for signal {signal_id}: {_oe}")
                else:
                    existing = await self.signal_db.get_signal_by_message_id(str(message.id))
                    if existing and existing["status"] != "cancelled":
                        await self.safe_add_reaction(message, "⚠️")
                    else:
                        await self.safe_add_reaction(message, "♻️")
            elif self.looks_like_signal(message.content):
                await self.safe_add_reaction(message, "⚠️")
                self.logger.debug(f"Failed to parse apparent signal from message {message.id}")

        except Exception as e:
            self.logger.error(f"Error processing signal: {str(e)!r}", exc_info=True)
            await self.safe_add_reaction(message, "⚠️")

    async def _handle_overlap_prompt(
        self,
        message: discord.Message,
        new_signal_id: int,
        overlapping: list,
    ):
        """
        Post an overlap warning in the same channel, wait 30 s for a reaction,
        then cancel the old signal(s) (✅ or timeout) or keep both (❌).
        Deleted the prompt message afterward regardless of outcome.
        """
        try:
            guild_id = message.guild.id if message.guild else None
            channel = message.channel

            parts = []
            for sig in overlapping:
                msg_id = str(sig["message_id"])
                ch_id = sig["channel_id"]
                label = f"Signal #{sig['id']} ({sig['instrument']} {sig['direction'].upper()})"
                if guild_id and not msg_id.startswith("manual_"):
                    url = f"https://discord.com/channels/{guild_id}/{ch_id}/{msg_id}"
                    label = f"[{label}]({url})"
                parts.append(f"• {label}")

            overlap_list = "\n".join(parts)
            prompt_content = (
                f"⚠️ **Overlap Detected** — Signal #{new_signal_id} overlaps with:\n"
                f"{overlap_list}\n\n"
                f"✅ — Cancel the old signal(s), keep this one\n"
                f"❌ — Keep both signals active\n"
                f"*(Auto-cancels old signal(s) in 30 seconds if no reaction)*"
            )

            prompt_msg = await channel.send(prompt_content)
            await prompt_msg.add_reaction("✅")
            await prompt_msg.add_reaction("❌")

            def _check(reaction, user):
                if user.bot or reaction.message.id != prompt_msg.id:
                    return False
                if str(reaction.emoji) not in ("✅", "❌"):
                    return False
                return is_signal_manager(self.bot, user)

            cancel_old = True  # default on timeout
            try:
                reaction, _ = await self.bot.wait_for(
                    "reaction_add", timeout=30.0, check=_check
                )
                cancel_old = str(reaction.emoji) == "✅"
            except asyncio.TimeoutError:
                pass

            if cancel_old:
                monitor = self.bot.services.monitor if self.bot.services else None
                alert_system = self.alert_system or (
                    self.bot.services.alert_system if self.bot.services else None
                )
                for sig in overlapping:
                    old_id = sig["id"]
                    ok = await self.signal_db.manually_set_signal_status(
                        old_id,
                        "cancelled",
                        f"Cancelled — overlapped by new signal #{new_signal_id}",
                    )
                    if not ok:
                        continue

                    if monitor:
                        monitor.sync_signal_status_in_memory(old_id, "cancelled")
                        await monitor.finalize_trailing_on_manual_close(old_id)
                        monitor.active_signals.pop(old_id, None)
                        try:
                            monitor.nm_monitor.evict_signal(old_id)
                            monitor.tp_monitor.evict_signal(old_id)
                        except Exception:
                            pass

                    try:
                        await react_to_original_signal(self.bot, sig, "❌")
                    except Exception:
                        pass

                    if alert_system:
                        try:
                            ping = (
                                f"❌ **{sig['instrument']}** {sig['direction'].upper()} — "
                                f"cancelled (overlapped by signal #{new_signal_id})"
                            )
                            await alert_system.update_embed_for_signal_id(
                                old_id, "cancelled", ping_text=ping
                            )
                        except Exception as _ue:
                            self.logger.warning(
                                f"Could not update embed for overlapping signal {old_id}: {_ue}"
                            )

                self.logger.info(
                    f"Cancelled {len(overlapping)} overlapping signal(s) for new signal {new_signal_id}"
                )

            try:
                await prompt_msg.delete()
            except Exception:
                pass

        except Exception as e:
            self.logger.error(f"Error in overlap prompt for signal {new_signal_id}: {e}", exc_info=True)

    async def safe_add_reaction(self, message: discord.Message, emoji: str):
        """Safely add a reaction to a message, handling common Discord API errors"""
        try:
            await message.add_reaction(emoji)
        except discord.NotFound:
            self.logger.warning(
                f"Could not add reaction to message {message.id} - message not found"
            )
        except discord.Forbidden:
            self.logger.warning(
                f"Could not add reaction to message {message.id} - missing permissions"
            )
        except discord.HTTPException as e:
            self.logger.warning(
                f"Could not add reaction to message {message.id} - HTTP error: {str(e)!r}"
            )
        except Exception as e:
            self.logger.error(f"Unexpected error adding reaction: {str(e)!r}", exc_info=False)

    async def handle_message_edit(self, message: discord.Message):
        """Handle message edits with signal reparsing"""
        if message.author.bot:
            return

        if not self.is_allowed_channel(message.channel.id):
            return

        if message.channel.id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message edited in monitored channel: {message.channel.name}")

        existing = await self.signal_db.get_signal_by_message_id(str(message.id))
        if not existing:
            await message.clear_reactions()
            await self.process_signal(message)
            return

        channel_name = self.get_channel_name(message.channel.id)
        parsed = parse_signal(message.content, channel_name)

        if isinstance(parsed, RejectedSignal):
            await message.clear_reactions()
            await message.add_reaction("❌")
            self.logger.info(
                f"Signal edit rejected as malformed (likely typo): {message.id}: {parsed.reason}"
            )
            return

        if parsed:
            if existing["status"] == "cancelled":
                reactivated = await self.signal_db.reactivate_cancelled_signal(
                    existing["id"], parsed
                )
                if reactivated:
                    await self.signal_db.update_signal_from_edit(str(message.id), parsed)

                    if self.bot.services.nm_monitor:
                        self.bot.services.nm_monitor.mark_immune(existing["id"])

                    monitor = self.bot.services.monitor
                    if monitor:
                        await monitor.refresh_signal_in_memory(existing["id"])

                    await message.clear_reactions()
                    await message.add_reaction("✅")
                    await message.add_reaction("♻️")
                    self.logger.info(f"Cancelled signal reactivated after edit: {message.id}")

                    if self.alert_system:
                        try:
                            updated_signal = await self.signal_db.get_signal_with_limits(
                                existing["id"]
                            )
                            if updated_signal:
                                ping_text = (
                                    f"♻️ **{updated_signal['instrument']}** {updated_signal['direction'].upper()} — "
                                    f"signal reactivated by sender (edited)"
                                )
                                await self.alert_system.update_signal_message(
                                    signal=updated_signal,
                                    event="reactivated",
                                    ping_text=ping_text,
                                )
                        except Exception as _ue:
                            self.logger.warning(
                                f"Could not update embed after reactivation via edit: {_ue}"
                            )
                else:
                    await message.add_reaction("❌")
                    self.logger.warning(
                        f"Failed to reactivate cancelled signal on edit: {message.id}"
                    )
                return

            success, alert_invalidated = await self.signal_db.update_signal_from_edit(
                str(message.id), parsed
            )

            if success:
                await message.clear_reactions()
                await message.add_reaction("✅")
                await message.add_reaction("📝")
                self.logger.info(f"Signal updated after edit: {message.id}")

                monitor = self.bot.services.monitor
                if monitor:
                    await monitor.refresh_signal_in_memory(existing["id"])

                if self.alert_system:
                    try:
                        if alert_invalidated:
                            # The edit corrected a limit that had already fired a (false)
                            # approaching/hit alert — retract the stale embed/ping so a
                            # corrected alert fires fresh when the real level is reached.
                            await self.alert_system.retract_approaching_embed(existing["id"])
                            self.logger.info(
                                f"Retracted stale alert embed after corrective edit: {message.id}"
                            )
                        else:
                            updated_signal = await self.signal_db.get_signal_with_limits(
                                existing["id"]
                            )
                            if updated_signal:
                                ping_text = (
                                    f"📝 **{updated_signal['instrument']}** {updated_signal['direction'].upper()} — "
                                    f"signal updated by sender"
                                )
                                await self.alert_system.update_signal_message(
                                    signal=updated_signal,
                                    event="edited",
                                    ping_text=ping_text,
                                )
                    except Exception as _ue:
                        self.logger.warning(f"Could not update embed after signal edit: {_ue}")
            elif existing["status"] in ["profit", "breakeven", "stop_loss"]:
                await message.add_reaction("🔒")
                self.logger.info(f"Cannot update signal in final status: {existing['status']}")
            else:
                # The DB update failed and rolled back (e.g. a constraint violation),
                # leaving the old limits in place. Surface it instead of silently
                # swallowing the failure — otherwise the signal keeps showing stale
                # limits in !active with no indication the edit didn't take.
                await message.add_reaction("⚠️")
                self.logger.error(
                    f"Signal edit failed to persist for message {message.id} "
                    f"(signal {existing['id']}, status {existing['status']}); limits unchanged"
                )
        else:
            await message.clear_reactions()
            await message.add_reaction("❌")
            self.logger.info(f"Signal parse failed after edit: {message.id}")

    async def handle_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle message deletions with signal cancellation"""
        if not self.is_allowed_channel(payload.channel_id):
            return

        if payload.channel_id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message deleted in monitored channel: {payload.message_id}")
        result = await self.signal_db.cancel_signal_by_message(str(payload.message_id))

        if result:
            self.logger.info(f"Signal cancelled due to message deletion: {payload.message_id}")
            if self.alert_system:
                try:
                    cancelled_signal = await self.signal_db.get_signal_by_message_id(
                        str(payload.message_id)
                    )
                    if cancelled_signal:
                        await self.alert_system.update_embed_for_signal_id(
                            cancelled_signal["id"], "cancelled"
                        )
                except Exception as _ue:
                    self.logger.warning(
                        f"Could not update embed after message delete cancel: {_ue}"
                    )

    def looks_like_signal(self, text: str) -> bool:
        """Check if text appears to be a trading signal"""
        text = re.sub(r"<@&\d+>.*", "", text).strip().lower()
        has_numbers = bool(re.search(r"\d+\.?\d*", text))
        keywords = ["stop", "sl", "long", "short", "buy", "sell", "entry"]
        has_keywords = any(word in text for word in keywords)
        return has_numbers and has_keywords

    async def has_bot_success_reaction(self, message: discord.Message) -> bool:
        """Check if message has a ✅ reaction from the bot"""
        for reaction in message.reactions:
            if str(reaction.emoji) == "✅":
                async for user in reaction.users():
                    if user.id == self.bot.user.id:
                        return True
        return False

    def get_channel_name(self, channel_id: int) -> Optional[str]:
        return _get_channel_name(self.bot.channels_config, channel_id)
