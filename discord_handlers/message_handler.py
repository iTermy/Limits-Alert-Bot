"""
Message Handler
"""

import asyncio
import contextlib
import re
from datetime import datetime
from typing import ClassVar, Optional

import discord
import pytz

from core.parser import RejectedSignal, parse_signal
from database import db
from database.signal_ops import MAX_INSTANT_ENTRIES
from models.signal import LimitData, SignalData, breakeven_price
from price_feeds.alerting.embed_builders import _build_signal_embed, _set_archive_footer
from price_feeds.monitors.streaming_monitor import react_to_original_signal
from price_feeds.config.tp_config import TPConfig
from utils.formatting import format_price
from utils.formatting import get_channel_name as _get_channel_name
from utils.logger import get_logger
from utils.permissions import is_signal_manager

logger = get_logger("message_handler")

# Auto-delete delay for transient bot replies in monitored / alert channels.
_REPLY_DELETE_AFTER = 15.0

# Failure/timeout notices linger longer so a command that did NOT apply to a
# live signal can't vanish before the user notices it.
_ERROR_REPLY_DELETE_AFTER = 60.0

# Status-change reply commands retry once on timeout: a transient event-loop
# stall can blow the per-attempt window, and a missed cancel on a live signal
# is worse than a second attempt.
#
# The budget has to clear the round-trip cost of a full status transition
# against the Supabase pooler, which measured 2-5s in production — a 5s window
# left no headroom and timed out on the heaviest path (profit on a hit signal).
_STATUS_CALL_TIMEOUT = 15.0
_STATUS_CALL_ATTEMPTS = 2
_STATUS_RETRY_DELAY = 0.5

# Budget for the post-timeout status re-read (see _status_already_applied).
_STATUS_VERIFY_TIMEOUT = 5.0

# Oldest live price accepted as the entry for an instant-entry signal. Healthy
# feeds tick several times a second; anything older means the market is closed
# or the feed has stalled, and entering there would book a fictional price.
_INSTANT_ENTRY_MAX_PRICE_AGE = 15.0

# Reply phrases that arm or disarm a signal's breakeven stop. Kept apart from
# the bare "be" command, which closes the signal at breakeven right away.
_ARM_BE_PHRASES = frozenset({"set be", "set breakeven"})
_DISARM_BE_PHRASES = frozenset({"unset be", "unset breakeven", "remove be"})

# DM sent when a non-manager tries to manage a signal via reply.
_NO_PERMISSION_DM = (
    "You don't have permission to manage signals. "
    "If you'd like access, please ask an admin."
)


async def _await_with_retry(coro_factory, *, label: str):
    """Await a coroutine under a timeout, retrying once on TimeoutError.

    coro_factory must return a fresh coroutine on each call (a coroutine can
    only be awaited once). Raises TimeoutError if every attempt times out.
    """
    for attempt in range(1, _STATUS_CALL_ATTEMPTS + 1):
        try:
            return await asyncio.wait_for(coro_factory(), timeout=_STATUS_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            if attempt >= _STATUS_CALL_ATTEMPTS:
                raise
            logger.warning(
                f"{label} timed out (attempt {attempt}/{_STATUS_CALL_ATTEMPTS}) — retrying"
            )
            await asyncio.sleep(_STATUS_RETRY_DELAY)


class MessageHandler:
    """Handles all message-related events for signal processing"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.signal_db = bot.signal_db
        self.tp_config = TPConfig()
        self.alert_system = None  # Set by monitor when initialized
    def is_allowed_channel(self, channel_id: int) -> bool:
        return channel_id in self.bot.allowed_channel_ids

    async def _safe_remove_reaction(self, message: discord.Message, emoji: str) -> None:
        with contextlib.suppress(Exception):
            await message.remove_reaction(emoji, self.bot.user)

    async def _safe_delete(self, message: discord.Message) -> None:
        with contextlib.suppress(Exception):
            await message.delete()

    async def _deny_signal_management(self, message: discord.Message) -> None:
        """Delete an unauthorized management reply and DM the user why."""
        try:
            await message.author.send(_NO_PERMISSION_DM)
        except discord.Forbidden:
            self.logger.debug(
                f"Could not DM {message.author} about denied signal management (DMs closed)"
            )
        except Exception as e:
            self.logger.warning(f"Error DMing user about denied signal management: {e}")
        await self._safe_delete(message)

    async def handle_new_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.channel.id in self.bot.monitored_channels:
            self.logger.debug(f"New message in monitored channel: {message.channel.name}")
            await self.process_signal(message)
        await self.check_signal_management_reply(message)
        await self.check_alert_management_reply(message)

    async def _react_to_original_signal(self, signal: SignalData, action_taken: str):
        """Add a reaction to the original signal message based on the action taken."""
        try:
            message_id = signal.message_id
            channel_id = signal.channel_id
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
            self.logger.debug(
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
                logger.debug("Alert system resolved from services")
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

            row = await self.signal_db.get_signal_by_message_id(str(referenced.id))
            if not row:
                return

            if not is_signal_manager(self.bot, message.author):
                await self._deny_signal_management(message)
                return

            signal = await self.signal_db.get_signal_with_limits(row["id"])
            if not signal:
                return

            await self._handle_reply_command(
                message, referenced, signal, signal.signal_id, from_signal_reply=True
            )

        except Exception as e:
            self.logger.error(f"Error in signal management reply: {e}", exc_info=True)

    # Reply commands that map straight onto a manual status change:
    # command aliases -> (new status, action label)
    _REPLY_STATUS_COMMANDS: ClassVar[dict] = {
        "cancel": ("cancelled", "cancelled"),
        "nm": ("cancelled", "cancelled"),
        "cancelled": ("cancelled", "cancelled"),
        "profit": ("profit", "marked as PROFIT"),
        "win": ("profit", "marked as PROFIT"),
        "tp": ("profit", "marked as PROFIT"),
        "breakeven": ("breakeven", "marked as BREAKEVEN"),
        "be": ("breakeven", "marked as BREAKEVEN"),
        "sl": ("stop_loss", "marked as STOP LOSS"),
        "stop": ("stop_loss", "marked as STOP LOSS"),
        "stoploss": ("stop_loss", "marked as STOP LOSS"),
    }

    async def _handle_reply_command(
        self,
        message: discord.Message,
        referenced: discord.Message,
        signal: SignalData,
        signal_id: int,
        from_signal_reply: bool = False,
    ) -> None:
        """Unified command processor shared by both alert-reply and signal-reply paths."""
        path = "signal" if from_signal_reply else "alert"
        logger.debug(
            f"Processing {path} management command for signal {signal_id}: '{message.content}'"
        )

        command_parts = message.content.lower().strip().split()
        command = command_parts[0] if command_parts else ""
        phrase = " ".join(command_parts)

        success = False
        action_taken = None

        try:
            if phrase in _ARM_BE_PHRASES or phrase in _DISARM_BE_PHRASES:
                # Protection, not a status change — handled end to end here so it
                # skips the terminal-command tail below.
                await self._reply_breakeven_stop(
                    message, path, signal, signal_id, arm=phrase in _ARM_BE_PHRASES
                )
                return

            if command == "add":
                # Averages into an open position rather than changing its status,
                # so it skips the terminal-command tail below like `set be` does.
                await self._reply_add_entry(message, path, signal, signal_id)
                return

            if command in self._REPLY_STATUS_COMMANDS:
                status, action_label = self._REPLY_STATUS_COMMANDS[command]
                if status == "profit":
                    signal = await self._auto_hit_first_pending(signal, signal_id)
                success = await self._reply_set_status(message, path, signal_id, status)
                action_taken = action_label if success else None

            elif command in ("hit",):
                success = await self._reply_hit(message, path, signal, signal_id)
                action_taken = "marked as HIT" if success else None

            elif command in ("reactivate", "reopen", "active"):
                success, signal = await self._reply_reactivate(
                    message, referenced, signal, signal_id, from_signal_reply
                )
                if not success:
                    return  # _reply_reactivate already replied with the reason
                action_taken = "reactivated"

            else:
                await message.reply(
                    "❓ Unknown command. Valid commands: `cancel`, `profit`, `tp`, `breakeven`, `be`, `set be`, `add`, `sl`, `stop`, `reactivate`",
                    delete_after=_REPLY_DELETE_AFTER,
                )
                await self._safe_delete(message)
                return

        except asyncio.TimeoutError:
            logger.error(f"Operation timed out for command: {command}")
            await message.reply(
                await self._describe_timeout_outcome(signal_id, command),
                delete_after=_ERROR_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return
        except Exception as e:
            logger.error(f"Error processing command '{command}': {e}", exc_info=True)
            await message.reply(
                f"❌ Error processing {command} command.",
                delete_after=_ERROR_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        if not (success and action_taken):
            await message.reply(
                "❌ Failed to process command.", delete_after=_ERROR_REPLY_DELETE_AFTER
            )
            logger.warning(f"Failed to process command '{command}' for signal {signal_id}")
            await self._safe_delete(message)
            return

        await self._apply_reply_reactions(
            message, referenced, signal, action_taken, from_signal_reply
        )

        # For alert-reply path, also react on the original signal message
        if not from_signal_reply:
            await self._react_to_original_signal(signal, action_taken)

        # Delete user reply to reduce clutter
        await self._safe_delete(message)

        await self._refresh_embed_after_reply(signal, action_taken, message.author)

        logger.info(f"Signal {signal_id} {action_taken} via {path} reply by {message.author.name}")

    def _timeout_target_status(self, command: str) -> Optional[str]:
        """The status a reply command drives a signal to, or None if it has no
        single target (e.g. reactivate, which lands on active or hit)."""
        if command in self._REPLY_STATUS_COMMANDS:
            return self._REPLY_STATUS_COMMANDS[command][0]
        if command == "hit":
            return "hit"
        return None

    async def _status_already_applied(self, signal_id: int, target: str) -> Optional[bool]:
        """Whether a timed-out status change actually landed.

        asyncio.wait_for cancels the DB call mid-statement, but a cancel issued
        through the Supabase session pooler does not reliably reach the backend:
        the transaction can commit after the client has given up. Without this
        check the bot reported "NOT applied" on writes that had succeeded, which
        sent users into retry loops against already-updated signals.

        Returns True if the signal now holds the target status, False if it
        demonstrably does not, and None if the status could not be re-read.
        """
        try:
            signal = await asyncio.wait_for(
                self.signal_db.get_signal_with_limits(signal_id),
                timeout=_STATUS_VERIFY_TIMEOUT,
            )
        except Exception as e:
            logger.warning(f"Could not verify status of signal {signal_id} after timeout: {e}")
            return None
        if signal is None:
            return None
        return signal.status == target

    async def _describe_timeout_outcome(self, signal_id: int, command: str) -> str:
        """Build the user-facing notice for a command that exhausted its retries."""
        target = self._timeout_target_status(command)
        if target is None:
            return (
                f"⚠️ {command.title()} timed out. Check the signal before retrying — "
                f"the change may or may not have applied."
            )

        applied = await self._status_already_applied(signal_id, target)
        if applied is None:
            return (
                f"⚠️ {command.title()} timed out and the outcome could not be verified. "
                f"Check signal {signal_id} before retrying."
            )
        if applied:
            # The commit landed between _reply_set_status's check and this one.
            # Say so rather than sending the user into a retry loop; the embed
            # may lag until the next refresh, but the status itself is correct.
            return (
                f"⚠️ {command.title()} timed out, but the change **did apply** — "
                f"signal {signal_id} is now {target.upper()}. No need to retry."
            )
        return (
            f"❌ {command.title()} timed out and was NOT applied — signal {signal_id} is "
            f"unchanged. Please try again."
        )

    async def _reply_set_status(
        self, message: discord.Message, path: str, signal_id: int, status: str
    ) -> bool:
        """Apply a manual status change plus the in-memory sync every terminal
        reply command shares."""
        verb = "Cancelled" if status == "cancelled" else "Set"
        try:
            success = await _await_with_retry(
                lambda: self.signal_db.manually_set_signal_status(
                    signal_id,
                    status,
                    f"{verb} via {path} reply by {message.author.name}",
                ),
                label=f"set-status {status} for signal {signal_id}",
            )
        except asyncio.TimeoutError:
            # The write may have committed after the client gave up. Treat a
            # confirmed commit as success so the follow-through the caller owns
            # — embed edit, reactions, in-memory sync — still runs.
            if not await self._status_already_applied(signal_id, status):
                raise
            logger.warning(
                f"set-status {status} for signal {signal_id} timed out but the write "
                f"landed — continuing with the success path"
            )
            success = True
        if success and self.bot.services.monitor:
            self.bot.services.monitor.sync_signal_status_in_memory(signal_id, status)
            await self.bot.services.monitor.finalize_trailing_on_manual_close(signal_id)
        return success

    async def _live_bid(self, instrument: str) -> Optional[float]:
        """Current bid for a symbol, or None when no live price is available.

        The breakeven stop fires on the bid in both directions, so arming it is
        validated against the same price it will later be measured on. Unlike an
        instant entry there is no staleness gate: arming over a closed market is
        a legitimate thing to do to a position already held.
        """
        stream_manager = self.bot.services.stream_manager
        if stream_manager is None:
            return None
        price = await stream_manager.get_latest_price(instrument)
        return price["bid"] if price else None

    async def _can_arm_breakeven_stop(
        self,
        message: discord.Message,
        signal: SignalData,
        signal_id: int,
        be_price: Optional[float],
    ) -> bool:
        """Whether a breakeven stop may be armed now; replies with the reason if not."""
        instrument = signal.instrument

        if signal.status != "hit" or be_price is None:
            await message.reply(
                f"❌ A breakeven stop needs an open position — signal {signal_id} is "
                f"{signal.status.upper()}.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            return False

        bid = await self._live_bid(instrument)
        if bid is None:
            await message.reply(
                f"❌ No live price for **{instrument}** — can't confirm the trade is in "
                "profit, so the breakeven stop was not armed.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            return False

        in_profit = bid > be_price if signal.direction.lower() == "long" else bid < be_price
        if not in_profit:
            await message.reply(
                f"❌ **{instrument}** is at {format_price(bid, instrument)}, not past "
                f"breakeven ({format_price(be_price, instrument)}) — arming now would close "
                "the trade immediately.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            return False

        return True

    async def _breakeven_disarm_note(self, signal: SignalData, entry: float) -> Optional[str]:
        """The ping line explaining why an added entry disarmed the breakeven stop,
        or None when the stop stays armed (or was never armed).

        Averaging in moves the breakeven point to the new mean, and averaging down
        is the main reason to add at all — so the mean lands on the far side of the
        market and the stop would close the trade on the next tick. That is exactly
        what `_can_arm_breakeven_stop` refuses to set up at arm time, arriving from
        the other direction, so the add takes precedence and the floor comes off.
        """
        if not signal.be_stop_armed_at:
            return None

        instrument = signal.instrument
        new_be = breakeven_price(signal.hit_limits + [LimitData(price_level=entry)])
        bid = await self._live_bid(instrument)

        if bid is None:
            return (
                "🛡️ Breakeven stop removed — no live price to confirm the new average "
                f"({format_price(new_be, instrument)}) is still safe."
            )

        still_safe = bid > new_be if signal.direction.lower() == "long" else bid < new_be
        if still_safe:
            return None

        return (
            f"🛡️ Breakeven stop removed — the new average "
            f"({format_price(new_be, instrument)}) is past the current price "
            f"({format_price(bid, instrument)})."
        )

    async def _reply_breakeven_stop(
        self,
        message: discord.Message,
        path: str,
        signal: SignalData,
        signal_id: int,
        arm: bool,
    ) -> None:
        """Arm or disarm a signal's breakeven stop from a reply.

        Once armed the signal closes flat when price reverses to the mean of its
        filled limits, instead of riding down to the stop loss. Take-profit and
        every other exit are untouched — this only puts a floor under the trade.
        """
        instrument = signal.instrument
        be_price = breakeven_price(signal.hit_limits)

        if arm and not await self._can_arm_breakeven_stop(message, signal, signal_id, be_price):
            await self._safe_delete(message)
            return

        if not await self.signal_db.set_breakeven_stop(signal_id, arm):
            await message.reply(
                f"❌ Could not update the breakeven stop for signal {signal_id}.",
                delete_after=_ERROR_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        signal.be_stop_armed_at = datetime.now(pytz.UTC) if arm else None
        monitor = self.bot.services.monitor
        if monitor:
            # Without this the tick path would keep evaluating the pre-arm copy
            # until the next periodic refresh, up to 30s of unprotected trade.
            await monitor.refresh_signal_in_memory(signal_id)

        if arm:
            ping = (
                f"🛡️ **{instrument}** {signal.direction.upper()} — breakeven stop armed at "
                f"{format_price(be_price, instrument)} (by {message.author.display_name})"
            )
        else:
            ping = (
                f"🛡️ **{instrument}** {signal.direction.upper()} — breakeven stop removed "
                f"(by {message.author.display_name})"
            )

        if self.alert_system:
            await self.alert_system.update_signal_message(
                signal=signal, event="hit", ping_text=ping
            )

        logger.info(
            f"Signal {signal_id} breakeven stop {'armed' if arm else 'removed'} via "
            f"{path} reply by {message.author.name}"
        )
        await self._safe_delete(message)

    async def _reply_add_entry(
        self,
        message: discord.Message,
        path: str,
        signal: SignalData,
        signal_id: int,
    ) -> None:
        """Average an instant-entry signal in with a second market entry.

        Only instant signals can take one: an ordinary signal enters on limits
        the sender chose, and a market fill added to those would be a level
        nobody asked for. The new entry shares the signal's stop loss and take
        profit, so everything downstream — TP, breakeven, exits — just sees a
        second filled limit.
        """
        instrument = signal.instrument

        if signal.take_profit is None:
            await message.reply(
                f"❌ Signal {signal_id} is not a market-entry signal — `add` only applies "
                "to those.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        if signal.status != "hit":
            await message.reply(
                f"❌ Adding an entry needs an open position — signal {signal_id} is "
                f"{signal.status.upper()}.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        if len(signal.limits) >= MAX_INSTANT_ENTRIES:
            await message.reply(
                f"❌ Signal {signal_id} already holds {MAX_INSTANT_ENTRIES} entries.",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        entry, reason = await self._market_entry_price(
            instrument, signal.direction, signal.stop_loss, signal.take_profit
        )
        if entry is None:
            await message.reply(
                f"❌ {reason} — no entry added.", delete_after=_ERROR_REPLY_DELETE_AFTER
            )
            await self._safe_delete(message)
            return

        disarm_note = await self._breakeven_disarm_note(signal, entry)

        # The disarm rides in the same locked write as the fill: a signal that is
        # briefly both averaged and still armed is a signal a tick can close flat
        # on the spot, which is the whole thing being avoided here.
        limit_id = await self.signal_db.add_instant_entry(
            signal_id, entry, disarm_breakeven=disarm_note is not None
        )
        if limit_id is None:
            await message.reply(
                f"❌ Could not add an entry to signal {signal_id}.",
                delete_after=_ERROR_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return

        monitor = self.bot.services.monitor
        if monitor:
            # The tick path, the TP monitor's hit-limit cache and the 15s embed
            # refresh all read cached copies that predate this fill.
            await monitor.refresh_signal_in_memory(signal_id)
        refreshed = await self.signal_db.get_signal_with_limits(signal_id) or signal

        ping = (
            f"➕ **{instrument}** {signal.direction.upper()} — entry added @ "
            f"{format_price(entry, instrument)} "
            f"({len(refreshed.hit_limits)}/{refreshed.total_limits} filled, "
            f"by {message.author.display_name})"
        )
        if disarm_note:
            ping += f"\n{disarm_note}"
        if self.alert_system:
            await self.alert_system.update_signal_message(
                signal=refreshed, event="hit", current_price=entry, ping_text=ping
            )

        logger.info(
            f"Signal {signal_id} took an added entry at {entry} via {path} reply "
            f"by {message.author.name}"
        )
        await self._safe_delete(message)

    async def _auto_hit_first_pending(self, signal: SignalData, signal_id: int) -> SignalData:
        """Before a profit command on a signal with no hits, mark the first
        pending limit hit so P&L has an entry price. Returns the refreshed signal."""
        if signal.hit_limits or not signal.pending_limits:
            return signal
        first = min(signal.pending_limits, key=lambda lim: lim.sequence_number)
        try:
            await db.mark_limit_hit(first.id, first.price_level)
            if self.bot.services.monitor:
                self.bot.services.monitor._mutate_limit_hit_in_memory(
                    signal_id, first.id, first.price_level
                )
            return await self.signal_db.get_signal_with_limits(signal_id) or signal
        except Exception as e:
            logger.warning(f"Could not auto-hit limit for signal {signal_id} on profit reply: {e}")
            return signal

    async def _reply_hit(
        self, message: discord.Message, path: str, signal: SignalData, signal_id: int
    ) -> bool:
        """Mark a signal HIT via reply, re-adding it to the monitor if it was
        cancelled (limits resubscribed so ticks see it immediately)."""
        was_cancelled = signal.status == "cancelled"
        try:
            transitioned = await _await_with_retry(
                lambda: self.signal_db.manually_set_signal_to_hit(
                    signal_id, f"Set via {path} reply by {message.author.name}"
                ),
                label=f"set-hit for signal {signal_id}",
            )
        except asyncio.TimeoutError:
            if not await self._status_already_applied(signal_id, "hit"):
                raise
            logger.warning(
                f"set-hit for signal {signal_id} timed out but the write landed — "
                f"continuing with the success path"
            )
            transitioned = True
        if not transitioned:
            return False

        monitor = self.bot.services.monitor
        if monitor:
            await self.bot.services.tp_monitor.refresh_hit_limits(signal_id)
            if signal_id in monitor.active_signals:
                monitor.active_signals[signal_id].status = "hit"
                monitor.mark_first_pending_limit_hit_in_memory(signal_id)
            elif was_cancelled:
                reloaded = await self.signal_db.get_signal_with_limits(signal_id)
                if reloaded:
                    reloaded.status = "hit"
                    monitor.active_signals[signal_id] = reloaded
                    monitor._annotate_asset_class(reloaded)
                    sym = signal.instrument
                    if sym:
                        monitor.symbol_to_signals.setdefault(sym, [])
                        if signal_id not in monitor.symbol_to_signals[sym]:
                            monitor.symbol_to_signals[sym].append(signal_id)
                        await monitor.stream_manager.bulk_subscribe([sym])
        return True

    async def _reply_reactivate(
        self,
        message: discord.Message,
        referenced: discord.Message,
        signal: SignalData,
        signal_id: int,
        from_signal_reply: bool,
    ) -> tuple:
        """Reactivate a cancelled/stopped signal via reply. Returns
        (success, refreshed signal); on failure the user has already been told why."""
        if signal.status not in ("cancelled", "stop_loss"):
            await message.reply(
                f"❌ Signal is not reactivatable (current status: {signal.status})",
                delete_after=_REPLY_DELETE_AFTER,
            )
            await self._safe_delete(message)
            return False, signal

        try:
            guard = await self.signal_db.check_reactivation_guard(signal_id)
        except Exception as e:
            logger.warning(f"Reactivation guard check failed for signal {signal_id}: {e}")
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
            return False, signal

        # Re-parse the original message when available so edits made while the
        # signal was cancelled are respected; fall back to DB state otherwise.
        parsed = None
        if from_signal_reply:
            channel_name = self.get_channel_name(referenced.channel.id)
            try:
                parsed = parse_signal(referenced.content, channel_name)
            except Exception as e:
                logger.debug(f"Could not parse referenced content for reactivate: {e}")
        elif signal.message_id and signal.channel_id:
            try:
                original_channel = self.bot.get_channel(int(signal.channel_id))
                if original_channel is None:
                    original_channel = await self.bot.fetch_channel(int(signal.channel_id))
                original_message = await original_channel.fetch_message(int(signal.message_id))
                channel_name = self.get_channel_name(int(signal.channel_id))
                parsed = parse_signal(original_message.content, channel_name)
            except Exception as e:
                logger.debug(
                    f"Could not fetch original message for signal {signal_id} "
                    f"— reactivating from DB state: {e}"
                )

        success = await asyncio.wait_for(
            self.signal_db.reactivate_cancelled_signal(signal_id, parsed),
            timeout=5.0,
        )
        if not success:
            return False, signal

        if self.bot.services.nm_monitor:
            self.bot.services.nm_monitor.mark_immune(signal_id)
        # Re-add to the streaming monitor immediately. Without this the signal
        # is invisible to price ticks until the 30s periodic refresh.
        if self.bot.services.monitor:
            await self.bot.services.monitor.refresh_signal_in_memory(signal_id)
        # Re-fetch so the embed update + downstream code see the
        # post-reactivation state (status active/hit, limits pending).
        refreshed = await self.signal_db.get_signal_with_limits(signal_id)
        return True, refreshed or signal

    async def _apply_reply_reactions(
        self,
        message: discord.Message,
        referenced: discord.Message,
        signal: SignalData,
        action_taken: str,
        from_signal_reply: bool,
    ) -> None:
        """Update reactions on the referenced message to reflect the action."""
        if action_taken == "cancelled":
            await self._safe_remove_reaction(referenced, "✅")
            await referenced.add_reaction("❌")
            if from_signal_reply:
                await self._handle_cancel_without_embed(message, referenced, signal)
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

    async def _handle_cancel_without_embed(
        self,
        message: discord.Message,
        referenced: discord.Message,
        signal: SignalData,
    ) -> None:
        """A signal cancelled via reply before any alert embed existed: in
        auto-purge channels the original message is deleted and a cancellation
        embed is posted to the finished-signals channel instead (tracked so a
        `reactivate` reply against it still works)."""
        sig_id = signal.signal_id
        has_embed = self.alert_system and sig_id in self.alert_system.signal_messages
        if has_embed:
            return

        is_purge_channel = self.alert_system and self.alert_system.is_auto_purge_channel(
            referenced.channel.id
        )
        if not is_purge_channel:
            return

        try:
            await referenced.delete()
            logger.debug(
                f"Deleted original signal message {referenced.id} "
                f"(signal {sig_id} cancelled with no alert embed)"
            )
        except Exception as e:
            logger.warning(f"Could not delete original signal message {referenced.id}: {e}")

        if not self.alert_system:
            return
        try:
            finished_channel = self.alert_system._get_finished_channel()
            if not finished_channel:
                return

            guild_id_val = signal.guild_id
            if not guild_id_val and self.bot.guilds:
                guild_id_val = self.bot.guilds[0].id
            _embed_limits = signal.limits
            try:
                _full = await self.signal_db.get_signal_with_limits(sig_id)
                if _full:
                    _embed_limits = _full.limits or _embed_limits
            except Exception as e:
                self.logger.warning(
                    f"Could not fetch limits for cancelled embed (signal {sig_id}): {e}"
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
                f"{self.alert_system.role_mention} ❌ **{signal.instrument}** "
                f"{signal.direction.upper()} — cancelled by sender "
                f"(by {message.author.display_name})"
            )
            await finished_channel.send(ping_line)
            cancel_embed_msg = await finished_channel.send(embed=cancel_embed)
            # Register so the user can reply "reactivate" to this embed
            # and reactivate_embed() can delete it on reactivation.
            self.alert_system.track_alert_message(cancel_embed_msg.id, sig_id)
            self.alert_system.signal_finished_messages[sig_id] = cancel_embed_msg
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
            except Exception as e:
                logger.warning(f"Could not persist finished embed IDs for signal {sig_id}: {e}")
            logger.debug(
                f"Sent direct cancellation embed to finished-signals "
                f"for signal {sig_id} (no prior alert embed)"
            )
        except Exception as e:
            logger.warning(
                f"Could not send cancellation embed to finished-signals for signal {sig_id}: {e}"
            )

    async def _refresh_embed_after_reply(
        self, signal: SignalData, action_taken: str, author: discord.abc.User
    ) -> None:
        """Edit the persistent alert embed and send the notification ping."""
        if not self.alert_system:
            return

        event_map = {
            "cancelled": "cancelled",
            "marked as PROFIT": "profit",
            "marked as HIT": "hit",
            "marked as BREAKEVEN": "breakeven",
            "marked as STOP LOSS": "stop_loss",
            "reactivated": "reactivated",
        }
        embed_event = event_map.get(action_taken)
        if not embed_event:
            return

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
            f"{emoji} **{signal.instrument}** {signal.direction.upper()} — "
            f"manually {action_taken.lower()} (by {author.display_name})"
        )
        try:
            if embed_event == "reactivated":
                await self.alert_system.reactivate_embed(signal=signal, ping_text=ping_text)
            else:
                await self.alert_system.update_signal_message(
                    signal=signal, event=embed_event, ping_text=ping_text
                )
        except Exception as e:
            logger.warning(f"Could not update signal embed after manual command: {e}")

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

    async def _live_entry_price(self, instrument: str, direction: str) -> Optional[float]:
        """Current market price to enter at: the ask for a long, the bid for a
        short. Subscribes the symbol first if no signal was watching it yet.

        Returns None when no fresh price is available, which blocks the entry —
        an instant signal is only meaningful at a price we actually observed.
        """
        stream_manager = self.bot.services.stream_manager
        if stream_manager is None:
            return None

        price = await stream_manager.get_latest_price(instrument)
        if price is None:
            await stream_manager.bulk_subscribe([instrument])
            price = await stream_manager.get_latest_price(instrument)
        if price is None:
            return None

        updated_at = price.get("updated_at")
        if updated_at is not None and updated_at.tzinfo is not None:
            age = (datetime.now(pytz.UTC) - updated_at).total_seconds()
            if age > _INSTANT_ENTRY_MAX_PRICE_AGE:
                self.logger.warning(
                    f"Instant entry for {instrument} rejected: price is {age:.0f}s old"
                )
                return None

        return price["ask"] if direction == "long" else price["bid"]

    async def _market_entry_price(
        self, instrument: str, direction: str, stop_loss: float, take_profit: float
    ) -> tuple[Optional[float], Optional[str]]:
        """Market price to enter an instant signal at, or (None, reason).

        An entry is refused when no live price is available, or when price
        already sits past the stated stop loss or take profit — that trade would
        open only to close on the next tick.
        """
        entry = await self._live_entry_price(instrument, direction)
        if entry is None:
            return None, f"No live price for **{instrument}**"

        low, high = sorted((stop_loss, take_profit))
        if not low < entry < high:
            return None, (
                f"**{instrument}** is at {format_price(entry, instrument)}, already past "
                f"the stop loss or take profit"
            )

        return entry, None

    async def _resolve_instant_entry(self, message: discord.Message, parsed) -> Optional[float]:
        """Entry price for a new instant-entry signal, or None when it can't be
        taken — in which case the message is rejected with ⚠️ plus a reply."""
        entry, reason = await self._market_entry_price(
            parsed.instrument, parsed.direction, parsed.stop_loss, parsed.take_profit
        )
        if entry is None:
            await self.safe_add_reaction(message, "⚠️")
            await message.reply(
                f"⚠️ {reason} — signal not opened.",
                delete_after=_ERROR_REPLY_DELETE_AFTER,
            )
            self.logger.info(f"Instant signal rejected for message {message.id}: {reason}")
            return None

        return entry

    async def _open_instant_position(self, signal_id: int, entry_price: float) -> None:
        """Fill an instant signal's single limit at the market and open its embed
        as HIT — these signals have no waiting phase to alert on."""
        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal or not signal.limits:
            self.logger.error(f"Instant signal {signal_id} has no entry limit to fill")
            return

        # save_signal writes the entry already filled, and a reactivated signal comes
        # back that way too; filling it again would double-count limits_hit. The mark
        # remains for a signal whose limit somehow arrived pending.
        if signal.limits[0].status == "pending":
            await db.mark_limit_hit(signal.limits[0].id, entry_price)

        monitor = self.bot.services.monitor
        if monitor:
            await monitor.refresh_signal_in_memory(signal_id)
            signal = monitor.active_signals.get(signal_id, signal)
        if signal.guild_id is None and self.bot.guilds:
            signal.guild_id = self.bot.guilds[0].id

        alert_system = self.alert_system or self.bot.services.alert_system
        if alert_system:
            await alert_system.send_limit_hit_alert(signal, signal.limits[0], entry_price)

        self.logger.info(
            f"Instant signal {signal_id} opened at {entry_price} "
            f"({signal.instrument} {signal.direction})"
        )

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

            if parsed and parsed.instant_entry:
                entry_price = await self._resolve_instant_entry(message, parsed)
                if entry_price is None:
                    return
                parsed.limits = [entry_price]

            if parsed:
                context = self._build_save_context(parsed)
                success, signal_id = await self.signal_db.save_signal(
                    parsed, str(message.id), str(message.channel.id), context=context
                )

                if success:
                    await self.safe_add_reaction(message, "✅")
                    self.logger.info(
                        f"Signal {signal_id} saved: {parsed.instrument} {parsed.direction}, "
                        f"{len(parsed.limits)} limit(s) ({channel_name})"
                    )

                    if parsed.instant_entry and signal_id:
                        # Overlap detection is skipped: this signal's only limit
                        # fills immediately, so it never competes for a fill with
                        # the pending limits of another signal.
                        await self._open_instant_position(signal_id, parsed.limits[0])
                    elif parsed.limits and signal_id:
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
                msg_id = str(sig.message_id)
                ch_id = sig.channel_id
                label = f"Signal #{sig.signal_id} ({sig.instrument} {sig.direction.upper()})"
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
                    old_id = sig.signal_id
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

                    with contextlib.suppress(Exception):
                        await react_to_original_signal(self.bot, sig, "❌")

                    if alert_system:
                        try:
                            ping = (
                                f"❌ **{sig.instrument}** {sig.direction.upper()} — "
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

            with contextlib.suppress(Exception):
                await prompt_msg.delete()

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

        self.logger.debug(f"Message edited in monitored channel: {message.channel.name}")

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
                f"Signal {existing['id']} edit rejected as malformed: {parsed.reason}"
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
                    self.logger.info(f"Signal {existing['id']} reactivated after edit")

                    if self.alert_system:
                        try:
                            updated_signal = await self.signal_db.get_signal_with_limits(
                                existing["id"]
                            )
                            if updated_signal:
                                ping_text = (
                                    f"♻️ **{updated_signal.instrument}** {updated_signal.direction.upper()} — "
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
                self.logger.info(
                    f"Signal {existing['id']} updated after edit: "
                    f"{parsed.instrument} {parsed.direction}, {len(parsed.limits)} limit(s)"
                )

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
                            self.logger.debug(
                                f"Retracted stale alert embed after corrective edit: {message.id}"
                            )
                        else:
                            updated_signal = await self.signal_db.get_signal_with_limits(
                                existing["id"]
                            )
                            if updated_signal:
                                ping_text = (
                                    f"📝 **{updated_signal.instrument}** {updated_signal.direction.upper()} — "
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
                self.logger.info(
                    f"Signal {existing['id']} not updated — already {existing['status']}"
                )
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
            self.logger.info(f"Signal {existing['id']} edit did not parse")

    async def handle_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle message deletions with signal cancellation"""
        if not self.is_allowed_channel(payload.channel_id):
            return

        if payload.channel_id not in self.bot.monitored_channels:
            return

        self.logger.debug(f"Message deleted in monitored channel: {payload.message_id}")
        result = await self.signal_db.cancel_signal_by_message(str(payload.message_id))

        if result:
            self.logger.info(f"Signal cancelled — signal message {payload.message_id} deleted")
            monitor = self.bot.services.monitor
            if monitor:
                try:
                    deleted_signal = await self.signal_db.get_signal_by_message_id(
                        str(payload.message_id)
                    )
                    if deleted_signal:
                        await monitor.finalize_trailing_on_manual_close(deleted_signal["id"])
                except Exception as _fe:
                    self.logger.warning(
                        f"Could not finalize trackers after message delete cancel: {_fe}"
                    )
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
