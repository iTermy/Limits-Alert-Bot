"""
Message Handler - Handles Discord message events for signal processing
Fixed to work with new enhanced database structure
"""
import re
import discord
from typing import Optional
from utils.embed_factory import EmbedFactory
from utils.logger import get_logger

logger = get_logger("message_handler")

class MessageHandler:
    """Handles all message-related events for signal processing"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.signal_db = bot.signal_db

    async def handle_new_message(self, message: discord.Message):
        """
        Process new messages for signals and reply-to-cancel functionality

        Args:
            message: The Discord message to process
        """
        # Ignore bot's own messages
        if message.author.bot:
            return

        # Check if message is in monitored channel
        if message.channel.id in self.bot.monitored_channels:
            self.logger.info(f"New message in monitored channel: {message.channel.name}")
            await self.process_signal(message)

        # Check for reply-to-cancel or reply-to-manage
        await self.check_signal_management_reply(message)

    async def process_signal(self, message: discord.Message):
        """
        Process a potential trading signal with enhanced parsing

        Args:
            message: Discord message to process
        """
        try:
            # Import parse_signal here to avoid circular imports
            from core.parser import parse_signal

            # Find channel name from configuration
            channel_name = self.get_channel_name(message.channel.id)

            # Parse the signal with channel awareness
            parsed = parse_signal(message.content, channel_name)

            if parsed:
                # Save to database
                success, signal_id = await self.signal_db.save_signal(
                    parsed,
                    str(message.id),
                    str(message.channel.id)
                )

                if success:
                    # Add success reaction
                    await message.add_reaction("✅")
                    self.logger.info(f"Signal #{signal_id} processed: {parsed.instrument} {parsed.direction}")
                else:
                    # Signal might already exist or be reactivated
                    existing = await self.signal_db.get_signal_by_message_id(str(message.id))
                    if existing and existing['status'] != 'cancelled':
                        logger.debug('⚠️')
                        await message.add_reaction("⚠️")
                    else:
                        # Reactivated cancelled signal
                        await message.add_reaction("♻️")
            else:
                # Only react with ❌ if we detected it might be a signal but failed to parse
                if self.looks_like_signal(message.content):
                    await message.add_reaction("⚠️")
                    self.logger.debug(f"Failed to parse apparent signal from message {message.id}")
                else:
                    self.logger.debug(f"Message doesn't appear to be a signal: {message.id}")

        except Exception as e:
            self.logger.error(f"Error processing signal: {e}", exc_info=True)
            await message.add_reaction("⚠️️")

    async def check_signal_management_reply(self, message: discord.Message):
        """
        Check if message is a reply to manage a signal (cancel, profit, breakeven, etc.)

        Args:
            message: The message to check
        """
        if not message.reference or message.author.bot:
            return

        try:
            # Get the referenced message
            referenced = await message.channel.fetch_message(message.reference.message_id)

            # Check if the referenced message has a ✅ reaction from the bot
            has_bot_reaction = await self.has_bot_success_reaction(referenced)

            if not has_bot_reaction:
                self.logger.debug(f"Referenced message {referenced.id} doesn't have bot success reaction")
                return

            # Parse the command
            command = message.content.lower().strip()
            self.logger.info(f"Processing signal management command: '{command}' for message {referenced.id}")

            # Get the signal from database
            signal = await self.signal_db.get_signal_by_message_id(str(referenced.id))
            if not signal:
                self.logger.warning(f"No signal found for message {referenced.id}")
                return

            # Check if user is authorized (signal author or admin)
            is_author = message.author.id == referenced.author.id
            is_admin = message.author.guild_permissions.administrator if hasattr(message.author, 'guild_permissions') else False

            if not (is_author or is_admin):
                await message.reply("Only the signal sender or admins can manage this signal.")
                return

            success = False
            action_taken = None

            # Import asyncio for timeouts
            import asyncio

            # Process different commands with timeout protection
            try:
                if command in ("cancel", "nm", "cancelled"):
                    # For cancel, we need to use the cancel_signal_by_message method
                    success = await asyncio.wait_for(
                        self.signal_db.cancel_signal_by_message(str(referenced.id)),
                        timeout=5.0
                    )
                    action_taken = "cancelled"
                    self.logger.info(f"Cancel command result: {success}")

                elif command in ("profit", "win", "tp"):
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal['id'], 'profit', f"Set by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as PROFIT"

                elif command in ("breakeven", "be"):
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal['id'], 'breakeven', f"Set by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as BREAKEVEN"

                elif command in ("sl", "stop", "stoploss", "stop loss"):
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal['id'], 'stop_loss', f"Set by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as STOP LOSS"

                elif command in ("reactivate", "reopen", "active"):
                    # Only allow if signal was cancelled
                    if signal['status'] == 'cancelled':
                        # Need to re-parse to get the signal data
                        from core.parser import parse_signal
                        channel_name = self.get_channel_name(referenced.channel.id)
                        parsed = parse_signal(referenced.content, channel_name)
                        if parsed:
                            success = await asyncio.wait_for(
                                self.signal_db.reactivate_cancelled_signal(signal['id'], parsed),
                                timeout=5.0
                            )
                            action_taken = "reactivated"

            except asyncio.TimeoutError:
                self.logger.error(f"Operation timed out for command: {command}")
                await message.reply(f"❌ {command.title()} operation timed out. Please try again.")
                return
            except Exception as e:
                self.logger.error(f"Error processing command '{command}': {e}", exc_info=True)
                await message.reply(f"❌ Error processing {command} command.")
                return

            if success and action_taken:
                # Update reactions on original message
                if action_taken == "cancelled":
                    try:
                        await referenced.remove_reaction("✅", self.bot.user)
                    except:
                        pass  # Reaction might not exist
                    await referenced.add_reaction("❌")
                elif action_taken == "marked as PROFIT":
                    await referenced.add_reaction("💰")
                elif action_taken == "marked as BREAKEVEN":
                    await referenced.add_reaction("➖")
                elif action_taken == "marked as STOP LOSS":
                    await referenced.add_reaction("🛑")
                elif action_taken == "reactivated":
                    try:
                        await referenced.remove_reaction("❌", self.bot.user)
                    except:
                        pass
                    await referenced.add_reaction("✅")
                    await referenced.add_reaction("♻️")

                # React to the command message
                await message.add_reaction("👍")
                self.logger.info(f"Signal {signal['id']} {action_taken} by {message.author.name}")
            else:
                self.logger.warning(f"Failed to process command '{command}' for signal {signal['id']}")

        except Exception as e:
            self.logger.error(f"Error in signal management reply: {e}", exc_info=True)

    async def handle_message_edit(self, before: discord.Message, after: discord.Message):
        """
        Handle message edits with signal re-parsing

        Args:
            before: Message before edit
            after: Message after edit
        """
        if after.author.bot:
            return

        if after.channel.id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message edited in monitored channel: {after.channel.name}")

        # Check if this message has a signal
        existing = await self.signal_db.get_signal_by_message_id(str(after.id))
        if not existing:
            # Try to process as new signal
            await self.process_signal(after)
            return

        # Import parse_signal here
        from core.parser import parse_signal

        # Find channel name
        channel_name = self.get_channel_name(after.channel.id)

        # Re-parse the signal
        parsed = parse_signal(after.content, channel_name)

        if parsed:
            # Update in database
            success = await self.signal_db.update_signal_from_edit(str(after.id), parsed)

            if success:
                # Update reactions
                await after.clear_reactions()
                await after.add_reaction("✅")
                await after.add_reaction("📝")  # Edited indicator
                self.logger.info(f"Signal updated after edit: {after.id}")
            else:
                # Might be in final status
                if existing['status'] in ['profit', 'breakeven', 'stop_loss']:
                    await after.add_reaction("🔒")  # Locked indicator
                    self.logger.info(f"Cannot update signal in final status: {existing['status']}")
        else:
            # Parsing failed after edit
            await after.clear_reactions()
            await after.add_reaction("❌")
            self.logger.info(f"Signal parse failed after edit: {after.id}")

    async def handle_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """
        Handle message deletions with signal cancellation

        Args:
            payload: Raw message delete event payload
        """
        if payload.channel_id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message deleted in monitored channel: {payload.message_id}")

        # Cancel the signal
        success = await self.signal_db.cancel_signal_by_message(str(payload.message_id))

        if success:
            self.logger.info(f"Signal cancelled due to message deletion: {payload.message_id}")

    def looks_like_signal(self, text: str) -> bool:
        """
        Check if text appears to be a trading signal

        Args:
            text: The text to check

        Returns:
            bool: True if text looks like a signal
        """
        # Clean text by removing discord role
        text = re.sub(r"<@&\d+>.*", "", text).strip().lower()

        # Check if it has numbers
        has_numbers = bool(re.search(r'\d+\.?\d*', text))
        if has_numbers:
            logger.info(f'looks_like_signal found numbers')

        # Check if it has trading keywords
        keywords = ['stop', 'sl', 'long', 'short', 'buy', 'sell', 'entry']
        has_keywords = any(word in text for word in keywords)
        if has_keywords:
            logger.info(f'looks_like_signal found keywords')

        return has_numbers and has_keywords

    async def has_bot_success_reaction(self, message: discord.Message) -> bool:
        """
        Check if message has a ✅ reaction from the bot

        Args:
            message: The message to check

        Returns:
            bool: True if bot has added ✅ reaction
        """
        for reaction in message.reactions:
            if str(reaction.emoji) == "✅":
                async for user in reaction.users():
                    if user.id == self.bot.user.id:
                        return True
        return False

    def get_channel_name(self, channel_id: int) -> Optional[str]:
        """
        Get channel name from configuration

        Args:
            channel_id: The Discord channel ID

        Returns:
            str: Channel name or None
        """
        for name, ch_id in self.bot.channels_config.get("monitored_channels", {}).items():
            if int(ch_id) == channel_id:
                return name
        return None