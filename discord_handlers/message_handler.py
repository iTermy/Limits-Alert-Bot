"""
Message Handler - Debug version with extra logging for alert reply feature
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
        # We'll need access to the alert system to check alert messages
        self.alert_system = None  # Will be set by monitor when initialized
        logger.info("MessageHandler initialized, alert_system is None initially")

    async def handle_new_message(self, message: discord.Message):
        """
        Process new messages for signals and reply functionality

        Args:
            message: The Discord message to process
        """
        # Ignore bot's own messages
        if message.author.bot:
            return

        # Debug logging for replies
        if message.reference:
            logger.debug(f"Message is a reply from {message.author.name}: '{message.content}'")
            logger.debug(f"Alert system available: {self.alert_system is not None}")
            if self.alert_system:
                logger.debug(f"Tracked alert messages: {len(self.alert_system.alert_messages)}")

        # Check if message is in monitored channel
        if message.channel.id in self.bot.monitored_channels:
            self.logger.info(f"New message in monitored channel: {message.channel.name}")
            await self.process_signal(message)

        # Check for reply-to-signal management
        await self.check_signal_management_reply(message)

        # Check for reply-to-alert management
        await self.check_alert_management_reply(message)

    async def check_alert_management_reply(self, message: discord.Message):
        """
        Check if message is a reply to an alert message to manage a signal

        Args:
            message: The message to check
        """
        logger.debug(f"check_alert_management_reply called for message from {message.author.name}")

        if not message.reference:
            logger.debug("Not a reply, skipping")
            return

        if message.author.bot:
            logger.debug("Author is bot, skipping")
            return

        # Check if we have access to the alert system
        if not self.alert_system:
            logger.debug("Alert system not set on message handler, checking bot.monitor")
            # The connection should have been made in bot.setup_hook
            # But double-check here as a fallback
            if hasattr(self.bot, 'monitor') and self.bot.monitor and self.bot.monitor.alert_system:
                self.alert_system = self.bot.monitor.alert_system
                logger.info(f"Got alert system from bot.monitor, has {len(self.alert_system.alert_messages)} tracked messages")
            else:
                logger.warning("Alert system not available - monitor may not be initialized")
                logger.warning(f"bot.monitor exists: {hasattr(self.bot, 'monitor')}")
                logger.warning(f"bot.monitor value: {self.bot.monitor if hasattr(self.bot, 'monitor') else 'N/A'}")
                return

        try:
            # Get the referenced message
            referenced = await message.channel.fetch_message(message.reference.message_id)
            logger.debug(f"Referenced message ID: {referenced.id}, Author: {referenced.author.name}")

            # Check if this is an alert message
            signal_id = self.alert_system.get_signal_from_alert(str(referenced.id))
            logger.debug(f"Signal ID from alert lookup: {signal_id}")

            if not signal_id:
                # Not an alert message, check if it's from the bot (could be untracked alert)
                if referenced.author.id == self.bot.user.id:
                    logger.debug("Referenced message is from bot but not tracked as alert")
                    # Check if it looks like an alert by embed title
                    if referenced.embeds:
                        embed = referenced.embeds[0]
                        if any(keyword in embed.title.lower() for keyword in ['approaching', 'hit', 'stop loss']):
                            logger.warning(f"Message looks like alert but isn't tracked: {referenced.id}")
                            await message.reply("❌ This alert is not tracked. It may have been sent before the bot restarted.")
                            return
                else:
                    logger.debug("Referenced message is not from bot, not an alert")
                return

            logger.info(f"Processing alert management command for signal {signal_id}: '{message.content}'")

            # Parse the command
            command = message.content.lower().strip()

            # Get the signal from database
            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.warning(f"No signal found with ID {signal_id}")
                await message.reply("❌ Signal not found.")
                return

            logger.debug(f"Found signal: {signal['instrument']} {signal['direction']}, status: {signal['status']}")

            # Note: Anyone can manage signals via alert replies (not just the author)

            success = False
            action_taken = None

            # Import asyncio for timeouts
            import asyncio

            # Process different commands with timeout protection
            try:
                if command in ("cancel", "nm", "cancelled"):
                    logger.debug(f"Processing cancel command for signal {signal_id}")
                    # Use the signal ID directly since we have it
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'cancelled', f"Cancelled via alert reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "cancelled"
                    logger.debug(f"Cancel result: {success}")

                elif command in ("profit", "win", "tp", "hit"):
                    logger.debug(f"Processing profit command for signal {signal_id}")
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'profit', f"Set via alert reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as PROFIT"

                elif command in ("breakeven", "be"):
                    logger.debug(f"Processing breakeven command for signal {signal_id}")
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'breakeven', f"Set via alert reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as BREAKEVEN"

                elif command in ("sl", "stop", "stoploss", "stop loss"):
                    logger.debug(f"Processing stop loss command for signal {signal_id}")
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'stop_loss', f"Set via alert reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as STOP LOSS"

                elif command in ("reactivate", "reopen", "active"):
                    logger.debug(f"Processing reactivate command for signal {signal_id}")
                    # Only allow if signal was cancelled
                    if signal['status'] == 'cancelled':
                        # Try to get the original message to re-parse
                        if signal.get('message_id') and signal.get('channel_id'):
                            try:
                                original_channel = self.bot.get_channel(int(signal['channel_id']))
                                if original_channel:
                                    original_message = await original_channel.fetch_message(int(signal['message_id']))

                                    from core.parser import parse_signal
                                    channel_name = self.get_channel_name(int(signal['channel_id']))
                                    parsed = parse_signal(original_message.content, channel_name)

                                    if parsed:
                                        success = await asyncio.wait_for(
                                            self.signal_db.reactivate_cancelled_signal(signal_id, parsed),
                                            timeout=5.0
                                        )
                                        action_taken = "reactivated"
                            except Exception as e:
                                logger.error(f"Error getting original message: {e}")
                                await message.reply("❌ Cannot reactivate - original signal message not found.")
                                return
                    else:
                        await message.reply(f"❌ Signal is not cancelled (current status: {signal['status']})")
                        return

                else:
                    # Unknown command
                    logger.debug(f"Unknown command: '{command}'")
                    await message.reply(
                        "❓ Unknown command. Valid commands: `cancel`, `profit`, `tp`, `breakeven`, `be`, `sl`, `stop`, `reactivate`"
                    )
                    return

            except asyncio.TimeoutError:
                logger.error(f"Operation timed out for command: {command}")
                await message.reply(f"❌ {command.title()} operation timed out. Please try again.")
                return
            except Exception as e:
                logger.error(f"Error processing command '{command}': {e}", exc_info=True)
                await message.reply(f"❌ Error processing {command} command.")
                return

            if success and action_taken:
                logger.info(f"Successfully processed command, sending confirmation")
                # React to the command message
                await message.add_reaction("👍")

                # Send confirmation
                embed = discord.Embed(
                    title="✅ Signal Updated",
                    description=f"Signal #{signal_id} {action_taken}",
                    color=0x00FF00,
                    timestamp=discord.utils.utcnow()
                )
                embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
                embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
                embed.add_field(name="Updated By", value=message.author.mention, inline=True)
                embed.set_footer(text=f"Via alert reply")

                await message.channel.send(embed=embed)

                logger.info(f"Signal {signal_id} {action_taken} via alert reply by {message.author.name}")
            else:
                await message.reply(f"❌ Failed to process command.")
                logger.warning(f"Failed to process command '{command}' for signal {signal_id}")

        except Exception as e:
            logger.error(f"Error in alert management reply: {e}", exc_info=True)
            await message.reply("❌ An error occurred processing your command.")

    async def check_signal_management_reply(self, message: discord.Message):
        """
        Check if message is a reply to manage a signal (cancel, profit, breakeven, etc.)
        This handles replies to original signal messages (not alerts)

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

    # ... rest of the methods remain the same ...

    async def process_signal(self, message: discord.Message):
        """Process a potential trading signal with enhanced parsing"""
        try:
            from core.parser import parse_signal
            channel_name = self.get_channel_name(message.channel.id)
            parsed = parse_signal(message.content, channel_name)

            if parsed:
                success, signal_id = await self.signal_db.save_signal(
                    parsed,
                    str(message.id),
                    str(message.channel.id)
                )

                if success:
                    await message.add_reaction("✅")
                    self.logger.info(f"Signal #{signal_id} processed: {parsed.instrument} {parsed.direction}")
                else:
                    existing = await self.signal_db.get_signal_by_message_id(str(message.id))
                    if existing and existing['status'] != 'cancelled':
                        await message.add_reaction("⚠️")
                    else:
                        await message.add_reaction("♻️")
            else:
                if self.looks_like_signal(message.content):
                    await message.add_reaction("⚠️")
                    self.logger.debug(f"Failed to parse apparent signal from message {message.id}")

        except Exception as e:
            self.logger.error(f"Error processing signal: {e}", exc_info=True)
            await message.add_reaction("⚠️️")

    async def handle_message_edit(self, before: discord.Message, after: discord.Message):
        """Handle message edits with signal re-parsing"""
        if after.author.bot:
            return

        if after.channel.id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message edited in monitored channel: {after.channel.name}")

        existing = await self.signal_db.get_signal_by_message_id(str(after.id))
        if not existing:
            await self.process_signal(after)
            return

        from core.parser import parse_signal
        channel_name = self.get_channel_name(after.channel.id)
        parsed = parse_signal(after.content, channel_name)

        if parsed:
            success = await self.signal_db.update_signal_from_edit(str(after.id), parsed)

            if success:
                await after.clear_reactions()
                await after.add_reaction("✅")
                await after.add_reaction("📝")
                self.logger.info(f"Signal updated after edit: {after.id}")
            else:
                if existing['status'] in ['profit', 'breakeven', 'stop_loss']:
                    await after.add_reaction("🔒")
                    self.logger.info(f"Cannot update signal in final status: {existing['status']}")
        else:
            await after.clear_reactions()
            await after.add_reaction("❌")
            self.logger.info(f"Signal parse failed after edit: {after.id}")

    async def handle_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle message deletions with signal cancellation"""
        if payload.channel_id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message deleted in monitored channel: {payload.message_id}")
        success = await self.signal_db.cancel_signal_by_message(str(payload.message_id))

        if success:
            self.logger.info(f"Signal cancelled due to message deletion: {payload.message_id}")

    def looks_like_signal(self, text: str) -> bool:
        """Check if text appears to be a trading signal"""
        text = re.sub(r"<@&\d+>.*", "", text).strip().lower()
        has_numbers = bool(re.search(r'\d+\.?\d*', text))
        keywords = ['stop', 'sl', 'long', 'short', 'buy', 'sell', 'entry']
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
        """Get channel name from configuration"""
        for name, ch_id in self.bot.channels_config.get("monitored_channels", {}).items():
            if int(ch_id) == channel_id:
                return name
        return None