"""
Message Handler
"""
import re
import discord
from typing import Optional
from utils.embed_factory import EmbedFactory
from utils.logger import get_logger
from price_feeds.tp_config import TPConfig

logger = get_logger("message_handler")

class MessageHandler:
    """Handles all message-related events for signal processing"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.signal_db = bot.signal_db
        self.tp_config = TPConfig()
        # We'll need access to the alert system to check alert messages
        self.alert_system = None  # Will be set by monitor when initialized
        logger.info("MessageHandler initialized, alert_system is None initially")

        # Cache allowed channels for quick lookup
        self._allowed_channels = None

    def _get_allowed_channels(self):
        """Get set of allowed channel IDs (monitored + alert + command channels)"""
        if self._allowed_channels is None:
            self._allowed_channels = set()

            # Add monitored channels
            for channel_id in self.bot.monitored_channels:
                self._allowed_channels.add(channel_id)

            # Add alert channel
            if hasattr(self.bot, 'alert_channel_id') and self.bot.alert_channel_id:
                self._allowed_channels.add(self.bot.alert_channel_id)

            # Add command channel
            if hasattr(self.bot, 'command_channel_id') and self.bot.command_channel_id:
                self._allowed_channels.add(self.bot.command_channel_id)

            # Try to get from channels_config if not set
            if hasattr(self.bot, 'channels_config'):
                if 'alert_channel' in self.bot.channels_config:
                    self._allowed_channels.add(int(self.bot.channels_config['alert_channel']))
                if 'command_channel' in self.bot.channels_config:
                    self._allowed_channels.add(int(self.bot.channels_config['command_channel']))
                if 'pa-alert-channel' in self.bot.channels_config:
                    self._allowed_channels.add(int(self.bot.channels_config['pa-alert-channel']))
                if 'toll-alert-channel' in self.bot.channels_config:
                    self._allowed_channels.add(int(self.bot.channels_config['toll-alert-channel']))
                if 'general-tolls-alert' in self.bot.channels_config and self.bot.channels_config['general-tolls-alert']:
                    self._allowed_channels.add(int(self.bot.channels_config['general-tolls-alert']))
                if 'finished_signals' in self.bot.channels_config and self.bot.channels_config['finished_signals']:
                    self._allowed_channels.add(int(self.bot.channels_config['finished_signals']))
                if 'profit_channel' in self.bot.channels_config and self.bot.channels_config['profit_channel']:
                    self._allowed_channels.add(int(self.bot.channels_config['profit_channel']))

        return self._allowed_channels

    def is_allowed_channel(self, channel_id: int) -> bool:
        """Check if bot should process messages in this channel"""
        return channel_id in self._get_allowed_channels()

    async def handle_new_message(self, message: discord.Message):
        """
        Process new messages for signals and reply functionality

        Args:
            message: The Discord message to process
        """
        # Ignore bot's own messages
        if message.author.bot:
            return

        if not self.is_allowed_channel(message.channel.id):
            # Silently ignore messages in non-trading channels
            return

        # Check if message is in monitored channel
        if message.channel.id in self.bot.monitored_channels:
            self.logger.info(f"New message in monitored channel: {message.channel.name}")
            await self.process_signal(message)

        # Check for reply-to-signal management
        await self.check_signal_management_reply(message)

        # Check for reply-to-alert management
        await self.check_alert_management_reply(message)

    async def _react_to_original_signal(self, signal: dict, action_taken: str):
        """
        Add a reaction to the original signal message based on the action taken

        Args:
            signal: Signal dictionary containing message_id and channel_id
            action_taken: The action that was performed (e.g., "cancelled", "marked as PROFIT")
        """
        try:
            # Get the original message ID and channel ID
            message_id = signal.get('message_id')
            channel_id = signal.get('channel_id')

            # Skip if this is a manual signal or missing info
            if not message_id or not channel_id or str(message_id).startswith('manual_'):
                self.logger.debug(f"Skipping original message reaction - manual signal or missing IDs")
                return

            # Fetch the original signal message
            try:
                channel = self.bot.get_channel(int(channel_id))
                if not channel:
                    # Try fetching the channel
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

            # Add the appropriate reaction based on action
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
                # Remove the X and add check and recycle
                try:
                    await original_message.remove_reaction("❌", self.bot.user)
                except:
                    pass
                await self.safe_add_reaction(original_message, "♻️")

            self.logger.info(f"Added reaction to original signal message {message_id} for action: {action_taken}")

        except Exception as e:
            # Don't fail the whole operation if reaction fails
            self.logger.error(f"Error adding reaction to original signal: {e}", exc_info=True)

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
                logger.info(
                    f"Got alert system from bot.monitor, has {len(self.alert_system.alert_messages)} tracked messages")
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
                            await message.reply(
                                "❌ This alert is not tracked. It may have been sent before the bot restarted.")
                            return
                else:
                    logger.debug("Referenced message is not from bot, not an alert")
                return

            logger.info(f"Processing alert management command for signal {signal_id}: '{message.content}'")

            # Parse the command
            command_parts = message.content.lower().strip().split()
            command = command_parts[0] if command_parts else ""

            # Parse optional profit amount (for profit commands)
            profit_amount = None
            if len(command_parts) > 1 and command in ("profit", "win", "tp", "hit"):
                try:
                    profit_amount = float(command_parts[1])
                    logger.debug(f"Parsed profit amount: {profit_amount}")
                except (ValueError, IndexError):
                    logger.debug(f"Could not parse profit amount from: {command_parts[1:]}")

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


                elif command in ("profit", "win", "tp"):
                    logger.debug(f"Processing profit command for signal {signal_id}")
                    # If signal has no limits hit yet (approaching→profit), mark limit 1 as hit first
                    if not signal.get('hit_limits'):
                        pending = sorted(
                            signal.get('pending_limits') or [],
                            key=lambda l: l.get('sequence_number', 999)
                        )
                        if pending:
                            try:
                                from database import db as _db
                                await _db.mark_limit_hit(pending[0]['id'], pending[0]['price_level'])
                                # Refresh signal so result_pips calc and embed see the hit limit
                                signal = await self.signal_db.get_signal_with_limits(signal_id) or signal
                            except Exception as _he:
                                logger.warning(f"Could not auto-hit limit for signal {signal_id} on profit reply: {_he}")
                    # Use TP threshold from config as the recorded result
                    profit_result_pips = self.tp_config.get_tp_value(signal['instrument'], scalp=signal.get('scalp', False))
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'profit', f"Set via alert reply by {message.author.name}",
                            result_pips=profit_result_pips,
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as PROFIT"
                elif command in ("hit",):
                    logger.debug(f"Processing hit command for signal {signal_id}")
                    was_cancelled = signal.get('status') == 'cancelled'
                    transitioned = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_to_hit(
                            signal_id, f"Set via alert reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    if transitioned:
                        # Populate TP cache immediately so auto-TP starts on the next tick
                        if hasattr(self.bot, 'monitor') and self.bot.monitor:
                            monitor = self.bot.monitor
                            await monitor.tp_monitor.refresh_hit_limits(signal_id)
                            if signal_id in monitor.active_signals:
                                monitor.active_signals[signal_id]['status'] = 'hit'
                            elif was_cancelled:
                                # Signal was cancelled and not in monitor — re-add it now
                                # so price tracking and auto-TP resume immediately
                                reloaded = await self.signal_db.get_signal_with_limits(signal_id)
                                if reloaded:
                                    reloaded_for_monitor = dict(reloaded)
                                    reloaded_for_monitor['signal_id'] = signal_id
                                    reloaded_for_monitor['status'] = 'hit'
                                    monitor.active_signals[signal_id] = reloaded_for_monitor
                                    symbol = signal.get('instrument')
                                    if symbol:
                                        monitor.symbol_to_signals.setdefault(symbol, [])
                                        if signal_id not in monitor.symbol_to_signals[symbol]:
                                            monitor.symbol_to_signals[symbol].append(signal_id)
                                        await monitor.stream_manager.bulk_subscribe([symbol])
                        success = True
                        action_taken = "marked as HIT"
                    else:
                        # Already HIT — nothing to do
                        success = False
                        action_taken = None

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
                    # Sum P&L of all hit limits at the stop loss price
                    sl_result_pips = None
                    try:
                        hit_limits = await self.signal_db.get_hit_limits_for_signal(signal_id)
                        stop_price = signal.get('stop_loss')
                        if hit_limits and stop_price:
                            combined = 0.0
                            for lim in hit_limits:
                                entry = lim.get('hit_price') or lim.get('price_level')
                                if entry is not None:
                                    combined += self.tp_config.calculate_pnl(
                                        signal['instrument'], signal['direction'], entry, stop_price,
                                        scalp=signal.get('scalp', False)
                                    )
                            sl_result_pips = combined
                    except Exception as e:
                        logger.warning(f"Could not calculate SL result_pips for signal {signal_id}: {e}")
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal_id, 'stop_loss', f"Set via alert reply by {message.author.name}",
                            result_pips=sl_result_pips,
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
                                if original_channel is None:
                                    original_channel = await self.bot.fetch_channel(int(signal['channel_id']))
                                original_message = await original_channel.fetch_message(int(signal['message_id']))

                                from core.parser import parse_signal
                                channel_name = self.get_channel_name(int(signal['channel_id']))
                                parsed = parse_signal(original_message.content, channel_name)

                                if parsed:
                                    success = await asyncio.wait_for(
                                        self.signal_db.reactivate_cancelled_signal(signal_id, parsed),
                                        timeout=5.0
                                    )
                                    if success:
                                        action_taken = "reactivated"
                                        # Mark NM-immune so the monitor can't auto-cancel again
                                        if (hasattr(self.bot, 'monitor') and self.bot.monitor and
                                                hasattr(self.bot.monitor, 'nm_monitor')):
                                            self.bot.monitor.nm_monitor.mark_immune(signal_id)
                                else:
                                    await message.reply("❌ Cannot reactivate - failed to parse original signal.")
                                    return
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
                        "❓ Unknown command. Valid commands: `cancel`, `profit`, `tp`, `breakeven`, `be`, `sl`, `stop`, `reactivate`\n"
                        "For profit, you can optionally specify pips: `profit 40`"
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

                # Update reactions on alert message (referenced message)
                if action_taken == "cancelled":
                    try:
                        await referenced.remove_reaction("✅", self.bot.user)
                    except:
                        pass  # Reaction might not exist
                    await referenced.add_reaction("❌")
                elif action_taken == "marked as HIT":
                    await referenced.add_reaction("🎯")
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

                # ALSO react to the original signal message
                await self._react_to_original_signal(signal, action_taken)

                # Delete the user's reply message to reduce clutter
                try:
                    await message.delete()
                except Exception:
                    pass

                # Update the persistent alert embed and send a ping saying who manually changed it
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
                            # get_signal_with_limits returns 'id', not 'signal_id' —
                            # normalise so update_signal_message can find the embed
                            _signal_for_update = dict(signal)
                            if 'signal_id' not in _signal_for_update:
                                _signal_for_update['signal_id'] = _signal_for_update.get('id', signal_id)
                            if embed_event == "reactivated":
                                # Rebuild embed with correct live state (approaching/hit) + current price
                                await self.alert_system.reactivate_embed(
                                    signal=_signal_for_update,
                                    ping_text=ping_text,
                                )
                            else:
                                await self.alert_system.update_signal_message(
                                    signal=_signal_for_update,
                                    event=embed_event,
                                    ping_text=ping_text,
                                )
                        except Exception as _ue:
                            logger.warning(f"Could not update signal embed after manual command: {_ue}")

                logger.info(f"Signal {signal_id} {action_taken} via alert reply by {message.author.name}")
            else:
                await message.reply(f"❌ Failed to process command.")
                logger.warning(f"Failed to process command '{command}' for signal {signal_id}")

        except Exception as e:
            logger.error(f"Error in alert management reply: {e}", exc_info=True)
            await message.reply("❌ An error occurred processing your command.")

    async def send_profit_alert(self, signal, user, profit_amount=None):
        """
        Send a profit alert to the profit channel

        Args:
            signal: The signal data from database
            user: The Discord user who marked it as profit
            profit_amount: Optional profit amount in pips/points
        """
        try:
            # Load channel configuration
            import json
            import os

            config_path = os.path.join('config', 'channels.json')
            with open(config_path, 'r') as f:
                channels_config = json.load(f)

            profit_channel_id = channels_config.get('profit_channel')
            if not profit_channel_id:
                logger.warning("No profit_channel configured in channels.json")
                return

            profit_channel = self.bot.get_channel(int(profit_channel_id))
            if not profit_channel:
                logger.error(f"Could not find profit channel with ID {profit_channel_id}")
                return

            # Create profit embed
            embed = discord.Embed(
                title="💰 PROFIT Alert",
                description=f"Signal #{signal['id']} has been marked as **PROFIT**",
                color=0x00FF00,  # Green
                timestamp=discord.utils.utcnow()
            )

            # Add signal details
            embed.add_field(name="Symbol", value=signal['instrument'], inline=True)
            embed.add_field(name="Position", value=signal['direction'].upper(), inline=True)

            # Add profit amount if specified
            if profit_amount:
                unit = self.get_pip_unit_name(signal['instrument'])
                embed.add_field(name="Profit", value=f"**{profit_amount:.1f} {unit}**", inline=True)
            else:
                embed.add_field(name="Status", value="✅ Profit", inline=True)

            # Add entry price if available
            if signal.get('entry_price'):
                embed.add_field(name="Entry", value=f"{signal['entry_price']}", inline=True)

            # Add limits if available
            if signal.get('limits'):
                limits_text = []
                for limit in signal['limits']:
                    if limit.get('status') == 'hit':
                        limits_text.append(f"~~{limit['price_level']}~~ ✅")
                    else:
                        limits_text.append(str(limit['price_level']))
                if limits_text:
                    embed.add_field(name="Limits", value="\n".join(limits_text[:3]), inline=True)  # Show max 3

            # Add stop loss if available
            if signal.get('stop_loss'):
                embed.add_field(name="Stop Loss", value=signal['stop_loss'], inline=True)

            # Add metadata
            embed.set_footer(text=f"Marked by {user.name}")

            # Add original message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                try:
                    original_channel = self.bot.get_channel(int(signal['channel_id']))
                    if original_channel:
                        message_link = f"https://discord.com/channels/{original_channel.guild.id}/{signal['channel_id']}/{signal['message_id']}"
                        embed.add_field(name="Original Signal", value=f"[View Message]({message_link})", inline=False)
                except Exception as e:
                    logger.debug(f"Could not create message link: {e}")

            # Send the profit alert
            await profit_channel.send(embed=embed)
            logger.info(f"Sent profit alert for signal {signal['id']} to profit channel")

        except Exception as e:
            logger.error(f"Error sending profit alert: {e}", exc_info=True)

    def get_pip_unit_name(self, instrument):
        """
        Get the appropriate unit name (pips/points) for an instrument

        Args:
            instrument: The trading instrument symbol

        Returns:
            str: "pips" or "points" depending on the instrument type
        """
        instrument_upper = instrument.upper()

        # Load alert config to determine asset type
        import json
        import os

        try:
            config_path = os.path.join('config', 'alert_distances.json')
            with open(config_path, 'r') as f:
                alert_config = json.load(f)

            # Check if it's in overrides first
            if instrument_upper in alert_config.get('overrides', {}):
                # Determine based on common patterns
                if 'USD' in instrument_upper and any(
                        x in instrument_upper for x in ['EUR', 'GBP', 'AUD', 'NZD', 'CAD', 'CHF', 'JPY']):
                    return "pips"
                elif any(x in instrument_upper for x in ['SPX', 'NAS', 'JP225', 'US30']):
                    return "points"
                elif any(x in instrument_upper for x in ['BTC', 'ETH', 'SOL']):
                    return "points"
                elif 'XAU' in instrument_upper or 'XAG' in instrument_upper:
                    return "pips"

            # Default logic based on instrument patterns
            if any(currency in instrument_upper for currency in
                   ['EUR', 'GBP', 'USD', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF']):
                # Check if it's a forex pair (has two currencies)
                forex_count = sum(
                    1 for curr in ['EUR', 'GBP', 'USD', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF'] if curr in instrument_upper)
                if forex_count >= 2 or 'XAU' in instrument_upper or 'XAG' in instrument_upper:
                    return "pips"

            # Default to points for indices, stocks, crypto
            return "points"

        except Exception as e:
            logger.debug(f"Error determining pip unit: {e}")
            # Safe default
            return "pips" if 'USD' in instrument_upper else "points"

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
                    # Use TP threshold from config as the recorded result
                    profit_result_pips = self.tp_config.get_tp_value(signal['instrument'], scalp=signal.get('scalp', False))
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal['id'], 'profit', f"Set by {message.author.name}",
                            result_pips=profit_result_pips,
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
                    # Sum P&L of all hit limits at the stop loss price
                    sl_result_pips = None
                    try:
                        hit_limits = await self.signal_db.get_hit_limits_for_signal(signal['id'])
                        stop_price = signal.get('stop_loss')
                        if hit_limits and stop_price:
                            combined = 0.0
                            for lim in hit_limits:
                                entry = lim.get('hit_price') or lim.get('price_level')
                                if entry is not None:
                                    combined += self.tp_config.calculate_pnl(
                                        signal['instrument'], signal['direction'], entry, stop_price,
                                        scalp=signal.get('scalp', False)
                                    )
                            sl_result_pips = combined
                    except Exception as e:
                        logger.warning(f"Could not calculate SL result_pips for signal {signal['id']}: {e}")
                    success = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_status(
                            signal['id'], 'stop_loss', f"Set by {message.author.name}",
                            result_pips=sl_result_pips,
                        ),
                        timeout=5.0
                    )
                    action_taken = "marked as STOP LOSS"

                elif command in ("hit",):
                    self.logger.debug(f"Processing hit command for signal {signal['id']} via signal reply")
                    was_cancelled = signal.get('status') == 'cancelled'
                    transitioned = await asyncio.wait_for(
                        self.signal_db.manually_set_signal_to_hit(
                            signal['id'], f"Set via signal reply by {message.author.name}"
                        ),
                        timeout=5.0
                    )
                    if transitioned:
                        if hasattr(self.bot, 'monitor') and self.bot.monitor:
                            monitor = self.bot.monitor
                            await monitor.tp_monitor.refresh_hit_limits(signal['id'])
                            if signal['id'] in monitor.active_signals:
                                monitor.active_signals[signal['id']]['status'] = 'hit'
                            elif was_cancelled:
                                reloaded = await self.signal_db.get_signal_with_limits(signal['id'])
                                if reloaded:
                                    reloaded_for_monitor = dict(reloaded)
                                    reloaded_for_monitor['signal_id'] = signal['id']
                                    reloaded_for_monitor['status'] = 'hit'
                                    monitor.active_signals[signal['id']] = reloaded_for_monitor
                                    sym = signal.get('instrument')
                                    if sym:
                                        monitor.symbol_to_signals.setdefault(sym, [])
                                        if signal['id'] not in monitor.symbol_to_signals[sym]:
                                            monitor.symbol_to_signals[sym].append(signal['id'])
                                        await monitor.stream_manager.bulk_subscribe([sym])
                        success = True
                        action_taken = "marked as HIT"
                    else:
                        success = False
                        action_taken = None

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

                    _sig_id_for_check = signal.get('signal_id') or signal.get('id')
                    has_alert_embed = (
                        self.alert_system and
                        _sig_id_for_check in self.alert_system.signal_messages
                    )
                    if not has_alert_embed:
                        # No approaching embed was ever sent — delete the original signal
                        # message immediately and send a cancellation embed straight to
                        # the finished-signals channel so there's still a record.
                        try:
                            await referenced.delete()
                            logger.info(
                                f"Deleted original signal message {referenced.id} "
                                f"(signal {_sig_id_for_check} cancelled with no alert embed)"
                            )
                        except Exception as _de:
                            logger.warning(f"Could not delete original signal message {referenced.id}: {_de}")

                        # Send cancellation embed directly to finished-signals channel.
                        if self.alert_system:
                            try:
                                finished_channel = self.alert_system._get_finished_channel()
                                if finished_channel:
                                    from price_feeds.alert_system import _build_signal_embed
                                    _sig_for_embed = dict(signal)
                                    if 'signal_id' not in _sig_for_embed:
                                        _sig_for_embed['signal_id'] = _sig_id_for_check

                                    guild_id_val = signal.get('guild_id')
                                    if not guild_id_val and self.bot and self.bot.guilds:
                                        guild_id_val = self.bot.guilds[0].id

                                    cancel_embed = _build_signal_embed(
                                        signal=_sig_for_embed,
                                        limits=signal.get('limits') or signal.get('pending_limits') or [],
                                        event='cancelled',
                                        guild_id=guild_id_val,
                                        bot=self.bot,
                                    )
                                    old_footer = cancel_embed.footer.text or ""
                                    clean_footer = old_footer.split(" • ⏳")[0].split(" • 🗑️")[0]
                                    cancel_embed.set_footer(text=f"{clean_footer} • 📁 Archived")

                                    role_mention = "<@&1334203997107650662>"
                                    ping_line = (
                                        f"{role_mention} ❌ **{signal['instrument']}** "
                                        f"{signal['direction'].upper()} — cancelled by sender "
                                        f"(by {message.author.display_name})"
                                    )
                                    await finished_channel.send(ping_line)
                                    await finished_channel.send(embed=cancel_embed)
                                    logger.info(
                                        f"Sent direct cancellation embed to finished-signals "
                                        f"for signal {_sig_id_for_check} (no prior alert embed)"
                                    )
                            except Exception as _fe:
                                logger.warning(
                                    f"Could not send cancellation embed to finished-signals "
                                    f"for signal {_sig_id_for_check}: {_fe}"
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
                    try:
                        await referenced.remove_reaction("❌", self.bot.user)
                    except:
                        pass
                    await referenced.add_reaction("✅")
                    await referenced.add_reaction("♻️")

                # Delete the user's reply message to reduce clutter
                try:
                    await message.delete()
                except Exception:
                    pass

                # Update the persistent alert embed and send a ping saying who manually changed it
                if self.alert_system:
                    _sig_id = signal.get('signal_id') or signal.get('id')
                    _signal_for_update = dict(signal)
                    _signal_for_update['signal_id'] = _sig_id
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
                                # Rebuild embed with correct live state (approaching/hit) + current price
                                await self.alert_system.reactivate_embed(
                                    signal=_signal_for_update,
                                    ping_text=ping_text,
                                )
                            else:
                                await self.alert_system.update_signal_message(
                                    signal=_signal_for_update,
                                    event=embed_event,
                                    ping_text=ping_text,
                                )
                        except Exception as _ue:
                            logger.warning(f"Could not update signal embed after manual command: {_ue}")

                self.logger.info(f"Signal {signal['id']} {action_taken} by {message.author.name}")
            else:
                self.logger.warning(f"Failed to process command '{command}' for signal {signal['id']}")

        except Exception as e:
            self.logger.error(f"Error in signal management reply: {e}", exc_info=True)

    async def process_signal(self, message: discord.Message):
        """Process a potential trading signal with enhanced parsing"""
        try:
            from core.parser import parse_signal, RejectedSignal
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
                success, signal_id = await self.signal_db.save_signal(
                    parsed,
                    str(message.id),
                    str(message.channel.id)
                )

                if success:
                    await self.safe_add_reaction(message, "✅")
                    self.logger.info(f"Signal #{signal_id} processed: {parsed.instrument} {parsed.direction}")
                else:
                    existing = await self.signal_db.get_signal_by_message_id(str(message.id))
                    if existing and existing['status'] != 'cancelled':
                        await self.safe_add_reaction(message, "⚠️")
                    else:
                        await self.safe_add_reaction(message, "♻️")
            else:
                if self.looks_like_signal(message.content):
                    await self.safe_add_reaction(message, "⚠️")
                    self.logger.debug(f"Failed to parse apparent signal from message {message.id}")

        except Exception as e:
            # Use repr() to safely convert any problematic characters
            self.logger.error(f"Error processing signal: {repr(str(e))}", exc_info=True)
            await self.safe_add_reaction(message, "⚠️")

    async def safe_add_reaction(self, message: discord.Message, emoji: str):
        """Safely add a reaction to a message, handling common Discord API errors"""
        try:
            await message.add_reaction(emoji)
        except discord.NotFound:
            # Message was deleted or we lost access
            self.logger.warning(f"Could not add reaction to message {message.id} - message not found")
        except discord.Forbidden:
            # Lost permissions to add reactions
            self.logger.warning(f"Could not add reaction to message {message.id} - missing permissions")
        except discord.HTTPException as e:
            # Other Discord API errors
            self.logger.warning(f"Could not add reaction to message {message.id} - HTTP error: {repr(str(e))}")
        except Exception as e:
            # Catch-all for any other errors
            self.logger.error(f"Unexpected error adding reaction: {repr(str(e))}", exc_info=False)

    async def handle_message_edit(self, before: discord.Message, after: discord.Message):
        """Handle message edits with signal reparsing"""
        if after.author.bot:
            return

        # CHECK: Only process edits in allowed channels
        if not self.is_allowed_channel(after.channel.id):
            return

        if after.channel.id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message edited in monitored channel: {after.channel.name}")

        existing = await self.signal_db.get_signal_by_message_id(str(after.id))
        if not existing:
            await after.clear_reactions()
            await self.process_signal(after)
            return

        from core.parser import parse_signal, RejectedSignal
        channel_name = self.get_channel_name(after.channel.id)
        parsed = parse_signal(after.content, channel_name)

        if isinstance(parsed, RejectedSignal):
            await after.clear_reactions()
            await after.add_reaction("❌")
            self.logger.info(
                f"Signal edit rejected as malformed (likely typo): {after.id}: {parsed.reason}"
            )
            return

        if parsed:
            # If the signal was cancelled, reactivate it with the updated content
            if existing['status'] == 'cancelled':
                from database.signal_operations.lifecycle import LifecycleManager
                lifecycle = LifecycleManager(self.signal_db.db)
                reactivated = await lifecycle.reactivate_cancelled_signal(
                    existing['id'], parsed, self.signal_db.db
                )
                if reactivated:
                    # Also update the signal fields with the newly parsed content
                    await self.signal_db.update_signal_from_edit(str(after.id), parsed)

                    # Mark NM-immune so the near-miss monitor won't auto-cancel it again
                    if hasattr(self.bot, 'monitor') and self.bot.monitor:
                        nm = getattr(self.bot.monitor, 'nm_monitor', None)
                        if nm:
                            nm.mark_immune(existing['id'])

                    await after.clear_reactions()
                    await after.add_reaction("✅")
                    await after.add_reaction("♻️")
                    self.logger.info(f"Cancelled signal reactivated after edit: {after.id}")

                    if self.alert_system:
                        try:
                            updated_signal = await self.signal_db.get_signal_with_limits(existing['id'])
                            if updated_signal:
                                _sig_id = updated_signal.get('signal_id') or updated_signal.get('id')
                                _signal_for_update = dict(updated_signal)
                                _signal_for_update['signal_id'] = _sig_id
                                ping_text = (
                                    f"♻️ **{updated_signal['instrument']}** {updated_signal['direction'].upper()} — "
                                    f"signal reactivated by sender (edited)"
                                )
                                await self.alert_system.update_signal_message(
                                    signal=_signal_for_update,
                                    event="reactivated",
                                    ping_text=ping_text,
                                )
                        except Exception as _ue:
                            self.logger.warning(f"Could not update embed after reactivation via edit: {_ue}")
                else:
                    await after.add_reaction("❌")
                    self.logger.warning(f"Failed to reactivate cancelled signal on edit: {after.id}")
                return

            success = await self.signal_db.update_signal_from_edit(str(after.id), parsed)

            if success:
                await after.clear_reactions()
                await after.add_reaction("✅")
                await after.add_reaction("📝")
                self.logger.info(f"Signal updated after edit: {after.id}")

                # Update the persistent embed and send an alert ping
                if self.alert_system:
                    try:
                        updated_signal = await self.signal_db.get_signal_with_limits(existing['id'])
                        if updated_signal:
                            _sig_id = updated_signal.get('signal_id') or updated_signal.get('id')
                            _signal_for_update = dict(updated_signal)
                            _signal_for_update['signal_id'] = _sig_id
                            ping_text = (
                                f"📝 **{updated_signal['instrument']}** {updated_signal['direction'].upper()} — "
                                f"signal updated by sender"
                            )
                            await self.alert_system.update_signal_message(
                                signal=_signal_for_update,
                                event="edited",
                                ping_text=ping_text,
                            )
                    except Exception as _ue:
                        self.logger.warning(f"Could not update embed after signal edit: {_ue}")
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
        # CHECK: Only process deletions in allowed channels
        if not self.is_allowed_channel(payload.channel_id):
            return

        if payload.channel_id not in self.bot.monitored_channels:
            return

        self.logger.info(f"Message deleted in monitored channel: {payload.message_id}")
        result = await self.signal_db.cancel_signal_by_message(str(payload.message_id))

        # cancel_signal_by_message returns True or the signal id depending on implementation
        if result:
            self.logger.info(f"Signal cancelled due to message deletion: {payload.message_id}")
            # Update the persistent alert embed
            if self.alert_system:
                try:
                    # Fetch the signal id from the DB so we can update the embed
                    cancelled_signal = await self.signal_db.get_signal_by_message_id(str(payload.message_id))
                    if cancelled_signal:
                        sig_id = cancelled_signal.get('id') or cancelled_signal.get('signal_id')
                        if sig_id:
                            await self.alert_system.update_embed_for_signal_id(sig_id, 'cancelled')
                except Exception as _ue:
                    self.logger.warning(f"Could not update embed after message delete cancel: {_ue}")

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