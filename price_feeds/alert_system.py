"""
Alert System - Handles all alert generation and sending for the price monitor
Enhanced with message ID tracking for reply-based status management
"""

import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime
from enum import Enum
import discord

from utils.embed_factory import EmbedFactory
from utils.logger import get_logger

logger = get_logger('alert_system')


class AlertType(Enum):
    """Types of alerts"""
    APPROACHING = "approaching"
    HIT = "hit"
    STOP_LOSS = "stop_loss"


class AlertSystem:
    """
    Handles all alert generation and sending for trading signals

    Features:
    - Uses EmbedFactory for consistent embed creation
    - Only sends approaching alert for first limit
    - Handles limit hit and stop loss alerts
    - Tracks alert statistics
    - Stores alert message IDs for reply-based management
    """

    def __init__(self, alert_channel: Optional[discord.TextChannel] = None, bot=None):
        """
        Initialize the alert system

        Args:
            alert_channel: Discord channel for sending alerts
            bot: Discord bot instance for fetching channels
        """
        self.alert_channel = alert_channel
        self.bot = bot

        # Alert message tracking - maps alert message ID to signal ID
        # This allows users to reply to alerts to manage signals
        self.alert_messages = {}  # {message_id: signal_id}

        # Statistics tracking
        self.stats = {
            'approaching_sent': 0,
            'hit_sent': 0,
            'stop_loss_sent': 0,
            'total_alerts': 0,
            'errors': 0
        }

    def set_channel(self, channel: discord.TextChannel):
        """Update the alert channel"""
        self.alert_channel = channel
        logger.info(f"Alert channel set to #{channel.name} ({channel.id})")

    def _format_price(self, price: float) -> str:
        """Format price with appropriate decimal places"""
        if price == 0:
            return "0"

        # Convert to string and strip trailing zeros
        price_str = f"{price:.5f}".rstrip('0').rstrip('.')

        # Ensure at least 2 decimal places for most currencies
        if '.' not in price_str:
            price_str += '.00'
        elif len(price_str.split('.')[1]) < 2:
            price_str += '0'

        return price_str

    def track_alert_message(self, message_id: int, signal_id: int):
        """
        Track an alert message for reply-based management

        Args:
            message_id: Discord message ID of the alert
            signal_id: Database signal ID this alert relates to
        """
        self.alert_messages[str(message_id)] = signal_id
        logger.debug(f"Tracked alert message {message_id} for signal {signal_id}")

        # Clean up old entries if we have too many (keep last 1000)
        if len(self.alert_messages) > 1000:
            # Remove oldest entries
            to_remove = len(self.alert_messages) - 1000
            for key in list(self.alert_messages.keys())[:to_remove]:
                del self.alert_messages[key]

    def get_signal_from_alert(self, message_id: str) -> Optional[int]:
        """
        Get the signal ID associated with an alert message

        Args:
            message_id: Discord message ID of the alert

        Returns:
            Signal ID if found, None otherwise
        """
        return self.alert_messages.get(str(message_id))

    async def send_approaching_alert(self, signal: Dict, limit: Dict, current_price: float, distance_pips: float) -> bool:
        """
        Send alert for approaching limit
        ONLY sends for the FIRST limit (sequence_number == 1)

        Args:
            signal: Signal dictionary
            limit: Limit dictionary with sequence_number
            current_price: Current market price
            distance_pips: Distance to limit in pips

        Returns:
            True if alert was sent successfully
        """
        if not self.alert_channel:
            logger.warning("No alert channel configured")
            return False

        # ONLY send approaching alert for the first limit
        if limit.get('sequence_number', 0) != 1:
            logger.debug(f"Skipping approaching alert for limit #{limit['sequence_number']} (not first limit)")
            return False

        try:
            # Build embed
            embed = discord.Embed(
                title="🟡 First Limit Approaching",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0xFFA500,  # Orange
                timestamp=datetime.utcnow()
            )

            # Add fields
            embed.add_field(
                name="Limit Details",
                value=f"Limit #{limit['sequence_number']}: {self._format_price(limit['price_level'])}",
                inline=False
            )
            embed.add_field(
                name="Current Price",
                value=self._format_price(current_price),
                inline=True
            )
            embed.add_field(
                name="Distance",
                value=f"{distance_pips:.1f} pips",
                inline=True
            )
            embed.add_field(
                name="Progress",
                value=f"{signal.get('limits_hit', 0)}/{signal.get('total_limits', 0)} hit",
                inline=True
            )

            # Add message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
                    # Get channel name
                    channel_name = "unknown-channel"
                    try:
                        # Try to get channel from cache first, then fetch if needed
                        channel = self.bot.get_channel(int(signal['channel_id']))
                        if not channel:
                            channel = await self.bot.fetch_channel(int(signal['channel_id']))
                        channel_name = channel.name
                    except Exception as e:
                        logger.debug(f"Could not get channel name for {signal['channel_id']}: {e}")

                    # Get guild ID from the bot's first guild if not provided
                    guild_id = signal.get('guild_id')
                    if not guild_id and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id

                    message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    embed.add_field(
                        name="Source",
                        value=f"{message_url}",
                        inline=False
                    )

            embed.set_footer(text=f"Signal #{signal['signal_id']} • Reply to manage")

            message = await self.alert_channel.send(embed=embed)

            # Track this alert message
            self.track_alert_message(message.id, signal['signal_id'])

            # Update statistics
            self.stats['approaching_sent'] += 1
            self.stats['total_alerts'] += 1

            logger.info(f"Approaching alert sent for signal {signal['signal_id']}, first limit")
            return True

        except Exception as e:
            logger.error(f"Failed to send approaching alert: {e}")
            self.stats['errors'] += 1
            return False

    async def send_limit_hit_alert(self, signal: Dict, limit: Dict, current_price: float) -> bool:
        """
        Send alert for limit hit

        Args:
            signal: Signal dictionary
            limit: Limit dictionary
            current_price: Current market price

        Returns:
            True if alert was sent successfully
        """
        if not self.alert_channel:
            logger.warning("No alert channel configured")
            return False

        try:
            # Determine title based on limit number
            if limit['sequence_number'] == 1:
                title = "🎯 First Limit Hit!"
            elif limit['sequence_number'] == signal.get('total_limits', 0):
                title = "🎯🎯 Final Limit Hit!"
            else:
                title = f"🎯 Limit #{limit['sequence_number']} Hit!"

            embed = discord.Embed(
                title=title,
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0x00FF00,  # Green
                timestamp=datetime.utcnow()
            )

            # Add fields
            embed.add_field(
                name="Limit Hit",
                value=f"Limit #{limit['sequence_number']}: {self._format_price(limit['price_level'])}",
                inline=False
            )
            embed.add_field(
                name="Hit Price",
                value=self._format_price(current_price),
                inline=True
            )

            # Calculate progress
            progress = signal.get('limits_hit', 0) + 1
            total = signal.get('total_limits', 1)

            embed.add_field(
                name="Progress",
                value=f"{progress}/{total} limits hit",
                inline=True
            )

            # Add message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
                    # Get channel name
                    channel_name = "unknown-channel"
                    try:
                        # Try to get channel from cache first, then fetch if needed
                        channel = self.bot.get_channel(int(signal['channel_id']))
                        if not channel:
                            channel = await self.bot.fetch_channel(int(signal['channel_id']))
                        channel_name = channel.name
                    except Exception as e:
                        logger.debug(f"Could not get channel name for {signal['channel_id']}: {e}")

                    # Get guild ID from the bot's first guild if not provided
                    guild_id = signal.get('guild_id')
                    if not guild_id and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id

                    message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    embed.add_field(
                        name="Source",
                        value=f"{message_url}",
                        inline=False
                    )

            # Add special message for milestones
            if progress == 1:
                embed.add_field(
                    name="💡 Status",
                    value="Signal is now in HIT status",
                    inline=False
                )
            elif progress == total:
                embed.add_field(
                    name="✅ Complete",
                    value="All limits have been hit!",
                    inline=False
                )

            embed.set_footer(text=f"Signal #{signal['signal_id']} • Reply to manage")

            message = await self.alert_channel.send(embed=embed)

            # Track this alert message
            self.track_alert_message(message.id, signal['signal_id'])

            # Update statistics
            self.stats['hit_sent'] += 1
            self.stats['total_alerts'] += 1

            logger.info(f"Limit hit alert sent for signal {signal['signal_id']}, limit {limit['sequence_number']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send limit hit alert: {e}")
            self.stats['errors'] += 1
            return False

    async def send_stop_loss_alert(self, signal: Dict, current_price: float) -> bool:
        """
        Send alert for stop loss hit

        Args:
            signal: Signal dictionary
            current_price: Current market price

        Returns:
            True if alert was sent successfully
        """
        if not self.alert_channel:
            logger.warning("No alert channel configured")
            return False

        try:
            embed = discord.Embed(
                title="🛑 Stop Loss Hit!",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0xFF0000,  # Red
                timestamp=datetime.utcnow()
            )

            # Add fields
            embed.add_field(
                name="Stop Loss Level",
                value=self._format_price(signal['stop_loss']),
                inline=True
            )
            embed.add_field(
                name="Hit Price",
                value=self._format_price(current_price),
                inline=True
            )

            # Show progress
            if 'limits_hit' in signal and 'total_limits' in signal:
                embed.add_field(
                    name="Limits Hit Before SL",
                    value=f"{signal['limits_hit']}/{signal['total_limits']}",
                    inline=True
                )

            # Add message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
                    # Get channel name
                    channel_name = "unknown-channel"
                    try:
                        # Try to get channel from cache first, then fetch if needed
                        channel = self.bot.get_channel(int(signal['channel_id']))
                        if not channel:
                            channel = await self.bot.fetch_channel(int(signal['channel_id']))
                        channel_name = channel.name
                    except Exception as e:
                        logger.debug(f"Could not get channel name for {signal['channel_id']}: {e}")

                    # Get guild ID from the bot's first guild if not provided
                    guild_id = signal.get('guild_id')
                    if not guild_id and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id

                    message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    embed.add_field(
                        name="Source",
                        value=f"{message_url}",
                        inline=False
                    )

            # Add warning message
            embed.add_field(
                name="⚠️ Action Required",
                value="Signal has been stopped out. Review position immediately.",
                inline=False
            )

            embed.set_footer(text=f"Signal #{signal['signal_id']} • Status changed to STOP_LOSS • Reply to manage")

            message = await self.alert_channel.send(embed=embed)

            # Track this alert message
            self.track_alert_message(message.id, signal['signal_id'])

            # Update statistics
            self.stats['stop_loss_sent'] += 1
            self.stats['total_alerts'] += 1

            logger.info(f"Stop loss alert sent for signal {signal['signal_id']}")
            return True

        except Exception as e:
            logger.error(f"Failed to send stop loss alert: {e}")
            self.stats['errors'] += 1
            return False

    def get_stats(self) -> Dict:
        """Get alert system statistics"""
        return {
            'alerts': {
                'approaching': self.stats['approaching_sent'],
                'hit': self.stats['hit_sent'],
                'stop_loss': self.stats['stop_loss_sent'],
                'total': self.stats['total_alerts']
            },
            'errors': self.stats['errors'],
            'channel_configured': self.alert_channel is not None,
            'tracked_messages': len(self.alert_messages)
        }