"""
Alert System - Handles all alert generation and sending for the price monitor
Enhanced with message ID tracking for reply-based status management
ENHANCED: Shows spread value in alerts when spread buffer is enabled
"""

import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timezone
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
    - Shows spread value when spread buffer is enabled
    """

    def __init__(self, alert_channel: Optional[discord.TextChannel] = None, bot=None):
        """
        Initialize the alert system

        Args:
            alert_channel: Discord channel for sending alerts
            bot: Discord bot instance for fetching channels
        """
        self.alert_channel = alert_channel
        self.pa_alert_channel = None
        self.toll_alert_channel = None
        self.general_toll_alert_channel = None
        self._load_pa_channels()
        self._load_toll_channels()

        self.bot = bot

        # Alert message tracking - maps alert message ID to signal ID
        # This allows users to reply to alerts to manage signals
        self.alert_messages = {}  # {message_id: signal_id}

        # Statistics tracking
        self.stats = {
            'approaching_sent': 0,
            'hit_sent': 0,
            'stop_loss_sent': 0,
            'auto_tp_sent': 0,
            'spread_hour_cancelled': 0,
            'total_alerts': 0,
            'errors': 0
        }

    def set_channel(self, channel: discord.TextChannel):
        """Update the alert channel"""
        self.alert_channel = channel
        logger.info(f"Alert channel set to #{channel.name} ({channel.id})")

    def _load_pa_channels(self):
        """Load PA channel IDs from config"""
        try:
            from pathlib import Path
            import json

            config_path = Path(__file__).parent.parent / 'config' / 'channels.json'
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Get PA channel IDs
            monitored = config.get('monitored_channels', {})
            self.pa_channel_ids = set()

            for channel_name, channel_id in monitored.items():
                if 'pa' in channel_name.lower() or 'price-action' in channel_name.lower():
                    self.pa_channel_ids.add(str(channel_id))

            logger.info(f"Loaded {len(self.pa_channel_ids)} PA channel IDs: {self.pa_channel_ids}")

        except Exception as e:
            logger.error(f"Failed to load PA channels: {e}")
            self.pa_channel_ids = set()

    def _load_toll_channels(self):
        """Load toll channel IDs from config"""
        try:
            from pathlib import Path
            import json

            config_path = Path(__file__).parent.parent / 'config' / 'channels.json'
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Get toll channel IDs
            monitored = config.get('monitored_channels', {})
            self.toll_channel_ids = set()
            self.general_toll_channel_ids = set()

            for channel_name, channel_id in monitored.items():
                if not channel_id:
                    continue
                if channel_name.lower() == 'general-tolls':
                    self.general_toll_channel_ids.add(str(channel_id))
                elif 'toll' in channel_name.lower():
                    self.toll_channel_ids.add(str(channel_id))

            logger.info(f"Loaded {len(self.toll_channel_ids)} toll channel IDs: {self.toll_channel_ids}")
            logger.info(f"Loaded {len(self.general_toll_channel_ids)} general-toll channel IDs: {self.general_toll_channel_ids}")

        except Exception as e:
            logger.error(f"Failed to load toll channels: {e}")
            self.toll_channel_ids = set()
            self.general_toll_channel_ids = set()

    def set_pa_channel(self, channel: discord.TextChannel):
        """Set the PA alert channel"""
        self.pa_alert_channel = channel
        logger.info(f"PA alert channel set: #{channel.name} ({channel.id})")

    def set_toll_channel(self, channel: discord.TextChannel):
        """Set the toll alert channel"""
        self.toll_alert_channel = channel
        logger.info(f"Toll alert channel set: #{channel.name} ({channel.id})")

    def set_general_toll_channel(self, channel: discord.TextChannel):
        """Set the general-tolls alert channel"""
        self.general_toll_alert_channel = channel
        logger.info(f"General-toll alert channel set: #{channel.name} ({channel.id})")

    def is_pa_signal(self, signal: Dict) -> bool:
        """Check if signal originated from a PA channel"""
        channel_id = str(signal.get('channel_id', ''))
        is_pa = channel_id in self.pa_channel_ids
        if is_pa:
            logger.debug(f"Signal {signal.get('signal_id')} identified as PA signal (channel: {channel_id})")
        return is_pa

    def is_toll_signal(self, signal: Dict) -> bool:
        """Check if signal originated from a toll channel"""
        channel_id = str(signal.get('channel_id', ''))
        is_toll = channel_id in self.toll_channel_ids
        if is_toll:
            logger.debug(f"Signal {signal.get('signal_id')} identified as toll signal (channel: {channel_id})")
        return is_toll

    def is_general_toll_signal(self, signal: Dict) -> bool:
        """Check if signal originated from the general-tolls channel"""
        channel_id = str(signal.get('channel_id', ''))
        is_general = channel_id in self.general_toll_channel_ids
        if is_general:
            logger.debug(f"Signal {signal.get('signal_id')} identified as general-toll signal (channel: {channel_id})")
        return is_general

    def _get_alert_channel(self, signal: Dict) -> discord.TextChannel:
        """
        Determine which alert channel to use based on signal source

        Args:
            signal: Signal dictionary with channel_id

        Returns:
            Alert channel to use
        """
        # Check general-toll signals (most specific — its own channel)
        if self.is_general_toll_signal(signal):
            if self.general_toll_alert_channel:
                logger.debug(f"Routing to general-toll alert channel for signal {signal.get('signal_id')}")
                return self.general_toll_alert_channel
            else:
                logger.warning("General-toll signal detected but no general-toll alert channel configured, using main channel")
                return self.alert_channel

        # Check toll signals first (more specific)
        if self.is_toll_signal(signal):
            if self.toll_alert_channel:
                logger.debug(f"Routing to toll alert channel for signal {signal.get('signal_id')}")
                return self.toll_alert_channel
            else:
                logger.warning(f"Toll signal detected but no toll alert channel configured, using main channel")
                return self.alert_channel

        # Check PA signals
        if self.is_pa_signal(signal):
            if self.pa_alert_channel:
                logger.debug(f"Routing to PA alert channel for signal {signal.get('signal_id')}")
                return self.pa_alert_channel
            else:
                logger.warning(f"PA signal detected but no PA alert channel configured, using main channel")
                return self.alert_channel

        return self.alert_channel

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

    async def send_approaching_alert(self, signal: Dict, limit: Dict, current_price: float,
                                    distance_formatted: str, spread: float = None,
                                    spread_buffer_enabled: bool = False) -> bool:
        """
        Send alert for approaching limit
        ONLY sends for the FIRST limit (sequence_number == 1)

        Args:
            signal: Signal dictionary
            limit: Limit dictionary with sequence_number
            current_price: Current market price
            distance_pips: Distance to limit in pips
            spread: Current spread value (optional)
            spread_buffer_enabled: Whether spread buffer is enabled

        Returns:
            True if alert was sent successfully
        """
        target_channel = self._get_alert_channel(signal)

        if not target_channel:
            logger.error("No alert channel configured")
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
                timestamp=datetime.now(timezone.utc)
            )

            # Add fields
            embed.add_field(
                name="Limit Details:",
                value=f"Limit #{limit['sequence_number']}: {self._format_price(limit['price_level'])}",
                inline=False
            )

            # Current price with optional spread
            if spread_buffer_enabled and spread and spread > 0:
                price_display = f"{self._format_price(current_price + spread)}"
            else:
                price_display = self._format_price(current_price)

            embed.add_field(
                name="Current Price",
                value=price_display,
                inline=True
            )

            embed.add_field(
                name="Distance",
                value=distance_formatted,
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

            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)

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

    async def send_limit_hit_alert(self, signal: Dict, limit: Dict, current_price: float,
                                   spread: float = None, spread_buffer_enabled: bool = False) -> bool:
        """
        Send alert for limit hit

        Args:
            signal: Signal dictionary
            limit: Limit dictionary
            current_price: Current market price
            spread: Current spread value (optional)
            spread_buffer_enabled: Whether spread buffer is enabled

        Returns:
            True if alert was sent successfully
        """
        target_channel = self._get_alert_channel(signal)

        if not target_channel:
            logger.error("No alert channel configured")
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
                timestamp=datetime.now(timezone.utc)
            )

            # Add limit price
            embed.add_field(
                name="Limit Hit:",
                value=f"Limit #{limit['sequence_number']}: {self._format_price(limit['price_level'])}",
                inline=False
            )

            # Hit price with optional spread
            if spread_buffer_enabled and spread and spread > 0:
                price_display = f"{self._format_price(current_price + spread)}"
            else:
                price_display = self._format_price(current_price)

            embed.add_field(
                name="Hit Price",
                value=price_display,
                inline=True
            )

            # Calculate progress — use sequence_number of the hit limit (always current)
            # rather than signal['limits_hit'] which may be stale when limits fire rapidly
            progress = limit['sequence_number']
            total = signal.get('total_limits', 1)

            embed.add_field(
                name="Progress",
                value=f"{progress}/{total} limits hit",
                inline=True
            )

            # Add message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
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

            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)

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
        NOTE: Stop loss alerts NEVER show spread (exact prices only)

        Args:
            signal: Signal dictionary
            current_price: Current market price

        Returns:
            True if alert was sent successfully
        """
        target_channel = self._get_alert_channel(signal)

        if not target_channel:
            logger.error("No alert channel configured")
            return False

        try:
            embed = discord.Embed(
                title="🛑 Stop Loss Hit!",
                description=f"**{signal['instrument']}** {signal['direction'].upper()}",
                color=0xFF0000,  # Red
                timestamp=datetime.now(timezone.utc)
            )

            # Add fields - NO SPREAD for stop loss
            embed.add_field(
                name="Stop Loss Level",
                value=self._format_price(signal['stop_loss']),
                inline=False
            )
            embed.add_field(
                name="Hit Price",
                value=self._format_price(current_price),
                inline=False
            )

            # Add message link if available
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
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

            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)

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

    async def send_spread_hour_cancel_alert(self, signal: Dict, current_price: float) -> bool:
        """
        Send a single informational embed when a signal is hit during spread hour
        (5–6 PM EST weekdays) and is automatically cancelled.

        A role ping is sent so traders are notified of the cancellation.

        Args:
            signal: Signal dictionary
            current_price: The price that triggered the (rejected) hit

        Returns:
            True if the embed was sent successfully
        """
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            logger.error("No alert channel configured for spread hour cancel alert")
            return False

        try:
            embed = discord.Embed(
                title="🕔 Spread Hour — Signal Auto-Cancelled",
                description=(
                    f"**{signal['instrument']}** {signal['direction'].upper()} was triggered "
                    f"during the **5–6 PM EST spread hour** and has been automatically cancelled."
                ),
                color=0xFFA500,   # Orange — informational, not a real hit or loss
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="Trigger Price",
                value=self._format_price(current_price),
                inline=True
            )
            embed.add_field(
                name="Stop Loss Level",
                value=self._format_price(signal['stop_loss']),
                inline=True
            )

            # Show remaining pending limits so the trader knows what to re-enter
            pending = signal.get('pending_limits', [])
            if pending:
                levels = "  |  ".join(
                    self._format_price(lim['price_level']) for lim in pending
                )
                embed.add_field(
                    name=f"Pending Limits ({len(pending)})",
                    value=levels,
                    inline=False
                )

            # Source link
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
                    guild_id = signal.get('guild_id')
                    if not guild_id and self.bot and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id
                    message_url = (
                        f"https://discord.com/channels/"
                        f"{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    )
                    embed.add_field(name="Source", value=message_url, inline=False)

            embed.add_field(
                name="ℹ️ What happened?",
                value=(
                    "Broker spreads widen significantly between 5–6 PM EST each weekday "
                    "as liquidity providers roll positions.  This hit was likely caused by "
                    "spread, not a genuine price move.  The signal has been cancelled to "
                    "protect the trade record."
                ),
                inline=False
            )

            embed.set_footer(text=f"Signal #{signal['signal_id']} • Auto-cancelled (spread hour)")

            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)

            self.track_alert_message(message.id, signal['signal_id'])
            self.stats['spread_hour_cancelled'] += 1
            self.stats['total_alerts'] += 1

            logger.info(
                f"Spread hour cancel alert sent for signal {signal['signal_id']} "
                f"({signal['instrument']} @ {self._format_price(current_price)})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send spread hour cancel alert: {e}")
            self.stats['errors'] += 1
            return False

    async def send_news_cancel_alert(
        self,
        signal: Dict,
        current_price: float,
        news_event,        # NewsEvent — typed loosely to avoid circular import
    ) -> bool:
        """
        Send a compact alert when a signal is auto-cancelled during a news window.
        Includes a role ping.
        """
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            logger.error("No alert channel configured for news cancel alert")
            return False

        try:
            import pytz
            EST = pytz.timezone('America/New_York')
            news_time_est = news_event.news_time.astimezone(EST)

            # Build a compact summary of the signal's limits
            all_limits = signal.get('limits', signal.get('pending_limits', []))
            if all_limits:
                limit_prices = "  |  ".join(
                    self._format_price(
                        lim['price_level'] if isinstance(lim, dict) else lim
                    )
                    for lim in sorted(
                        all_limits,
                        key=lambda l: l['sequence_number'] if isinstance(l, dict) else 0
                    )
                )
            else:
                limit_prices = "—"

            signal_summary = (
                f"**{signal['instrument']}** {signal['direction'].upper()}\n"
                f"Limits: {limit_prices}\n"
                f"SL: {self._format_price(signal['stop_loss'])}"
            )

            embed = discord.Embed(
                title="📰 Signal Cancelled — News",
                description=(
                    f"The following signal was cancelled due to news "
                    f"({news_event.category.upper()} @ "
                    f"{news_time_est.strftime('%I:%M %p')} EST):\n\n"
                    f"{signal_summary}"
                ),
                color=0x5865F2,
                timestamp=datetime.now(timezone.utc),
            )

            embed.set_footer(
                text=f"Signal #{signal['signal_id']} • Auto-cancelled (news mode)"
            )

            # Source link
            if signal.get('message_id') and signal.get('channel_id'):
                if not str(signal['message_id']).startswith('manual_'):
                    guild_id = signal.get('guild_id')
                    if not guild_id and self.bot and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id
                    message_url = (
                        f"https://discord.com/channels/"
                        f"{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    )
                    embed.add_field(name="Source", value=message_url, inline=False)

            # Role ping + embed
            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)

            self.track_alert_message(message.id, signal['signal_id'])
            self.stats['news_cancelled'] = self.stats.get('news_cancelled', 0) + 1
            self.stats['total_alerts'] += 1

            logger.info(
                f"News cancel alert sent for signal {signal['signal_id']} "
                f"({signal['instrument']} @ {self._format_price(current_price)})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send news cancel alert: {e}")
            self.stats['errors'] += 1
            return False

    async def send_news_activated_alert(self, news_event) -> bool:
        """
        Send a small informational embed when a news window becomes active.
        No role ping.
        """
        if not self.alert_channel:
            logger.warning("No alert channel configured for news activated alert")
            return False

        try:
            import pytz
            EST = pytz.timezone('America/New_York')
            news_time_est = news_event.news_time.astimezone(EST)
            start_est = news_event.start_time.astimezone(EST)
            end_est = news_event.end_time.astimezone(EST)

            embed = discord.Embed(
                title="📰 News Mode Active",
                description=(
                    f"News window activated for **{news_event.category.upper()}**\n"
                    f"{start_est.strftime('%I:%M %p')} → {end_est.strftime('%I:%M %p')} EST"
                ),
                color=0x5865F2,
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text=f"Event #{news_event.event_id} • Signals will be auto-cancelled if hit")

            await self.alert_channel.send(embed=embed)
            self.stats['total_alerts'] += 1
            logger.info(f"News activated alert sent for event #{news_event.event_id} ({news_event.category.upper()})")
            return True

        except Exception as e:
            logger.error(f"Failed to send news activated alert: {e}")
            self.stats['errors'] += 1
            return False
    async def send_auto_tp_alert(self, signal: Dict, hit_limits: list,
                                  last_pnl: float, tp_config) -> bool:
        """
        Send auto take-profit alerts to both the alert channel and the profit channel.

        Mirrors what happens on a manual 'profit' reply:
          - Role ping + profit embed → alert channel (same channel as limit-hit alerts)
          - Profit embed → profit_channel (from channels.json)

        Args:
            signal:      Signal dict (needs signal_id, instrument, direction,
                         message_id, channel_id, total_limits)
            hit_limits:  Ordered list of hit limit dicts (with hit_price, sequence_number)
            last_pnl:    P&L of the last hit limit in native units
            tp_config:   TPConfig instance for formatting

        Returns:
            True if at least the alert-channel message was sent successfully.
        """
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            logger.error("send_auto_tp_alert: no alert channel configured")
            return False

        instrument = signal['instrument']
        direction = signal['direction'].upper()
        pnl_display = tp_config.format_value(instrument, last_pnl)
        num_hit = len(hit_limits)
        total = signal.get('total_limits', num_hit)

        # Build source link
        message_url = None
        if signal.get('message_id') and signal.get('channel_id'):
            if not str(signal['message_id']).startswith('manual_'):
                guild_id = signal.get('guild_id')
                if not guild_id and self.bot and self.bot.guilds:
                    guild_id = self.bot.guilds[0].id
                if guild_id:
                    message_url = (
                        f"https://discord.com/channels/{guild_id}"
                        f"/{signal['channel_id']}/{signal['message_id']}"
                    )

        # --- Build the embed ---
        embed = discord.Embed(
            title="💰 Auto Take-Profit Triggered!",
            description=f"**{instrument}** {direction}",
            color=0x00FF00,
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(name="Profit", value=f"**+{pnl_display}**", inline=True)
        embed.add_field(name="Direction", value=direction, inline=True)
        embed.add_field(name="Limits Hit", value=f"{num_hit}/{total}", inline=True)

        # Show each hit limit with its P&L
        if hit_limits:
            limits_lines = []
            for lim in hit_limits:
                seq = lim.get('sequence_number', '?')
                price = lim.get('price_level') or lim.get('hit_price', '?')
                limits_lines.append(f"Limit #{seq}: {self._format_price(price)} ✅")
            embed.add_field(
                name="Hit Limits",
                value="\n".join(limits_lines),
                inline=False
            )

        if message_url:
            embed.add_field(name="Original Signal", value=message_url, inline=False)

        embed.set_footer(text=f"Signal #{signal['signal_id']} • Auto TP • Reply to manage")

        # --- Send to alert channel ---
        try:
            await target_channel.send("<@&1334203997107650662>")
            alert_message = await target_channel.send(embed=embed)
            self.track_alert_message(alert_message.id, signal['signal_id'])
            self.stats['auto_tp_sent'] += 1
            self.stats['total_alerts'] += 1
            logger.info(f"Auto-TP alert sent for signal {signal['signal_id']} to alert channel")
        except Exception as e:
            logger.error(f"Failed to send auto-TP alert to alert channel: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False

        # --- Send to profit channel ---
        try:
            profit_channel = await self._get_profit_channel()
            if profit_channel:
                profit_embed = discord.Embed(
                    title="💰 PROFIT Alert",
                    description=f"Signal #{signal['signal_id']} has been marked as **PROFIT** (Auto TP)",
                    color=0x00FF00,
                    timestamp=datetime.now(timezone.utc)
                )
                profit_embed.add_field(name="Symbol", value=instrument, inline=True)
                profit_embed.add_field(name="Position", value=direction, inline=True)
                profit_embed.add_field(name="Profit", value=f"**+{pnl_display}**", inline=True)

                if hit_limits:
                    limits_lines = []
                    for lim in hit_limits:
                        seq = lim.get('sequence_number', '?')
                        price = lim.get('price_level') or lim.get('hit_price', '?')
                        limits_lines.append(f"Limit #{seq}: {self._format_price(price)} ✅")
                    profit_embed.add_field(
                        name="Limits",
                        value="\n".join(limits_lines),
                        inline=True
                    )

                if signal.get('stop_loss'):
                    profit_embed.add_field(
                        name="Stop Loss",
                        value=self._format_price(signal['stop_loss']),
                        inline=True
                    )

                if message_url:
                    profit_embed.add_field(
                        name="Original Signal",
                        value=f"[View Message]({message_url})",
                        inline=False
                    )

                profit_embed.set_footer(text="Auto Take-Profit")
                await profit_channel.send(embed=profit_embed)
                logger.info(f"Auto-TP profit alert sent for signal {signal['signal_id']} to profit channel")
            else:
                logger.warning("send_auto_tp_alert: no profit_channel configured, skipping")
        except Exception as e:
            # Don't fail the whole call if only the profit channel send fails
            logger.error(f"Failed to send auto-TP alert to profit channel: {e}", exc_info=True)

        return True

    async def _get_profit_channel(self) -> Optional[discord.TextChannel]:
        """Load and return the profit channel from channels.json."""
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).resolve().parent.parent / 'config' / 'channels.json'
            with open(config_path, 'r') as f:
                channels_config = json.load(f)
            profit_channel_id = channels_config.get('profit_channel')
            if not profit_channel_id:
                return None
            channel = self.bot.get_channel(int(profit_channel_id))
            if not channel:
                channel = await self.bot.fetch_channel(int(profit_channel_id))
            return channel
        except Exception as e:
            logger.error(f"Could not load profit channel: {e}")
            return None

    def get_stats(self) -> Dict:
        """Get alert system statistics"""
        return {
            'alerts': {
                'approaching': self.stats['approaching_sent'],
                'hit': self.stats['hit_sent'],
                'stop_loss': self.stats['stop_loss_sent'],
                'auto_tp': self.stats['auto_tp_sent'],
                'total': self.stats['total_alerts']
            },
            'errors': self.stats['errors'],
            'channel_configured': self.alert_channel is not None,
            'tracked_messages': len(self.alert_messages)
        }