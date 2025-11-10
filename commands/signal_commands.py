"""
Signal Commands - Commands for managing trading signals
Updated to work with streaming architecture
FIXED: Better crypto support and decimal formatting
"""
from database.models import SignalStatus
from .base_command import BaseCog
from utils.embed_factory import EmbedFactory
from core.parser import parse_signal
from utils.logger import get_logger
import discord
from discord.ext import commands
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime

logger = get_logger("signal_commands")


def format_price(price: float, symbol: str = None) -> str:
    """
    Format price with appropriate decimal places
    FIXED: Removes redundant trailing zeros

    Args:
        price: Price to format
        symbol: Optional symbol for context-aware formatting

    Returns:
        Formatted price string
    """
    if price is None:
        return "N/A"

    # Determine decimal places based on price magnitude
    if price < 0.0001:
        # Very small prices (some crypto pairs)
        formatted = f"{price:.8f}"
    elif price < 0.01:
        # Small prices (JPY pairs, some crypto)
        formatted = f"{price:.5f}"
    elif price < 10:
        # Medium prices (forex, some indices)
        formatted = f"{price:.5f}"
    elif price < 100:
        # Larger prices (some indices)
        formatted = f"{price:.3f}"
    elif price < 10000:
        # Large prices (BTC, indices)
        formatted = f"{price:.2f}"
    else:
        # Very large prices (BTC, some stocks)
        formatted = f"{price:.2f}"

    # Remove redundant trailing zeros, but keep at least one decimal place
    # 100000.00000 -> 100000.0
    # 1.23400 -> 1.234
    # 1.10000 -> 1.1
    if '.' in formatted:
        # Remove trailing zeros
        formatted = formatted.rstrip('0')
        # Ensure at least one decimal place
        if formatted.endswith('.'):
            formatted += '0'

    return formatted


def format_distance_display(symbol: str, distance_value: float, is_crypto: bool = False) -> str:
    """
    Format distance for display in active command
    FIXED: Now supports crypto with $ formatting

    Args:
        symbol: Trading symbol
        distance_value: Distance in pips or dollars
        is_crypto: Whether this is a crypto symbol

    Returns:
        Formatted distance string
    """
    if is_crypto:
        # For crypto, show dollar amount
        # e.g., "$234.50 away"
        return f"${abs(distance_value):.2f}"
    else:
        # For forex/indices, show pips
        # e.g., "23.4 pips"
        if abs(distance_value) < 1:
            return f"{abs(distance_value):.1f} pip"
        else:
            return f"{abs(distance_value):.1f} pips"


class ActiveSignalsView(discord.ui.View):
    """Pagination view for active signals"""

    def __init__(self, signals: List[Dict], embed_factory, guild_id: int,
                 instrument: Optional[str], page_size: int = 10, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.signals = signals
        self.embed_factory = embed_factory
        self.guild_id = guild_id
        self.instrument = instrument
        self.page_size = page_size
        self.current_page = 0
        self.max_page = (len(signals) - 1) // page_size if signals else 0

        # Update button states
        self.update_buttons()

    def update_buttons(self):
        """Update button states based on current page"""
        self.previous_button.disabled = self.current_page <= 0
        self.next_button.disabled = self.current_page >= self.max_page

        # Update page counter label
        self.page_label.label = f"Page {self.current_page + 1}/{self.max_page + 1}"

    def get_page_embed(self) -> discord.Embed:
        """Get embed for current page"""
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.signals))
        page_signals = self.signals[start_idx:end_idx]

        # Create embed using existing factory method but with pagination info
        embed = self.create_active_signals_embed(
            page_signals,
            self.guild_id,
            self.instrument,
            page_info=(self.current_page + 1, self.max_page + 1, len(self.signals))
        )
        return embed

    def create_active_signals_embed(self, signals: List[Dict], guild_id: int,
                                    instrument: Optional[str], page_info: tuple) -> discord.Embed:
        """Create embed for active signals with pagination info"""
        current_page, total_pages, total_signals = page_info

        if not signals and current_page == 1:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500  # Warning color
            )
            return embed

        embed = discord.Embed(
            title="📊 Active Signals",
            description=f"Showing page {current_page}/{total_pages} ({total_signals} total signals)" +
                        (f" for {instrument}" if instrument else ""),
            color=0x00BFFF  # Info color
        )

        for signal in signals:
            # Get status emoji
            status_emoji = self._get_status_emoji(signal.get('status', 'active'))

            # Format limits
            pending_limits = signal.get('pending_limits', [])
            hit_limits = signal.get('hit_limits', [])

            if pending_limits:
                # FIXED: Use format_price for better formatting
                limits_str = ", ".join([format_price(p, signal['instrument']) for p in pending_limits[:3]])
                if len(pending_limits) > 3:
                    limits_str += f" (+{len(pending_limits) - 3} more)"
            else:
                limits_str = "None pending"

            if hit_limits:
                limits_str += f" | {len(hit_limits)} hit"

            # Create link or label
            if str(signal['message_id']).startswith("manual_"):
                link_label = "Manual Entry"
            else:
                message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                link_label = f"{message_url}"

            # Build field value
            field_value = f"**Limits:** {limits_str}"

            # Add distance information if available
            # FIXED: Now works for crypto too
            if signal.get('distance_info') and signal.get('status', 'active').lower() in ['active', 'hit']:
                distance_info = signal['distance_info']

                # Check if crypto or index
                is_crypto = signal.get('is_crypto', False)
                is_index = signal.get('is_index', False)

                if is_crypto or is_index:
                    # Show dollar distance for crypto and indices
                    distance_dollars = abs(distance_info.get('distance', 0))
                    if distance_dollars > 0 and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** ${distance_dollars:.2f} away"
                else:
                    # Show pip distance for forex
                    formatted_distance = distance_info.get('formatted', '')
                    if formatted_distance and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** {formatted_distance}"

            if signal.get('time_remaining'):
                field_value += f"\n**Expiry:** {signal['time_remaining']}"

            field_value += f"\n**Source:** {link_label}"

            embed.add_field(
                name=f"{status_emoji} #{signal['id']} - {signal['instrument']} - {signal['direction'].upper()}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Total: {total_signals} signals | Use buttons to navigate")
        return embed

    def _get_status_emoji(self, status: str) -> str:
        """Get emoji for status"""
        status_emojis = {
            'active': '🟢',
            'hit': '🎯',
            'profit': '💰',
            'breakeven': '➖',
            'stop_loss': '🛑',
            'cancelled': '❌'
        }
        return status_emojis.get(status.lower(), '⚫')

    def _format_price_list(self, prices: list) -> str:
        """Format list of prices - DEPRECATED, use format_price instead"""
        if not prices:
            return "None"
        return ", ".join(f"{p:.5f}" if isinstance(p, (int, float)) else str(p) for p in prices)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page"""
        self.current_page -= 1
        self.update_buttons()
        embed = self.get_page_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Page 1/1", style=discord.ButtonStyle.secondary, disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Page counter (non-interactive)"""
        pass

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page"""
        self.current_page += 1
        self.update_buttons()
        embed = self.get_page_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class SignalCommands(BaseCog):
    """Commands for managing trading signals"""

    @commands.command(name='active')
    async def active_signals(self, ctx: commands.Context, *, args: str = None):
        """
        Show active trading signals with sorting and pagination
        FIXED: Now works with crypto showing dollar distances

        Usage:
            !active - Show most recent active signals
            !active BTCUSDT - Filter by instrument
            !active sort:distance - Sort by distance to limit
            !active sort:recent - Sort by most recent (default)
            !active sort:oldest - Sort by oldest first
            !active sort:progress - Sort by most limits hit
            !active BTCUSDT sort:distance - Combine filter and sort
        """

        # Parse arguments
        instrument = None
        sort_method = 'recent'  # default

        if args:
            args_parts = args.split()
            for part in args_parts:
                if part.startswith('sort:'):
                    sort_method = part.split(':', 1)[1].lower()
                else:
                    # Assume it's an instrument filter
                    instrument = part.upper()

        # Validate sort method
        valid_sorts = ['recent', 'oldest', 'distance', 'progress']
        if sort_method not in valid_sorts:
            await ctx.send(f"❌ Invalid sort method. Valid options: {', '.join(valid_sorts)}")
            return

        # Get signals from database
        signals = await self.signal_db.get_active_signals_detailed(instrument)

        if not signals:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500
            )
            await ctx.send(embed=embed)
            return

        # In the active_signals command, around line 267 in your file:

        # Calculate distances for each signal (for sorting and display)
        # FIXED: Now properly handles crypto AND indices
        if hasattr(self.bot, 'monitor') and self.bot.monitor:
            if hasattr(self.bot.monitor, 'stream_manager'):
                from price_feeds.alert_config import AlertDistanceConfig
                alert_config = AlertDistanceConfig()

                for signal in signals:
                    if signal.get('pending_limits'):
                        # Get current price from stream manager
                        symbol = signal['instrument']

                        # Determine asset type
                        is_crypto = any(crypto in symbol.upper() for crypto in
                                        ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL',
                                         'DOT']) or 'USDT' in symbol.upper()

                        # CRITICAL FIX: Detect indices for dollar distance
                        is_index = any(idx in symbol.upper() for idx in
                                       ['SPX', 'NAS', 'DOW', 'DAX', 'CHINA50', 'US500', 'USTEC', 'US30',
                                        'US2000', 'RUSSELL', 'GER', 'DE30', 'DE40', 'JP225', 'NIKKEI'])

                        signal['is_crypto'] = is_crypto
                        signal['is_index'] = is_index

                        try:
                            cached_price = await self.bot.monitor.stream_manager.get_latest_price(symbol)

                            if cached_price:
                                # Determine which price to use based on direction
                                direction = signal['direction'].lower()
                                current_price = cached_price['ask'] if direction == 'long' else cached_price['bid']

                                # Get the first pending limit
                                if signal['pending_limits']:
                                    limit_price = signal['pending_limits'][0]

                                    # Calculate distance
                                    if direction == 'long':
                                        distance = current_price - limit_price
                                    else:
                                        distance = limit_price - current_price

                                    # Format based on asset type
                                    # CRITICAL FIX: Indices use dollar distance like crypto
                                    if is_crypto or is_index:
                                        # For crypto and indices, distance is in dollars
                                        distance_value = abs(distance)
                                        formatted = f"${distance_value:.2f} away"
                                    else:
                                        # For forex, use pips
                                        pip_size = alert_config.get_pip_size(symbol)
                                        distance_pips = abs(distance) / pip_size
                                        distance_value = distance_pips
                                        formatted = alert_config.format_distance_for_display(symbol, distance_pips)

                                    # Store distance info
                                    signal['distance_info'] = {
                                        'distance': distance_value,
                                        'current_price': current_price,
                                        'formatted': formatted
                                    }
                        except Exception as e:
                            logger.warning(f"Could not get price for {symbol}: {e}")
                            pass

        # Apply sorting
        if sort_method == 'recent':
            # Already sorted by created_at DESC from database
            pass
        elif sort_method == 'oldest':
            signals.reverse()
        elif sort_method == 'distance':
            # Sort by distance (closest first)
            def get_distance_key(signal):
                if signal.get('distance_info'):
                    return signal['distance_info']['distance']
                return float('inf')  # Put signals without distance at the end

            signals.sort(key=get_distance_key)
        elif sort_method == 'progress':
            # Sort by number of limits hit (most progress first)
            signals.sort(key=lambda s: len(s.get('hit_limits', [])), reverse=True)

        # Create pagination view
        view = ActiveSignalsView(
            signals=signals,
            embed_factory=None,
            guild_id=ctx.guild.id,
            instrument=instrument,
            page_size=10
        )

        # Send initial embed with view
        embed = view.get_page_embed()

        # Add sort info to embed
        sort_descriptions = {
            'recent': 'Most Recent First',
            'oldest': 'Oldest First',
            'distance': 'Closest to Limit',
            'progress': 'Most Progress'
        }
        embed.add_field(
            name="Sort Method",
            value=sort_descriptions.get(sort_method, sort_method.title()),
            inline=False
        )

        await ctx.send(embed=embed, view=view)

    @commands.command(name='activetxt')
    async def active_signals_text(self, ctx: commands.Context, instrument: str = None):
        """
        Show active trading signals in plain text format for easy copying
        FIXED: Better decimal formatting

        Usage:
            !activetxt - Show all active signals in text format
            !activetxt BTCUSDT - Filter by instrument
        """

        # Get signals from database (most recent first)
        signals = await self.signal_db.get_active_signals_detailed(instrument)

        if not signals:
            await ctx.send("No active signals found" + (f" for {instrument}" if instrument else ""))
            return

        # Format signals into text lines
        formatted_lines = []

        for signal in signals:
            # Get signal ID
            signal_id = signal['id']

            # Create hyperlink for the signal ID
            if str(signal['message_id']).startswith("manual_"):
                signal_id_formatted = f"**#{signal_id}**"
            else:
                message_url = f"https://discord.com/channels/{ctx.guild.id}/{signal['channel_id']}/{signal['message_id']}"
                signal_id_formatted = f"[**#{signal_id}**]({message_url})"

            # Get instrument (convert to underscore format if needed)
            instrument_name = signal['instrument']
            if len(instrument_name) == 6 and '_' not in instrument_name:
                instrument_name = f"{instrument_name[:3]}_{instrument_name[3:]}"
            elif instrument_name.startswith('XAU') and len(instrument_name) == 6:
                instrument_name = f"XAU_{instrument_name[3:]}"
            elif instrument_name.startswith('XAG') and len(instrument_name) == 6:
                instrument_name = f"XAG_{instrument_name[3:]}"

            # Get direction
            direction = signal['direction'].upper()

            # Format pending limits as entries - FIXED: Better formatting
            pending_limits = signal.get('pending_limits', [])
            if pending_limits:
                entries_parts = []
                for i, limit_price in enumerate(pending_limits, 1):
                    formatted_price = format_price(limit_price, signal['instrument'])
                    entries_parts.append(f"{i}: {formatted_price}")
                entries_str = ", ".join(entries_parts)
            else:
                entries_str = "None"

            # Format stop loss - FIXED: Better formatting
            stop_loss = signal.get('stop_loss')
            if stop_loss:
                sl_str = format_price(stop_loss, signal['instrument'])
            else:
                sl_str = "None"

            # Format expiry time
            expiry_time = signal.get('expiry_type')
            expiry_map = {
                'day_end': 'VTD',
                'week_end': 'VTW',
                'month_end': 'VTM',
                'no_expiry': 'VTAI'
            }
            expiry_str = expiry_map.get(expiry_time, 'N/A')

            # Build the formatted line
            line = f"{signal_id_formatted} | {instrument_name} | {direction} | Entries: {entries_str} | SL: {sl_str} | {expiry_str}"
            formatted_lines.append(line)

        # Split into messages if needed
        messages = []
        current_message = []
        current_length = 0

        # Add header
        header = f"**Active Signals ({len(signals)} total)**\n"
        if instrument:
            header = f"**Active Signals for {instrument} ({len(signals)} total)**\n"

        current_message.append(header)
        current_length = len(header)

        for line in formatted_lines:
            line_with_newline = line + "\n"
            line_length = len(line_with_newline)

            if current_length + line_length > 1900:
                messages.append("".join(current_message))
                current_message = []
                current_length = 0

            current_message.append(line_with_newline)
            current_length += line_length

        if current_message:
            messages.append("".join(current_message))

        # Send all messages
        for i, message in enumerate(messages):
            if i > 0:
                await asyncio.sleep(0.5)
            await ctx.send(message)

        if len(messages) > 1:
            await ctx.send(f"*Sent in {len(messages)} parts due to length*")

    @commands.command(name='all')
    async def all_signals(self, ctx: commands.Context, status: str = None):
        """
        Show all signals or filter by status
        FIXED: Better decimal formatting

        Usage: !all [status]
        Valid statuses: active, hit, profit, breakeven, stop_loss, cancelled
        """
        from database import db

        valid_statuses = ['active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled']

        if status:
            status = status.lower()
            if status not in valid_statuses:
                await ctx.reply(f"Invalid status. Valid options: {', '.join(valid_statuses)}")
                return
            query = "SELECT * FROM signals WHERE status = ? ORDER BY created_at DESC LIMIT 20"
            signals = await db.fetch_all(query, (status,))
        else:
            query = "SELECT * FROM signals ORDER BY created_at DESC LIMIT 20"
            signals = await db.fetch_all(query)

        if not signals:
            await ctx.reply(f"No signals found" + (f" with status '{status}'" if status else ""))
            return

        # Create embed
        embed = discord.Embed(
            title=f"📊 {'All' if not status else status.title()} Signals",
            description=f"Showing {len(signals)} most recent signal(s)",
            color=discord.Color.blue()
        )

        status_emoji_map = {
            'active': '🟢',
            'hit': '🎯',
            'profit': '💰',
            'breakeven': '➖',
            'stop_loss': '🛑',
            'cancelled': '❌'
        }

        for signal in signals[:10]:
            # FIXED: Better stop loss formatting
            stop_loss_value = format_price(signal['stop_loss'], signal['instrument']) if signal['stop_loss'] else "N/A"
            status_emoji = status_emoji_map.get(signal['status'], '❓')

            embed.add_field(
                name=f"#{signal['id']} {status_emoji} {signal['instrument']} - {signal['direction'].upper()}",
                value=f"**Status:** {signal['status']}\n**Stop:** {stop_loss_value}",
                inline=False
            )

        if len(signals) > 10:
            embed.set_footer(text=f"Showing first 10 of {len(signals)} signals")

        await ctx.send(embed=embed)

    @commands.command(name='add')
    async def add_signal(self, ctx: commands.Context, *, signal_text: str = None):
        """Add a trading signal manually from the command channel"""

        if self.bot.command_channel_id and ctx.channel.id != self.bot.command_channel_id:
            await ctx.reply("This command can only be used in the command channel.")
            return

        if not signal_text:
            await ctx.reply("Please provide signal text. Example: `!add 1.0850 1.0840 eurusd long stop 1.0820`")
            return

        # Determine default expiry based on instrument
        temp_parsed = parse_signal(signal_text, "command")

        # If no expiry specified in text, add default
        text_lower = signal_text.lower()
        has_expiry = any(exp in text_lower for exp in ['vth', 'vtai', 'vtd', 'vtwe', 'vtme', 'alien'])

        if not has_expiry and temp_parsed:
            major_forex = ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD']

            if temp_parsed.instrument in major_forex:
                signal_text += " vtd"
            else:
                signal_text += " vtwe"

        # Parse the signal
        parsed = parse_signal(signal_text, "command")

        if not parsed:
            await ctx.reply("❌ Failed to parse signal. Please check the format.")
            return

        # Save to database
        pseudo_message_id = f"manual_{ctx.author.id}_{ctx.message.id}"

        success, signal_id = await self.signal_db.save_signal(
            parsed,
            pseudo_message_id,
            str(ctx.channel.id)
        )

        if success:
            embed = EmbedFactory.signal_added(signal_id, parsed, ctx.author.name)
            await ctx.send(embed=embed)

            # Note: The monitor should automatically subscribe to the symbol
            # when it detects a new signal in the database
        else:
            await ctx.reply("⚠️ Failed to save signal to database.")

    @commands.command(name='delete')
    async def delete_signal(self, ctx: commands.Context, signal_id: int = None):
        """Delete a specific signal by ID"""

        if not signal_id:
            await ctx.reply("Please provide a signal ID. Example: `!delete 42`")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.reply(f"❌ Signal #{signal_id} not found.")
            return

        try:
            from database import db

            async with db.get_connection() as conn:
                await conn.execute("DELETE FROM signals WHERE id = ?", (signal_id,))
                await conn.commit()
                success = True
        except Exception as e:
            self.logger.error(f"Error deleting signal {signal_id}: {e}")
            success = False

        if success:
            embed = discord.Embed(
                title="🗑️ Signal Deleted",
                description=f"Signal #{signal_id} has been deleted",
                color=discord.Color.orange()
            )
            embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
            embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
            embed.add_field(name="Status", value=signal['status'], inline=True)
            embed.set_footer(text=f"Deleted by {ctx.author.name}")

            await ctx.send(embed=embed)
            self.logger.info(f"Signal {signal_id} deleted by {ctx.author.name}")

            # Note: The monitor should automatically unsubscribe from the symbol
            # if no other signals need it
        else:
            await ctx.reply("⚠️ Failed to delete signal.")

    @commands.command(name='info')
    async def signal_info(self, ctx: commands.Context, signal_id: int = None):
        """
        Show detailed information about a specific signal
        FIXED: Better decimal formatting
        """

        if not signal_id:
            await ctx.reply("Please provide a signal ID. Example: `!info 42`")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.reply(f"❌ Signal #{signal_id} not found.")
            return

        # Create detailed embed
        status_emoji = {
            'active': '🟢',
            'hit': '🎯',
            'profit': '💰',
            'breakeven': '➖',
            'stop_loss': '🛑',
            'cancelled': '❌'
        }.get(signal['status'], '❓')

        embed = discord.Embed(
            title=f"{status_emoji} Signal #{signal_id} - {signal['instrument']}",
            color=discord.Color.blue()
        )

        # Basic info
        embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
        embed.add_field(name="Status", value=signal['status'].upper(), inline=True)

        # FIXED: Better stop loss formatting
        stop_loss_formatted = format_price(signal['stop_loss'], signal['instrument']) if signal['stop_loss'] else "N/A"
        embed.add_field(name="Stop Loss", value=stop_loss_formatted, inline=True)

        # Streaming status (if available)
        if hasattr(self.bot, 'monitor') and self.bot.monitor:
            if hasattr(self.bot.monitor, 'stream_manager'):
                is_subscribed = signal['instrument'] in self.bot.monitor.stream_manager.subscribed_symbols
                embed.add_field(
                    name="Streaming Status",
                    value="🟢 Subscribed" if is_subscribed else "⚪ Not Subscribed",
                    inline=True
                )

        # Limits info - FIXED: Better formatting
        if signal['limits']:
            pending_limits = [l for l in signal['limits'] if l['status'] == 'pending']
            hit_limits = [l for l in signal['limits'] if l['status'] == 'hit']

            if pending_limits:
                pending_str = "\n".join([f"• {format_price(l['price_level'], signal['instrument'])}"
                                        for l in pending_limits[:5]])
                if len(pending_limits) > 5:
                    pending_str += f"\n... +{len(pending_limits) - 5} more"
                embed.add_field(name=f"Pending Limits ({len(pending_limits)})", value=pending_str, inline=False)

            if hit_limits:
                hit_str = "\n".join([f"• {format_price(l['price_level'], signal['instrument'])} ✅"
                                    for l in hit_limits[:5]])
                if len(hit_limits) > 5:
                    hit_str += f"\n... +{len(hit_limits) - 5} more"
                embed.add_field(name=f"Hit Limits ({len(hit_limits)})", value=hit_str, inline=False)

        # Progress
        embed.add_field(
            name="Progress",
            value=f"{signal.get('limits_hit', 0)}/{signal.get('total_limits', 0)} limits hit",
            inline=True
        )

        # Timestamps
        if signal.get('first_limit_hit_time'):
            embed.add_field(name="First Hit", value=f"<t:{int(signal['first_limit_hit_time'].timestamp())}:R>", inline=True)

        if signal.get('closed_at'):
            embed.add_field(name="Closed", value=f"<t:{int(signal['closed_at'].timestamp())}:R>", inline=True)

        # Expiry
        if signal.get('expiry_type'):
            embed.add_field(name="Expiry Type", value=signal['expiry_type'].replace('_', ' ').title(), inline=True)

        # Link to original message
        if not str(signal['message_id']).startswith("manual_"):
            message_url = f"https://discord.com/channels/{ctx.guild.id}/{signal['channel_id']}/{signal['message_id']}"
            embed.add_field(name="Source", value=f"[Jump to message]({message_url})", inline=False)
        else:
            embed.add_field(name="Source", value="Manual Entry", inline=False)

        embed.set_footer(text=f"Created {signal['created_at']}")

        await ctx.send(embed=embed)

    @commands.command(name='test_signal')
    async def test_signal(self, ctx: commands.Context, *, signal_text: str = None):
        """Test signal parsing with custom or sample text"""
        if not signal_text:
            signal_text = "1.34850—–1.34922——1.35035 gbpusd short vth Stops 1.35132"

        channel_name = self.get_channel_name(ctx.channel.id)

        parsed = parse_signal(signal_text, channel_name)

        embed = discord.Embed(
            title="🧪 Signal Parse Test",
            color=discord.Color.green() if parsed else discord.Color.red()
        )

        embed.add_field(
            name="Input",
            value=f"```{signal_text[:200]}```",
            inline=False
        )

        if channel_name:
            embed.add_field(name="Channel", value=channel_name, inline=True)

        if parsed:
            embed.add_field(name="✅ Parse Success", value=f"**Method:** {parsed.parse_method}", inline=False)
            embed.add_field(name="Instrument", value=parsed.instrument, inline=True)
            embed.add_field(name="Direction", value=parsed.direction.upper(), inline=True)

            # FIXED: Better formatting
            stop_formatted = format_price(parsed.stop_loss, parsed.instrument)
            embed.add_field(name="Stop Loss", value=stop_formatted, inline=True)

            # FIXED: Better limit formatting
            limits_formatted = "\n".join([f"• {format_price(limit, parsed.instrument)}" for limit in parsed.limits])
            embed.add_field(
                name="Limits",
                value=limits_formatted,
                inline=True
            )
            embed.add_field(name="Expiry", value=parsed.expiry_type, inline=True)
            if parsed.keywords:
                embed.add_field(name="Keywords", value=", ".join(parsed.keywords), inline=True)
        else:
            embed.add_field(name="❌ Parse Failed", value="Could not extract valid signal from text", inline=False)

        await ctx.send(embed=embed)

    @commands.command(name='stats')
    async def show_stats(self, ctx: commands.Context):
        """Show detailed statistics about signals"""
        stats = await self.signal_db.get_statistics()

        embed = discord.Embed(
            title="📊 Signal Statistics",
            color=discord.Color.blue()
        )

        # Overall stats
        embed.add_field(name="Total Signals", value=stats.get('total_signals', 0), inline=True)
        embed.add_field(name="Currently Tracking", value=stats.get('tracking_count', 0), inline=True)
        embed.add_field(name="Overall Win Rate", value=f"{stats.get('overall', {}).get('win_rate', 0)}%", inline=True)

        # Status breakdown
        if stats.get('by_status'):
            status_str = "\n".join([f"• {status.title()}: {count}" for status, count in stats['by_status'].items()])
            embed.add_field(name="By Status", value=status_str, inline=False)

        # Today's performance
        if stats.get('today'):
            today = stats['today']
            embed.add_field(
                name="Today's Performance",
                value=f"Total: {today.get('total_trades', 0)} | "
                      f"Profit: {today.get('profitable', 0)} | "
                      f"BE: {today.get('breakeven', 0)} | "
                      f"SL: {today.get('stop_loss', 0)}",
                inline=False
            )

        # By instrument
        if stats.get('by_instrument'):
            inst_str = "\n".join([f"• {inst['instrument']}: {inst['wins']}/{inst['total']} "
                                 f"({round(inst['wins']/inst['total']*100, 1)}%)"
                                 for inst in stats['by_instrument'][:5]])
            embed.add_field(name="Top Instruments", value=inst_str or "No data", inline=False)

        embed.set_footer(text=f"Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    # Include the rest of the commands from the original file
    # (setexpiry, setstatus, profit, breakeven, hit, stoploss, cancel, report)
    # These don't need changes for streaming architecture

    @commands.command(name='setexpiry')
    async def set_expiry(self, ctx, signal_id: str = None, *expiry_args):
        """
        Manually set the expiry for a signal

        Usage:
            !setexpiry <signal_id> <expiry_type>
            !setexpiry <signal_id> <YYYY-MM-DD> <HH:MM>

        Expiry types: day_end, week_end, month_end, no_expiry
        Custom format: YYYY-MM-DD HH:MM (in EST timezone)

        Examples:
            !setexpiry 123 week_end
            !setexpiry 456 no_expiry
            !setexpiry 789 2025-08-13 13:30
        """
        # Import here to avoid circular dependencies
        from database import signal_db
        from datetime import datetime
        import pytz
        import re

        # Validate arguments
        if not signal_id or not expiry_args:
            embed = discord.Embed(
                title="❌ Invalid Usage",
                description=(
                    "**Usage:** `!setexpiry <signal_id> <expiry_type>`\n"
                    "**OR:** `!setexpiry <signal_id> <YYYY-MM-DD> <HH:MM>`\n\n"
                    "**Valid expiry types:**\n"
                    "• `day_end` - End of current trading day (5 PM EST)\n"
                    "• `week_end` - End of trading week (Friday 5 PM EST)\n"
                    "• `month_end` - Last trading day of month\n"
                    "• `no_expiry` - Remove expiry\n\n"
                    "**Custom format:**\n"
                    "• Date: `YYYY-MM-DD` (e.g., 2025-08-13)\n"
                    "• Time: `HH:MM` in 24-hour format (e.g., 13:30 for 1:30 PM)\n"
                    "• Timezone: EST/EDT automatically applied\n\n"
                    "**Examples:**\n"
                    "`!setexpiry 123 week_end`\n"
                    "`!setexpiry 456 2025-08-13 15:30`"
                ),
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Validate signal ID is numeric
        try:
            sig_id = int(signal_id)
        except ValueError:
            embed = discord.Embed(
                title="❌ Invalid Signal ID",
                description=f"Signal ID must be a number, got: `{signal_id}`",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Determine if it's a predefined type or custom datetime
        expiry_type = None
        custom_datetime_iso = None

        if len(expiry_args) == 1:
            # Predefined expiry type
            expiry_type = expiry_args[0].lower()
            valid_types = ['day_end', 'week_end', 'month_end', 'no_expiry']

            if expiry_type not in valid_types:
                embed = discord.Embed(
                    title="❌ Invalid Expiry Type",
                    description=(
                        f"Invalid expiry type: `{expiry_type}`\n\n"
                        "**Valid types:** day_end, week_end, month_end, no_expiry\n\n"
                        "For custom datetime, use: `!setexpiry <id> YYYY-MM-DD HH:MM`"
                    ),
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return

        elif len(expiry_args) == 2:
            # Custom datetime format
            date_str = expiry_args[0]
            time_str = expiry_args[1]

            # Validate date format (YYYY-MM-DD)
            date_pattern = r'^\d{4}-\d{2}-\d{2}$'
            if not re.match(date_pattern, date_str):
                embed = discord.Embed(
                    title="❌ Invalid Date Format",
                    description=(
                        f"Invalid date format: `{date_str}`\n\n"
                        "**Expected format:** `YYYY-MM-DD`\n"
                        "**Example:** `2025-08-13`"
                    ),
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return

            # Validate time format (HH:MM)
            time_pattern = r'^\d{1,2}:\d{2}$'
            if not re.match(time_pattern, time_str):
                embed = discord.Embed(
                    title="❌ Invalid Time Format",
                    description=(
                        f"Invalid time format: `{time_str}`\n\n"
                        "**Expected format:** `HH:MM` (24-hour)\n"
                        "**Examples:** `13:30`, `09:00`, `23:59`"
                    ),
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return

            # Parse and validate the datetime
            try:
                # Combine date and time
                datetime_str = f"{date_str} {time_str}"

                # Parse as EST/EDT timezone
                est = pytz.timezone('America/New_York')
                expiry_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                expiry_dt = est.localize(expiry_dt)

                # Check if datetime is in the future
                now = datetime.now(est)
                if expiry_dt <= now:
                    embed = discord.Embed(
                        title="❌ Invalid Expiry Time",
                        description=(
                            f"Expiry time must be in the future!\n\n"
                            f"**Provided:** {expiry_dt.strftime('%b %d, %Y at %I:%M %p EST')}\n"
                            f"**Current:** {now.strftime('%b %d, %Y at %I:%M %p EST')}"
                        ),
                        color=discord.Color.red()
                    )
                    await ctx.send(embed=embed)
                    return

                # Convert to UTC for storage (ISO format)
                expiry_utc = expiry_dt.astimezone(pytz.UTC)
                custom_datetime_iso = expiry_utc.isoformat()
                expiry_type = 'custom'

            except ValueError as e:
                embed = discord.Embed(
                    title="❌ Invalid DateTime",
                    description=(
                        f"Could not parse datetime: `{datetime_str}`\n\n"
                        f"Error: {str(e)}\n\n"
                        "**Format:** `YYYY-MM-DD HH:MM`\n"
                        "**Example:** `2025-08-13 13:30`"
                    ),
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
        else:
            embed = discord.Embed(
                title="❌ Invalid Arguments",
                description=(
                    "**Expected formats:**\n"
                    "• `!setexpiry <id> <expiry_type>`\n"
                    "• `!setexpiry <id> <YYYY-MM-DD> <HH:MM>`\n\n"
                    f"**Received:** {len(expiry_args)} arguments"
                ),
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Check if signal exists
        signal = await signal_db.get_signal_with_limits(sig_id)
        if not signal:
            embed = discord.Embed(
                title="❌ Signal Not Found",
                description=f"No signal found with ID: `{sig_id}`",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Check if signal is in final status
        from database.models import SignalStatus
        if SignalStatus.is_final(signal['status']):
            embed = discord.Embed(
                title="❌ Cannot Modify Expiry",
                description=(
                    f"Cannot change expiry for signal `{sig_id}`\n"
                    f"Signal is in final status: **{signal['status'].upper()}**"
                ),
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return

        # Store old expiry for display
        old_expiry = signal.get('expiry_type', 'none')
        old_expiry_display = old_expiry if old_expiry else 'none'

        # Attempt to update expiry
        try:
            import asyncio

            # Add timeout protection
            success = await asyncio.wait_for(
                signal_db.manually_set_signal_expiry(sig_id, expiry_type, custom_datetime_iso),
                timeout=5.0
            )

            if success:
                # Get updated signal for display
                updated_signal = await signal_db.get_signal_with_limits(sig_id)

                # Format expiry time for display
                expiry_display = "No expiry"
                if expiry_type == 'custom':
                    # Parse the custom datetime for display
                    expiry_dt = datetime.fromisoformat(custom_datetime_iso)
                    est = pytz.timezone('America/New_York')
                    expiry_est = expiry_dt.astimezone(est)
                    expiry_display = expiry_est.strftime('%b %d, %Y at %I:%M %p EST')
                    expiry_type_display = f"custom ({expiry_args[0]} {expiry_args[1]})"
                elif expiry_type != 'no_expiry' and updated_signal.get('expiry_time'):
                    try:
                        expiry_dt = datetime.fromisoformat(updated_signal['expiry_time'])
                        if expiry_dt.tzinfo is None:
                            expiry_dt = pytz.UTC.localize(expiry_dt)
                        est = pytz.timezone('America/New_York')
                        expiry_est = expiry_dt.astimezone(est)
                        expiry_display = expiry_est.strftime('%b %d, %Y at %I:%M %p EST')
                    except:
                        expiry_display = updated_signal.get('expiry_time', 'Unknown')
                    expiry_type_display = expiry_type
                else:
                    expiry_type_display = expiry_type

                # Create success embed
                embed = discord.Embed(
                    title="✅ Expiry Updated",
                    color=discord.Color.green()
                )

                embed.add_field(
                    name="Signal",
                    value=f"ID: `{sig_id}`\n{updated_signal['instrument']} {updated_signal['direction'].upper()}",
                    inline=True
                )

                embed.add_field(
                    name="Expiry Change",
                    value=f"**From:** {old_expiry_display}\n**To:** {expiry_type_display}",
                    inline=True
                )

                embed.add_field(
                    name="New Expiry Time",
                    value=expiry_display,
                    inline=False
                )

                # Add time remaining if applicable
                if expiry_type != 'no_expiry':
                    now = datetime.now(pytz.UTC)
                    expiry_dt = datetime.fromisoformat(updated_signal['expiry_time']) if updated_signal.get(
                        'expiry_time') else None
                    if expiry_dt:
                        if expiry_dt.tzinfo is None:
                            expiry_dt = pytz.UTC.localize(expiry_dt)
                        remaining = expiry_dt - now
                        if remaining.total_seconds() > 0:
                            hours = int(remaining.total_seconds() // 3600)
                            minutes = int((remaining.total_seconds() % 3600) // 60)
                            embed.add_field(
                                name="Time Remaining",
                                value=f"{hours}h {minutes}m",
                                inline=True
                            )

                embed.set_footer(text=f"Updated by {ctx.author.name}")
                embed.timestamp = ctx.message.created_at

                await ctx.send(embed=embed)

                # Log the action
                if expiry_type == 'custom':
                    logger.info(f"User {ctx.author} set custom expiry for signal {sig_id}: {expiry_display}")
                else:
                    logger.info(
                        f"User {ctx.author} changed expiry for signal {sig_id} from {old_expiry} to {expiry_type}")

            else:
                embed = discord.Embed(
                    title="❌ Failed to Update Expiry",
                    description="Could not update the signal expiry. Please check the logs.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)

        except asyncio.TimeoutError:
            embed = discord.Embed(
                title="⏱️ Operation Timed Out",
                description="The expiry update operation took too long. Please try again.",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed)
            logger.error(f"Timeout updating expiry for signal {sig_id}")

        except Exception as e:
            logger.error(f"Error in setexpiry command: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred while updating expiry: {str(e)}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

    @commands.command(name='setstatus')
    async def set_signal_status(self, ctx: commands.Context, signal_id: int = None, status: str = None):
        """
        Manually set a signal's status

        Usage: !setstatus <signal_id> <status>
        Valid statuses: active, hit, profit, breakeven, stop_loss, cancelled, cancel
        """
        if not signal_id or not status:
            await ctx.reply(
                "Usage: `!setstatus <signal_id> <status>`\n"
                "Valid statuses: active, hit, profit, breakeven, stop_loss, cancelled, cancel"
            )
            return

        # Validate status
        valid_statuses = ['active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled', 'cancel']
        status = status.lower()
        if status == "cancel":
            status = "cancelled"

        if status not in valid_statuses:
            await ctx.reply(f"❌ Invalid status. Valid options: {', '.join(valid_statuses)}")
            return

        # Check if signal exists
        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.reply(f"❌ Signal #{signal_id} not found.")
            return

        # Set the status with timeout
        import asyncio
        try:
            success = await asyncio.wait_for(
                self.signal_db.manually_set_signal_status(
                    signal_id,
                    status,
                    f"Manual override by {ctx.author.name}"
                ),
                timeout=5.0
            )

            if success:
                status_emoji = {
                    'active': '🟢',
                    'hit': '🎯',
                    'profit': '💰',
                    'breakeven': '➖',
                    'stop_loss': '🛑',
                    'cancelled': '❌'
                }

                embed = discord.Embed(
                    title=f"{status_emoji.get(status, '❓')} Status Updated",
                    description=f"Signal #{signal_id} status changed to **{status.upper()}**",
                    color=discord.Color.green()
                )
                embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
                embed.add_field(name="Previous Status", value=signal['status'], inline=True)
                embed.set_footer(text=f"Changed by {ctx.author.name}")

                await ctx.send(embed=embed)
            else:
                await ctx.reply(f"❌ Failed to update signal status.")

        except asyncio.TimeoutError:
            await ctx.reply("❌ Operation timed out. Please try again.")
            self.logger.error(f"Timeout setting status for signal {signal_id}")
        except Exception as e:
            await ctx.reply(f"❌ Error updating status: {str(e)}")
            self.logger.error(f"Error in setstatus command: {e}", exc_info=True)

    # Helper to make short commands reuse set_signal_status
    async def _quick_status(self, ctx: commands.Context, signal_id: int, status: str):
        await self.set_signal_status(ctx, signal_id, status)

    # Shortcut commands
    @commands.command(name='profit', aliases=['tp'])
    async def set_profit(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "profit")

    @commands.command(name='breakeven', aliases=['be'])
    async def set_breakeven(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "breakeven")

    @commands.command(name='hit')
    async def set_hit(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "hit")

    @commands.command(name='stoploss', aliases=['sl'])
    async def set_stop_loss(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "stop_loss")

    @commands.command(name='cancel')
    async def set_cancelled(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "cancelled")

    @commands.command(name='report')
    async def report(self, ctx: commands.Context, period: str = 'week'):
        """
        Show trading performance report for the current week or month

        Usage:
            !report - Show current week's performance (default)
            !report week - Show current week's performance
            !report month - Show current month's performance

        Note: Trading week starts Sunday 6:00 PM UTC
        """

        # Validate period
        period = period.lower()
        if period not in ['week', 'month']:
            await ctx.send("❌ Invalid period. Use `!report week` or `!report month`")
            return

        # Show loading message
        loading_msg = await ctx.send("📊 Generating report...")

        try:
            # Get date range for the period
            date_range = await self.signal_db.get_trading_period_range(period)
            start_date = date_range['start']
            end_date = date_range['end']

            # Get signals with results for the period
            signals = await self.signal_db.get_period_signals_with_results(start_date, end_date)

            if not signals:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"No completed trades found for the current {period}",
                    color=0xFFA500
                )
                embed.add_field(
                    name="📅 Date Range",
                    value=f"{date_range['display_start']} - {date_range['display_end']}",
                    inline=False
                )
                embed.set_footer(text="Start trading to see your performance!")
                await loading_msg.edit(content=None, embed=embed)
                return

            # Organize signals by status (excluding breakeven from display)
            profit_signals = []
            stoploss_signals = []

            # Group signals by status
            for signal in signals:
                if signal['status'] == SignalStatus.PROFIT:
                    profit_signals.append(signal)
                elif signal['status'] == SignalStatus.STOP_LOSS:
                    stoploss_signals.append(signal)
                # Note: We're ignoring BREAKEVEN signals for display but they could still exist in the data

            # Calculate statistics (only counting profit and stop loss for win rate)
            total_signals = len([s for s in signals if s['status'] in [SignalStatus.PROFIT, SignalStatus.STOP_LOSS]])
            profit_count = len(profit_signals)
            stoploss_count = len(stoploss_signals)

            # Calculate win rate (profit as wins, stop loss as losses)
            win_rate = (profit_count / total_signals * 100) if total_signals > 0 else 0

            # Calculate percentages
            profit_pct = (profit_count / total_signals * 100) if total_signals > 0 else 0
            stoploss_pct = (stoploss_count / total_signals * 100) if total_signals > 0 else 0

            # Create embed
            embed = discord.Embed(
                title=f"📊 {period.title()} Trading Report",
                description=f"Performance summary for the current {period}",
                color=0x00FF00 if win_rate >= 50 else 0xFF0000
            )

            # Add date range
            embed.add_field(
                name="📅 Date Range",
                value=f"{date_range['display_start']} - {date_range['display_end']}",
                inline=False
            )

            # Add summary statistics (removed breakeven)
            embed.add_field(
                name="📈 Overview",
                value=f"**Total Signals:** {total_signals}\n"
                      f"**Win Rate:** {win_rate:.1f}%\n"
                      f"**Profit:** {profit_count} ({profit_pct:.1f}%)\n"
                      f"**Stop Loss:** {stoploss_count} ({stoploss_pct:.1f}%)",
                inline=False
            )

            # Build signal lines for profit trades
            if profit_signals:
                profit_lines = []
                for signal in profit_signals:
                    # Get first limit if available
                    first_limit = ""
                    if signal.get('limits') and len(signal['limits']) > 0:
                        first_limit = f" | {signal['limits'][0]}"

                    line = f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{first_limit} 🟢"
                    profit_lines.append(line)

                # Join all lines and check if we need pagination
                profit_text = "\n".join(profit_lines)

                # Discord embed field value limit is 1024 characters
                # If it exceeds, we'll truncate and show count
                if len(profit_text) > 1024:
                    # Calculate how many we can fit (roughly 50 chars per line)
                    max_lines = 1024 // 50
                    truncated_lines = profit_lines[:max_lines]
                    profit_text = "\n".join(truncated_lines)
                    profit_text += f"\n... and {len(profit_lines) - max_lines} more"

                embed.add_field(
                    name=f"💰 Profited Trades ({profit_count})",
                    value=profit_text,
                    inline=False
                )

            # Build signal lines for stop loss trades
            if stoploss_signals:
                sl_lines = []
                for signal in stoploss_signals:
                    # Get stop loss value if available
                    sl_value = ""
                    if signal.get('stop_loss'):
                        sl_value = f" | {signal['stop_loss']}"

                    line = f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{sl_value} 🛑"
                    sl_lines.append(line)

                # Join all lines
                sl_text = "\n".join(sl_lines)

                # Check field limit
                if len(sl_text) > 1024:
                    max_lines = 1024 // 50
                    truncated_lines = sl_lines[:max_lines]
                    sl_text = "\n".join(truncated_lines)
                    sl_text += f"\n... and {len(sl_lines) - max_lines} more"

                embed.add_field(
                    name=f"🛑 Stop Loss Trades ({stoploss_count})",
                    value=sl_text,
                    inline=False
                )

            # Check total embed size (6000 character limit)
            # If we're approaching the limit, we need to implement pagination
            total_chars = len(embed.title or '') + len(embed.description or '')
            for field in embed.fields:
                total_chars += len(field.name) + len(field.value)

            if total_chars > 5500:  # Leave some buffer
                # If we hit the limit, we need to use pagination
                # For now, let's truncate more aggressively

                # Rebuild embed with fewer entries
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"Performance summary for the current {period}",
                    color=0x00FF00 if win_rate >= 50 else 0xFF0000
                )

                embed.add_field(
                    name="📅 Date Range",
                    value=f"{date_range['display_start']} - {date_range['display_end']}",
                    inline=False
                )

                embed.add_field(
                    name="📈 Overview",
                    value=f"**Total Signals:** {total_signals}\n"
                          f"**Win Rate:** {win_rate:.1f}%\n"
                          f"**Profit:** {profit_count} ({profit_pct:.1f}%)\n"
                          f"**Stop Loss:** {stoploss_count} ({stoploss_pct:.1f}%)",
                    inline=False
                )

                # Show limited number of trades
                if profit_signals:
                    max_profit_show = min(15, len(profit_signals))
                    profit_lines = []
                    for signal in profit_signals[:max_profit_show]:
                        first_limit = ""
                        if signal.get('limits') and len(signal['limits']) > 0:
                            first_limit = f" | {signal['limits'][0]}"
                        line = f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{first_limit} 💰"
                        profit_lines.append(line)

                    profit_text = "\n".join(profit_lines)
                    if len(profit_signals) > max_profit_show:
                        profit_text += f"\n... and {len(profit_signals) - max_profit_show} more"

                    embed.add_field(
                        name=f"💰 Profited Trades ({profit_count})",
                        value=profit_text,
                        inline=False
                    )

                if stoploss_signals:
                    max_sl_show = min(15, len(stoploss_signals))
                    sl_lines = []
                    for signal in stoploss_signals[:max_sl_show]:
                        sl_value = ""
                        if signal.get('stop_loss'):
                            sl_value = f" | {signal['stop_loss']}"
                        line = f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{sl_value} 🛑"
                        sl_lines.append(line)

                    sl_text = "\n".join(sl_lines)
                    if len(stoploss_signals) > max_sl_show:
                        sl_text += f"\n... and {len(stoploss_signals) - max_sl_show} more"

                    embed.add_field(
                        name=f"🛑 Stop Loss Trades ({stoploss_count})",
                        value=sl_text,
                        inline=False
                    )

                # Add note about pagination if needed
                if len(profit_signals) > 15 or len(stoploss_signals) > 15:
                    embed.set_footer(
                        text=f"Showing limited results due to size constraints | Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                    )
                else:
                    embed.set_footer(
                        text=f"Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                    )
            else:
                embed.set_footer(
                    text=f"Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                )

            await loading_msg.edit(content=None, embed=embed)

        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error Generating Report",
                description=f"An error occurred while generating the report: {str(e)}",
                color=0xFF0000
            )
            await loading_msg.edit(content=None, embed=error_embed)

            # Log the error
            import traceback
            from utils.logger import get_logger
            logger = get_logger("signal_commands")
            logger.error(f"Error in report command: {e}\n{traceback.format_exc()}")


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(SignalCommands(bot))