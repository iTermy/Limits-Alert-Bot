"""
Signal Commands - Simplified version
Removed excessive logging, defensive programming, and consolidated repeated code
"""
from database.models import SignalStatus
from .base_command import BaseCog
from utils.embed_factory import EmbedFactory
from core.parser import parse_signal
from utils.logger import get_logger
from utils.formatting import (
    format_price, format_distance_display, is_crypto_symbol,
    is_index_symbol, get_status_emoji
)
import discord
from discord.ext import commands
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime

logger = get_logger("signal_commands")


class ActiveSignalsView(discord.ui.View):
    """Pagination view for active signals"""

    def __init__(self, signals: List[Dict], guild_id: int,
                 instrument: Optional[str], page_size: int = 10, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.signals = signals
        self.guild_id = guild_id
        self.instrument = instrument
        self.page_size = page_size
        self.current_page = 0
        self.max_page = (len(signals) - 1) // page_size if signals else 0
        self.update_buttons()

    def update_buttons(self):
        """Update button states based on current page"""
        self.previous_button.disabled = self.current_page <= 0
        self.next_button.disabled = self.current_page >= self.max_page
        self.page_label.label = f"Page {self.current_page + 1}/{self.max_page + 1}"

    def get_page_embed(self) -> discord.Embed:
        """Get embed for current page"""
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.signals))
        page_signals = self.signals[start_idx:end_idx]

        return self.create_active_signals_embed(
            page_signals,
            self.guild_id,
            self.instrument,
            page_info=(self.current_page + 1, self.max_page + 1, len(self.signals))
        )

    def create_active_signals_embed(self, signals: List[Dict], guild_id: int,
                                    instrument: Optional[str], page_info: tuple) -> discord.Embed:
        """Create embed for active signals with pagination info"""
        current_page, total_pages, total_signals = page_info

        if not signals and current_page == 1:
            return discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500
            )

        embed = discord.Embed(
            title="📊 Active Signals",
            description=f"Showing page {current_page}/{total_pages} ({total_signals} total signals)" +
                        (f" for {instrument}" if instrument else ""),
            color=0x00BFFF
        )

        for signal in signals:
            status_emoji = get_status_emoji(signal.get('status', 'active'))

            # Format limits - show ALL limits
            pending_limits = signal.get('pending_limits', [])
            hit_limits = signal.get('hit_limits', [])

            if pending_limits:
                limits_str = ", ".join([format_price(p, signal['instrument']) for p in pending_limits])
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
            if signal.get('distance_info') and signal.get('status', 'active').lower() in ['active', 'hit']:
                distance_info = signal['distance_info']
                is_crypto = signal.get('is_crypto', False)
                is_index = signal.get('is_index', False)

                if is_crypto or is_index:
                    distance_dollars = abs(distance_info.get('distance', 0))
                    if distance_dollars > 0 and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** ${distance_dollars:.2f} away"
                else:
                    formatted_distance = distance_info.get('formatted', '')
                    if formatted_distance and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** {formatted_distance}"

            # Add expiry time
            if signal.get('time_remaining'):
                field_value += f"\n**Expiry:** {signal['time_remaining']}"

            # Add source
            field_value += f"\n**Source:** {link_label}"

            embed.add_field(
                name=f"{status_emoji} #{signal['id']} - {signal['instrument']} - {signal['direction'].upper()}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Total: {total_signals} signals | Use buttons to navigate")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary, custom_id="previous")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page"""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

    @discord.ui.button(label="Page 1/1", style=discord.ButtonStyle.secondary, custom_id="page_label", disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Page indicator (disabled button)"""
        pass

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page"""
        if self.current_page < self.max_page:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)


class SignalCommands(BaseCog):
    """Signal management commands"""

    def __init__(self, bot):
        super().__init__(bot)

    @commands.command(name="signal")
    async def add_signal(
        self,
        ctx: commands.Context,
        instrument: str,
        direction: str,
        entry: float,
        stop_loss: float,
        limit1: float,
        limit2: Optional[float] = None,
        limit3: Optional[float] = None,
        limit4: Optional[float] = None
    ):
        """Add a new signal manually"""
        limits = [limit1]
        if limit2:
            limits.append(limit2)
        if limit3:
            limits.append(limit3)
        if limit4:
            limits.append(limit4)

        signal_data = {
            'instrument': instrument.upper(),
            'direction': direction.lower(),
            'entry': entry,
            'stop_loss': stop_loss,
            'limits': limits
        }

        signal_id = await self.signal_db.save_signal(
            signal_data,
            ctx.guild.id,
            ctx.channel.id,
            f"manual_{ctx.author.id}_{int(datetime.utcnow().timestamp())}"
        )

        embed = discord.Embed(
            title="✅ Signal Added",
            description=f"Signal #{signal_id} created successfully",
            color=0x00FF00
        )
        embed.add_field(name="Instrument", value=instrument.upper(), inline=True)
        embed.add_field(name="Direction", value=direction.upper(), inline=True)
        embed.add_field(name="Entry", value=str(entry), inline=True)
        embed.add_field(name="Stop Loss", value=str(stop_loss), inline=True)
        embed.add_field(name="Limits", value=", ".join(map(str, limits)), inline=False)

        await ctx.send(embed=embed)

    @commands.command(name='active')
    async def active_signals(
        self,
        ctx: commands.Context,
        instrument: Optional[str] = None
    ):
        """Display active trading signals with pagination"""
        loading_msg = await ctx.send("🔄 Loading active signals...")

        signals = await self.signal_db.get_active_signals_detailed(
            instrument.upper() if instrument else None
        )

        if not signals:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500
            )
            await loading_msg.edit(content=None, embed=embed)
            return

        # Add asset type flags and calculate distances
        for signal in signals:
            signal['is_crypto'] = is_crypto_symbol(signal['instrument'])
            signal['is_index'] = is_index_symbol(signal['instrument'])

            # Calculate distance to next limit
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                if hasattr(self.bot.monitor, 'stream_manager') and signal.get('pending_limits'):
                    try:
                        from price_feeds.alert_config import AlertDistanceConfig
                        alert_config = AlertDistanceConfig()

                        symbol = signal['instrument']
                        cached_price = await self.bot.monitor.stream_manager.get_latest_price(symbol)

                        if cached_price:
                            direction = signal['direction'].lower()
                            current_price = cached_price['ask'] if direction == 'long' else cached_price['bid']
                            limit_price = signal['pending_limits'][0]

                            # Calculate raw price distance
                            if direction == 'long':
                                distance = current_price - limit_price
                            else:
                                distance = limit_price - current_price

                            # Format based on asset type
                            if signal['is_crypto'] or signal['is_index']:
                                # For crypto and indices, show dollar distance
                                distance_value = abs(distance)
                                formatted = f"${distance_value:.2f} away"
                            else:
                                # For forex, use the format_distance_for_display which handles pip conversion
                                formatted = alert_config.format_distance_for_display(symbol, abs(distance), current_price)
                                # Extract pip value for sorting
                                pip_size = alert_config.get_pip_size(symbol)
                                distance_value = abs(distance) / pip_size

                            signal['distance_info'] = {
                                'distance': distance_value,
                                'current_price': current_price,
                                'formatted': formatted
                            }
                    except Exception as e:
                        logger.warning(f"Could not get price for {symbol}: {e}")

        # Create pagination view
        view = ActiveSignalsView(
            signals=signals,
            guild_id=ctx.guild.id,
            instrument=instrument.upper() if instrument else None
        )

        await loading_msg.edit(content=None, embed=view.get_page_embed(), view=view)

    @commands.command(name="delete")
    async def delete_signal(self, ctx: commands.Context, signal_id: int):
        """Delete a signal permanently"""
        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        from database import db
        async with db.get_connection() as conn:
            await conn.execute("DELETE FROM signals WHERE id = ?", (signal_id,))
            await conn.commit()

        embed = discord.Embed(
            title="🗑️ Signal Deleted",
            description=f"Signal #{signal_id} has been deleted",
            color=0xFFA500
        )
        embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
        embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
        embed.add_field(name="Status", value=signal['status'], inline=True)
        embed.set_footer(text=f"Deleted by {ctx.author.name}")

        await ctx.send(embed=embed)

    @commands.command(name="info")
    async def signal_info(self, ctx: commands.Context, signal_id: int):
        """Show detailed information about a signal"""
        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        status_emoji = get_status_emoji(signal['status'])

        embed = discord.Embed(
            title=f"{status_emoji} Signal #{signal_id} - {signal['instrument']}",
            color=0x00BFFF
        )

        embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
        embed.add_field(name="Status", value=signal['status'].upper(), inline=True)

        stop_loss_formatted = format_price(signal['stop_loss'], signal['instrument']) if signal['stop_loss'] else "N/A"
        embed.add_field(name="Stop Loss", value=stop_loss_formatted, inline=True)

        # Streaming status
        if hasattr(self.bot, 'monitor') and self.bot.monitor:
            if hasattr(self.bot.monitor, 'stream_manager'):
                is_subscribed = signal['instrument'] in self.bot.monitor.stream_manager.subscribed_symbols
                embed.add_field(
                    name="Streaming Status",
                    value="🟢 Subscribed" if is_subscribed else "⚪ Not Subscribed",
                    inline=True
                )

        # Limits info
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

        # Link to original message
        if not str(signal['message_id']).startswith("manual_"):
            message_url = f"https://discord.com/channels/{ctx.guild.id}/{signal['channel_id']}/{signal['message_id']}"
            embed.add_field(name="Source", value=f"[Jump to message]({message_url})", inline=False)
        else:
            embed.add_field(name="Source", value="Manual Entry", inline=False)

        embed.set_footer(text=f"Created {signal['created_at']}")

        await ctx.send(embed=embed)

    @commands.command(name="setstatus", description="Set signal status")
    async def set_signal_status(self, ctx: commands.Context, signal_id: int, status: str):
        """Manually set a signal's status"""
        valid_statuses = ['active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled', 'cancel']
        status = status.lower()

        if status == "cancel":
            status = "cancelled"

        if status not in valid_statuses:
            await ctx.send(f"❌ Invalid status. Valid options: {', '.join(valid_statuses)}")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        success = await self.signal_db.manually_set_signal_status(
            signal_id,
            status,
            f"Manual override by {ctx.author.name}"
        )

        if success:
            status_emoji = get_status_emoji(status)

            embed = discord.Embed(
                title=f"{status_emoji} Status Updated",
                description=f"Signal #{signal_id} status changed to **{status.upper()}**",
                color=0x00FF00
            )
            embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
            embed.add_field(name="Previous Status", value=signal['status'], inline=True)
            embed.set_footer(text=f"Changed by {ctx.author.name}")

            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update signal status")

    # Shortcut commands for status changes
    @commands.command(name="profit", aliases=["tp"], description="Mark signal as profit")
    async def set_profit(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "profit")

    @commands.command(name="hit", description="Mark signal as hit")
    async def set_hit(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "hit")

    @commands.command(name="stoploss", aliases=["sl"], description="Mark signal as stop loss")
    async def set_stop_loss(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "stop_loss")

    @commands.command(name="cancel", aliases=["nm", "cancle"], description="Cancel a signal")
    async def set_cancelled(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "cancelled")

    @commands.command(name="setexpiry", description="Set signal expiry")
    async def set_expiry(self, ctx: commands.Context, signal_id: int, expiry_type: str):
        """
        Set signal expiry
        Valid types: day_end, week_end, month_end, no_expiry
        """
        from database import signal_db

        valid_types = ['day_end', 'week_end', 'month_end', 'no_expiry']

        if expiry_type.lower() not in valid_types:
            await ctx.send(f"❌ Invalid expiry type. Valid options: {', '.join(valid_types)}")
            return

        signal = await signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        success = await signal_db.set_signal_expiry(signal_id, expiry_type.lower())

        if success:
            embed = discord.Embed(
                title="⏰ Expiry Updated",
                description=f"Signal #{signal_id} expiry set to **{expiry_type}**",
                color=0x00FF00
            )
            embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update expiry")

    @commands.command(name="report", description="Generate trading report")
    async def generate_report(
        self,
        ctx: commands.Context,
        period: str = "week"
    ):
        """Generate a trading report for specified period"""
        if period.lower() not in ['day', 'week', 'month']:
            await ctx.send("❌ Period must be 'day', 'week', or 'month'")
            return

        loading_msg = await ctx.send(f"📊 Generating {period} report...")

        try:
            date_range = await self.signal_db.get_trading_period_range(period)
            start_date = date_range['start']
            end_date = date_range['end']

            signals = await self.signal_db.get_period_signals_with_results(
                start_date,
                end_date
            )

            if not signals:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"No signals found for the current {period}",
                    color=0xFFA500
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            # Calculate statistics - filter by status (use string comparison)
            profit_signals = [s for s in signals if s.get('status', '').lower() == 'profit']
            stoploss_signals = [s for s in signals if s.get('status', '').lower() in ['stoploss', 'stop_loss']]

            total_signals = len(signals)
            profit_count = len(profit_signals)
            stoploss_count = len(stoploss_signals)
            win_rate = (profit_count / total_signals * 100) if total_signals > 0 else 0

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
                      f"**Profit:** {profit_count} ({profit_count/total_signals*100:.1f}%)\n"
                      f"**Stop Loss:** {stoploss_count} ({stoploss_count/total_signals*100:.1f}%)",
                inline=False
            )

            # Build profit trades list
            if profit_signals:
                profit_lines = []
                for signal in profit_signals[:15]:
                    first_limit = f" | {signal['limits'][0]}" if signal.get('limits') else ""
                    profit_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{first_limit} 🟢"
                    )

                profit_text = "\n".join(profit_lines)
                if len(profit_signals) > 15:
                    profit_text += f"\n... and {len(profit_signals) - 15} more"

                embed.add_field(
                    name=f"💰 Profited Trades ({profit_count})",
                    value=profit_text,
                    inline=False
                )

            # Build stop loss trades list
            if stoploss_signals:
                sl_lines = []
                for signal in stoploss_signals[:15]:
                    sl_value = f" | {signal['stop_loss']}" if signal.get('stop_loss') else ""
                    sl_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | {signal['direction'].upper()}{sl_value} 🛑"
                    )

                sl_text = "\n".join(sl_lines)
                if len(stoploss_signals) > 15:
                    sl_text += f"\n... and {len(stoploss_signals) - 15} more"

                embed.add_field(
                    name=f"🛑 Stop Loss Trades ({stoploss_count})",
                    value=sl_text,
                    inline=False
                )

            embed.set_footer(
                text=f"Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
            )

            await loading_msg.edit(content=None, embed=embed)

        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error Generating Report",
                description=f"An error occurred: {str(e)}",
                color=0xFF0000
            )
            await loading_msg.edit(content=None, embed=error_embed)
            logger.error(f"Error in report command: {e}")


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(SignalCommands(bot))