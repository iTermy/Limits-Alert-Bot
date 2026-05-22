"""
Report Commands — trading performance reports.
"""

import json
from datetime import datetime
from pathlib import Path

import discord
from discord.ext import commands

from price_feeds.tp_config import TPConfig
from utils.formatting import format_price
from utils.logger import get_logger

from .._base import BaseCog

logger = get_logger("report_commands")


class ReportsCog(BaseCog):
    """Trading report generation"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()

    @commands.command(name="report", description="Generate trading report")
    async def generate_report(
        self, ctx: commands.Context, period: str = "week", filter_type: str = None
    ):
        """
        Generate a trading report for specified period

        Args:
            period: 'day', 'week', or 'month'
            filter_type: Optional - 'stoploss', 'sl', 'profit', 'win' to filter results
        """
        if period.lower() not in ["day", "week", "month"]:
            await ctx.send("❌ Period must be 'day', 'week', or 'month'")
            return

        # Normalize filter type
        filter_normalized = None
        if filter_type:
            filter_lower = filter_type.lower()
            if filter_lower in ["stoploss", "sl", "stop", "stop_loss"]:
                filter_normalized = "stoploss"
            elif filter_lower in ["profit", "win", "tp"]:
                filter_normalized = "profit"
            else:
                await ctx.send("❌ Filter must be 'stoploss'/'sl' or 'profit'/'win'")
                return

        # Update loading message based on filter
        if filter_normalized:
            loading_msg = await ctx.send(
                f"📊 Generating {period} report ({filter_normalized} only)..."
            )
        else:
            loading_msg = await ctx.send(f"📊 Generating {period} report...")

        def cap_field_value(lines: list, max_length: int = 1024) -> str:
            """
            Cap field value to max_length by truncating lines and adding summary.
            Discord embed fields have a 1024 character limit.
            """
            if not lines:
                return ""

            result_lines = []
            current_length = 0
            omitted_count = 0

            for line in lines:
                line_length = len(line) + 1  # +1 for newline
                if (
                    current_length + line_length > max_length - 50
                ):  # Reserve 50 chars for "... +X more"
                    omitted_count = len(lines) - len(result_lines)
                    break
                result_lines.append(line)
                current_length += line_length

            result = "\n".join(result_lines)
            if omitted_count > 0:
                result += f"\n... +{omitted_count} more signal{'s' if omitted_count > 1 else ''}"

            return result

        try:
            date_range = await self.signal_db.get_trading_period_range(period)
            start_date = date_range["start"]
            end_date = date_range["end"]

            signals = await self.signal_db.get_period_signals_with_results(start_date, end_date)

            if not signals:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"No signals found for the current {period}",
                    color=0xFFA500,
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            # Fetch full signal details with limits for each signal
            enriched_signals = []
            for signal in signals:
                full_signal = await self.signal_db.get_signal_with_limits(signal["id"])
                if full_signal:
                    # Merge the status and other info from period query
                    full_signal["status"] = signal["status"]
                    full_signal["channel_id"] = signal["channel_id"]
                    enriched_signals.append(full_signal)

            signals = enriched_signals

            # Load channels.json directly
            channels_file = (
                Path(__file__).resolve().parent.parent.parent / "config" / "channels.json"
            )
            try:
                with open(channels_file) as f:
                    channels_data = json.load(f)
                monitored_channels = channels_data.get("monitored_channels", {})
            except Exception as e:
                logger.warning(f"Could not load channels.json: {e}")
                monitored_channels = {}
                channels_data = {}

            # Create reverse mapping: channel_id -> channel_name
            channel_id_to_name = {
                str(channel_id): name for name, channel_id in monitored_channels.items()
            }

            # Separate signals into PA, toll, and regular based on channel
            pa_signals = []
            toll_signals = []
            regular_signals = []

            for signal in signals:
                channel_id = str(signal.get("channel_id", ""))
                channel_name = channel_id_to_name.get(channel_id, "").lower()

                if "toll" in channel_name:
                    toll_signals.append(signal)
                elif any(x in channel_name for x in ["pa", "price-action"]):
                    pa_signals.append(signal)
                else:
                    regular_signals.append(signal)

            # Process regular signals
            regular_profit = [s for s in regular_signals if s.get("status", "").lower() == "profit"]
            regular_stoploss = [
                s
                for s in regular_signals
                if s.get("status", "").lower() in ["stoploss", "stop_loss"]
            ]

            # Process PA signals
            pa_profit = [s for s in pa_signals if s.get("status", "").lower() == "profit"]
            pa_stoploss = [
                s for s in pa_signals if s.get("status", "").lower() in ["stoploss", "stop_loss"]
            ]

            # Process toll signals
            toll_profit = [s for s in toll_signals if s.get("status", "").lower() == "profit"]
            toll_stoploss = [
                s for s in toll_signals if s.get("status", "").lower() in ["stoploss", "stop_loss"]
            ]

            # Apply filter if specified
            if filter_normalized == "stoploss":
                regular_profit = []
                pa_profit = []
                toll_profit = []
            elif filter_normalized == "profit":
                regular_stoploss = []
                pa_stoploss = []
                toll_stoploss = []

            # Check if filter resulted in no signals
            if filter_normalized:
                filtered_count = (
                    len(regular_profit)
                    + len(regular_stoploss)
                    + len(pa_profit)
                    + len(pa_stoploss)
                    + len(toll_profit)
                    + len(toll_stoploss)
                )
                if filtered_count == 0:
                    filter_label = "stop loss" if filter_normalized == "stoploss" else "profit"
                    embed = discord.Embed(
                        title=f"📊 {period.title()} Trading Report - {filter_label.title()} Only",
                        description=f"No {filter_label} signals found for the current {period}",
                        color=0xFFA500,
                    )
                    await loading_msg.edit(content=None, embed=embed)
                    return

            # Calculate overall statistics
            total_regular = len(
                [
                    s
                    for s in regular_signals
                    if s.get("status", "").lower() in ["profit", "stoploss", "stop_loss"]
                ]
            )
            total_pa = len(
                [
                    s
                    for s in pa_signals
                    if s.get("status", "").lower() in ["profit", "stoploss", "stop_loss"]
                ]
            )
            total_tolls = len(
                [
                    s
                    for s in toll_signals
                    if s.get("status", "").lower() in ["profit", "stoploss", "stop_loss"]
                ]
            )
            total_signals = total_regular + total_pa + total_tolls

            regular_profit_count = len(regular_profit)
            regular_sl_count = len(regular_stoploss)
            pa_profit_count = len(pa_profit)
            pa_sl_count = len(pa_stoploss)
            toll_profit_count = len(toll_profit)
            toll_sl_count = len(toll_stoploss)

            total_profit = regular_profit_count + pa_profit_count + toll_profit_count
            total_sl = regular_sl_count + pa_sl_count + toll_sl_count

            # Calculate win rates
            regular_win_rate = (
                (regular_profit_count / total_regular * 100) if total_regular > 0 else 0
            )
            pa_win_rate = (pa_profit_count / total_pa * 100) if total_pa > 0 else 0
            toll_win_rate = (toll_profit_count / total_tolls * 100) if total_tolls > 0 else 0
            overall_win_rate = (total_profit / total_signals * 100) if total_signals > 0 else 0

            # Create embed
            title_suffix = ""
            description_suffix = ""
            if filter_normalized == "stoploss":
                title_suffix = " - Stop Losses Only"
                description_suffix = " (stop loss signals only)"
            elif filter_normalized == "profit":
                title_suffix = " - Profits Only"
                description_suffix = " (profit signals only)"

            embed = discord.Embed(
                title=f"📊 {period.title()} Trading Report{title_suffix}",
                description=f"Date: {date_range['display_start']} - {date_range['display_end']}",
                color=0x00FF00 if overall_win_rate >= 50 else 0xFF0000,
            )

            # Regular Signals Section
            if total_regular > 0:
                embed.add_field(
                    name="Regular Signals",
                    value=f"Total: {total_regular} | Win Rate: {regular_win_rate:.1f}%\n"
                    f"Profit: {regular_profit_count} | Stop Loss: {regular_sl_count}",
                    inline=True,
                )

            # PA Signals Section
            if total_pa > 0:
                embed.add_field(
                    name="PA Signals",
                    value=f"Total: {total_pa} | Win Rate: {pa_win_rate:.1f}%\n"
                    f"Profit: {pa_profit_count} | Stop Loss: {pa_sl_count}",
                    inline=True,
                )

            # Tolls Signals Section
            if total_tolls > 0:
                embed.add_field(
                    name="Tolls Signals",
                    value=f"Total: {total_tolls} | Win Rate: {toll_win_rate:.1f}%\n"
                    f"Profit: {toll_profit_count} | Stop Loss: {toll_sl_count}",
                    inline=True,
                )

            # Build REGULAR TRADES section (profit first, then stop loss)
            if total_regular > 0:
                trade_lines = []

                for signal in regular_profit:
                    limits = signal.get("limits", [])
                    if limits:
                        first_limit = format_price(limits[0]["price_level"], signal["instrument"])
                        limit_display = (
                            f"{first_limit}, +{len(limits) - 1} more"
                            if len(limits) > 1
                            else first_limit
                        )
                    else:
                        limit_display = "N/A"
                    trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in regular_stoploss:
                    sl_value = (
                        format_price(signal.get("stop_loss"), signal["instrument"])
                        if signal.get("stop_loss")
                        else "N/A"
                    )
                    trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if trade_lines:
                    embed.add_field(
                        name=f"Regular Trades ({total_regular})",
                        value=cap_field_value(trade_lines),
                        inline=False,
                    )

            # Build PA TRADES section (profit first, then stop loss)
            if total_pa > 0:
                pa_trade_lines = []

                for signal in pa_profit:
                    limits = signal.get("limits", [])
                    if limits:
                        first_limit = format_price(limits[0]["price_level"], signal["instrument"])
                        limit_display = (
                            f"{first_limit}, +{len(limits) - 1} more"
                            if len(limits) > 1
                            else first_limit
                        )
                    else:
                        limit_display = "N/A"
                    pa_trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in pa_stoploss:
                    sl_value = (
                        format_price(signal.get("stop_loss"), signal["instrument"])
                        if signal.get("stop_loss")
                        else "N/A"
                    )
                    pa_trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if pa_trade_lines:
                    embed.add_field(
                        name=f"PA Trades ({total_pa})",
                        value=cap_field_value(pa_trade_lines),
                        inline=False,
                    )

            # Build TOLLS TRADES section (profit first, then stop loss)
            if total_tolls > 0:
                toll_trade_lines = []

                for signal in toll_profit:
                    limits = signal.get("limits", [])
                    if limits:
                        first_limit = format_price(limits[0]["price_level"], signal["instrument"])
                        limit_display = (
                            f"{first_limit}, +{len(limits) - 1} more"
                            if len(limits) > 1
                            else first_limit
                        )
                    else:
                        limit_display = "N/A"
                    toll_trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in toll_stoploss:
                    sl_value = (
                        format_price(signal.get("stop_loss"), signal["instrument"])
                        if signal.get("stop_loss")
                        else "N/A"
                    )
                    toll_trade_lines.append(
                        f"#{signal['signal_id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if toll_trade_lines:
                    embed.add_field(
                        name=f"Tolls Trades ({total_tolls})",
                        value=cap_field_value(toll_trade_lines),
                        inline=False,
                    )

            # Add live proof link from profit_channel
            profit_channel_id = channels_data.get("profit_channel")
            if profit_channel_id:
                profit_channel_url = (
                    f"https://discord.com/channels/{ctx.guild.id}/{profit_channel_id}"
                )
                embed.add_field(name="Live Proof", value=f"{profit_channel_url}", inline=False)

            embed.set_footer(
                text=f"Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
            )

            await loading_msg.edit(content=None, embed=embed)

        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error Generating Report",
                description=f"An error occurred: {e!s}",
                color=0xFF0000,
            )
            await loading_msg.edit(content=None, embed=error_embed)
            logger.error(f"Error in report command: {e}")
