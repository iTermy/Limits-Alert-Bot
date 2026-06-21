"""
Report Commands — trading performance reports.
"""

import json
import math
from datetime import datetime
from pathlib import Path

import discord
from discord.ext import commands

from price_feeds.tp_config import TPConfig
from utils.formatting import format_price
from utils.logger import get_logger

from .._base import BaseCog

logger = get_logger("report_commands")

# Channels whose signals are reported under the "Legends" section, regardless
# of signal type. Matched by their channels.json monitored-channel key.
LEGENDS_CHANNEL_KEYS = {"legends-trades", "lc-calls"}


class ReportsCog(BaseCog):
    """Trading report generation"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()

    def _format_distance(self, instrument: str, signal_type: str, raw: float) -> str:
        """
        Format a native-unit distance (pips for forex, dollars otherwise) with a
        signed label, rounding the magnitude up. e.g. "+8 pips", "-20 pips", "+$5".
        """
        tp_type = self.tp_config.get_tp_type(instrument, signal_type=signal_type)
        magnitude = math.ceil(abs(raw) - 1e-9)
        sign = "+" if raw >= 0 else "-"
        if tp_type == "pips":
            return f"{sign}{magnitude} pips"
        return f"{sign}${magnitude}"

    def _tp_distance_label(self, signal) -> str:
        """
        Profit distance from tp_price to the hit limit furthest from it
        (lowest hit limit for a long, highest for a short). Returns "" if the
        signal has no tp_price or no hit limits.
        """
        tp_price = signal.get("tp_price")
        if tp_price is None:
            return ""
        hit = [
            l for l in signal.get("limits", [])
            if l.get("status") == "hit" or l.get("hit_alert_sent")
        ]
        if not hit:
            return ""
        direction = signal["direction"].lower()
        prices = [l["price_level"] for l in hit]
        entry = min(prices) if direction == "long" else max(prices)
        signal_type = (signal.get("type") or "standard").lower()
        raw = self.tp_config.calculate_pnl(
            signal["instrument"], direction, entry, float(tp_price), signal_type=signal_type
        )
        return self._format_distance(signal["instrument"], signal_type, raw)

    def _sl_distance_label(self, signal) -> str:
        """
        Loss distance from the first limit to the stop loss. Returns "" if the
        signal has no stop loss or no limits.
        """
        sl = signal.get("stop_loss")
        limits = signal.get("limits", [])
        if not sl or not limits:
            return ""
        direction = signal["direction"].lower()
        first = sorted(limits, key=lambda l: l.get("sequence_number", 0))[0]["price_level"]
        signal_type = (signal.get("type") or "standard").lower()
        raw = self.tp_config.calculate_pnl(
            signal["instrument"], direction, first, float(sl), signal_type=signal_type
        )
        return self._format_distance(signal["instrument"], signal_type, raw)

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
                    full_signal["type"] = signal.get("type") or full_signal.get("type") or "standard"
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

            # Legends are still channel-driven (these channels can host signals
            # of any type), so collect their channel IDs from channels.json.
            legends_channel_ids = {
                str(channel_id)
                for name, channel_id in monitored_channels.items()
                if name in LEGENDS_CHANNEL_KEYS
            }

            # Partition into 5 groups: Legends wins by channel; the rest map by type.
            # standard / scalp / swing all fall under Regular.
            regular_signals = []
            tolls_signals = []
            pa_signals = []
            legends_signals = []
            one_to_one_signals = []

            for signal in signals:
                channel_id = str(signal.get("channel_id", ""))
                signal_type = (signal.get("type") or "standard").lower()

                if channel_id in legends_channel_ids:
                    legends_signals.append(signal)
                elif signal_type == "toll":
                    tolls_signals.append(signal)
                elif signal_type == "pa":
                    pa_signals.append(signal)
                elif signal_type == "1-1":
                    one_to_one_signals.append(signal)
                else:
                    regular_signals.append(signal)

            def split_profit_sl(group_signals):
                profit = [
                    s for s in group_signals if s.get("status", "").lower() == "profit"
                ]
                sl = [
                    s
                    for s in group_signals
                    if s.get("status", "").lower() in ("stoploss", "stop_loss")
                ]
                if filter_normalized == "stoploss":
                    profit = []
                elif filter_normalized == "profit":
                    sl = []
                return profit, sl

            groups = [
                ("Regular", regular_signals),
                ("Tolls", tolls_signals),
                ("PA", pa_signals),
                ("Legends", legends_signals),
                ("1-1", one_to_one_signals),
            ]

            # Per-group stats: { label: {"profit": [...], "sl": [...], "total": N, "win_rate": x} }
            group_stats = {}
            for label, group_signals in groups:
                profit, sl = split_profit_sl(group_signals)
                total = len(profit) + len(sl)
                win_rate = (len(profit) / total * 100) if total > 0 else 0
                group_stats[label] = {
                    "profit": profit,
                    "sl": sl,
                    "total": total,
                    "win_rate": win_rate,
                }

            total_signals = sum(g["total"] for g in group_stats.values())
            total_profit = sum(len(g["profit"]) for g in group_stats.values())

            # Check if filter resulted in no signals
            if filter_normalized and total_signals == 0:
                filter_label = "stop loss" if filter_normalized == "stoploss" else "profit"
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report - {filter_label.title()} Only",
                    description=f"No {filter_label} signals found for the current {period}",
                    color=0xFFA500,
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            overall_win_rate = (total_profit / total_signals * 100) if total_signals > 0 else 0

            # Create embed
            title_suffix = ""
            if filter_normalized == "stoploss":
                title_suffix = " - Stop Losses Only"
            elif filter_normalized == "profit":
                title_suffix = " - Profits Only"

            embed = discord.Embed(
                title=f"📊 {period.title()} Trading Report{title_suffix}",
                description=f"Date: {date_range['display_start']} - {date_range['display_end']}",
                color=0x00FF00 if overall_win_rate >= 50 else 0xFF0000,
            )

            # Per-group summary fields
            for label, _ in groups:
                stats = group_stats[label]
                if stats["total"] == 0:
                    continue
                embed.add_field(
                    name=f"{label} Signals",
                    value=(
                        f"Total: {stats['total']} | Win Rate: {stats['win_rate']:.1f}%\n"
                        f"Profit: {len(stats['profit'])} | Stop Loss: {len(stats['sl'])}"
                    ),
                    inline=True,
                )

            def trade_line(signal):
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
                tp_label = self._tp_distance_label(signal)
                symbol_seg = (
                    f"{signal['instrument']} | {tp_label}" if tp_label else signal["instrument"]
                )
                return (
                    f"#{signal['signal_id']} | {symbol_seg} | "
                    f"{limit_display} | {signal['direction'].upper()} 🟢"
                )

            def sl_line(signal):
                sl_value = (
                    format_price(signal.get("stop_loss"), signal["instrument"])
                    if signal.get("stop_loss")
                    else "N/A"
                )
                sl_label = self._sl_distance_label(signal)
                symbol_seg = (
                    f"{signal['instrument']} | {sl_label}" if sl_label else signal["instrument"]
                )
                return (
                    f"#{signal['signal_id']} | {symbol_seg} | "
                    f"SL: {sl_value} | {signal['direction'].upper()} 🛑"
                )

            # Per-group trade detail sections (profit first, then SL)
            for label, _ in groups:
                stats = group_stats[label]
                if stats["total"] == 0:
                    continue
                lines = [trade_line(s) for s in stats["profit"]]
                lines.extend(sl_line(s) for s in stats["sl"])
                if lines:
                    embed.add_field(
                        name=f"{label} Trades ({stats['total']})",
                        value=cap_field_value(lines),
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
