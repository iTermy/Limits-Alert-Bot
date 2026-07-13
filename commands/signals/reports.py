"""
Report Commands — trading performance reports.
"""

import json
import math
from datetime import datetime
from pathlib import Path

import discord
from discord.ext import commands

from database import report_queries
from price_feeds.tp_config import TPConfig
from utils.formatting import format_price
from utils.logger import get_logger

from .._base import BaseCog

logger = get_logger("report_commands")

# Channels whose signals are reported under the "Legends" section, regardless
# of signal type. Matched by their channels.json monitored-channel key.
LEGENDS_CHANNEL_KEYS = {"legends-trades", "lc-calls"}

# Channels whose signals are reported under the "Risky" section, regardless of
# signal type. Matched by their channels.json monitored-channel key.
RISKY_CHANNEL_KEYS = {"risky-gold"}

# Manual-profit closures are credited a fraction of the configured auto-TP
# target, since a manually closed signal didn't ride all the way to auto-TP.
MANUAL_PROFIT_TP_FRACTION = 2 / 3


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

    @staticmethod
    def _hit_limits(signal):
        return [lim for lim in signal.limits if lim.status == "hit" or lim.hit_alert_sent]

    def _tp_distance_raw(self, signal):
        """
        Cumulative profit distance (native units): the sum, over every hit
        limit, of that limit's P&L to the take-profit. The exit price is the
        recorded close: a retrospective manual_tp_price override wins, otherwise
        tp_price (set by both auto-TP and manual profit). Only when neither is
        present (pre-2026-06 manual closes) does each limit's P&L fall back to a
        fraction (MANUAL_PROFIT_TP_FRACTION) of the configured auto-TP target
        measured from the last hit limit.
        Returns None if the signal has no hit limits.
        """
        hit = self._hit_limits(signal)
        if not hit:
            return None
        direction = signal.direction.lower()
        instrument = signal.instrument
        signal_type = signal.type.lower()

        tp_price = signal.manual_tp_price
        if tp_price is None:
            tp_price = signal.tp_price
        if tp_price is not None:
            return sum(
                self.tp_config.calculate_pnl(
                    instrument, direction, lim.price_level, float(tp_price),
                    signal_type=signal_type,
                )
                for lim in hit
            )
        # No recorded exit price: each limit's P&L measured to a fraction of the
        # configured auto-TP target distance from the last hit limit, since a
        # manual close didn't ride all the way to auto-TP.
        target = (
            self.tp_config.get_tp_value(instrument, signal_type=signal_type)
            * MANUAL_PROFIT_TP_FRACTION
        )
        last_hit = max(hit, key=lambda lim: lim.sequence_number).price_level
        return sum(
            target
            + self.tp_config.calculate_pnl(
                instrument, direction, lim.price_level, last_hit,
                signal_type=signal_type,
            )
            for lim in hit
        )

    def _tp_distance_label(self, signal) -> str:
        """Signed profit-distance label, or "" if the signal has no hit limits."""
        raw = self._tp_distance_raw(signal)
        if raw is None:
            return ""
        return self._format_distance(signal.instrument, signal.type.lower(), raw)

    def _sl_distance_raw(self, signal):
        """
        Cumulative loss distance (native units, negative): the sum, over every
        hit limit, of that limit's P&L to the stop loss. Returns None if the
        signal has no stop loss or no hit limits.
        """
        sl = signal.stop_loss
        if not sl:
            return None
        hit = self._hit_limits(signal)
        if not hit:
            return None
        return sum(
            self.tp_config.calculate_pnl(
                signal.instrument, signal.direction.lower(), lim.price_level, float(sl),
                signal_type=signal.type.lower(),
            )
            for lim in hit
        )

    def _sl_distance_label(self, signal) -> str:
        """Signed loss-distance label, or "" if the signal has no stop loss."""
        raw = self._sl_distance_raw(signal)
        if raw is None:
            return ""
        return self._format_distance(signal.instrument, signal.type.lower(), raw)

    @commands.command(name="report", description="Generate trading report")
    async def generate_report(self, ctx: commands.Context, *args):
        """
        Generate a trading report for a period.

        Args (order-independent):
            period: 'day', 'week', or 'month' (default 'week')
            'risky': show ONLY risky-gold trades (excluded from the default report)
            filter: 'stoploss'/'sl' or 'profit'/'win' to filter results

        Examples:
            !report                 — this week, excluding risky
            !report month           — past 30 days, excluding risky
            !report risky           — this week, risky only
            !report risky months    — past 30 days, risky only
        """
        tokens = [a.lower() for a in args]

        # Risky is a mode toggle: excluded from the default report, shown alone
        # when explicitly requested.
        risky_mode = "risky" in tokens
        tokens = [t for t in tokens if t != "risky"]

        period = "week"
        filter_normalized = None
        for token in tokens:
            if token in ("day", "week", "month", "months"):
                period = "month" if token in ("month", "months") else token
            elif token in ("stoploss", "sl", "stop", "stop_loss"):
                filter_normalized = "stoploss"
            elif token in ("profit", "win", "tp"):
                filter_normalized = "profit"
            else:
                await ctx.send(
                    "❌ Unknown option. Usage: `!report [day/week/month] [risky] [stoploss/profit]`"
                )
                return

        period_label = "past 30 days" if period == "month" else f"current {period}"
        risky_note = " (risky only)" if risky_mode else ""

        # Update loading message based on filter
        if filter_normalized:
            loading_msg = await ctx.send(
                f"📊 Generating {period} report{risky_note} ({filter_normalized} only)..."
            )
        else:
            loading_msg = await ctx.send(f"📊 Generating {period} report{risky_note}...")

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
            date_range = await report_queries.get_trading_period_range(period)
            start_date = date_range["start"]
            end_date = date_range["end"]

            signals = await report_queries.get_period_signals_with_results(
                self.signal_db.db, start_date, end_date
            )

            if not signals:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"No signals found for the {period_label}",
                    color=0xFFA500,
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            # Fetch full signal details with limits for each signal
            enriched_signals = []
            for period_row in signals:
                full_signal = await self.signal_db.get_signal_with_limits(period_row["id"])
                if full_signal:
                    # Merge the status and other info from period query
                    full_signal.status = period_row["status"]
                    full_signal.channel_id = period_row["channel_id"]
                    full_signal.type = period_row.get("type") or full_signal.type or "standard"
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

            # Legends and Risky are channel-driven (these channels can host
            # signals of any type), so collect their channel IDs from channels.json.
            legends_channel_ids = {
                str(channel_id)
                for name, channel_id in monitored_channels.items()
                if name in LEGENDS_CHANNEL_KEYS
            }
            risky_channel_ids = {
                str(channel_id)
                for name, channel_id in monitored_channels.items()
                if name in RISKY_CHANNEL_KEYS
            }

            # Partition into 6 groups: Legends and Risky win by channel; the rest
            # map by type. standard / scalp / swing all fall under Regular.
            regular_signals = []
            tolls_signals = []
            pa_signals = []
            legends_signals = []
            one_to_one_signals = []
            risky_signals = []

            for signal in signals:
                channel_id = str(signal.channel_id or "")
                signal_type = signal.type.lower()

                if channel_id in legends_channel_ids:
                    legends_signals.append(signal)
                elif channel_id in risky_channel_ids or signal_type == "risky":
                    risky_signals.append(signal)
                elif signal_type == "toll":
                    tolls_signals.append(signal)
                elif signal_type == "pa":
                    pa_signals.append(signal)
                elif signal_type == "1-1":
                    one_to_one_signals.append(signal)
                else:
                    regular_signals.append(signal)

            def split_profit_sl(group_signals):
                profit = [s for s in group_signals if s.status.lower() == "profit"]
                sl = [
                    s
                    for s in group_signals
                    if s.status.lower() in ("stoploss", "stop_loss")
                ]
                if filter_normalized == "stoploss":
                    profit = []
                elif filter_normalized == "profit":
                    sl = []
                return profit, sl

            # Risky is reported only in risky mode; the default report omits it
            # entirely (no Risky group, and risky signals excluded from totals).
            if risky_mode:
                groups = [("Risky", risky_signals)]
            else:
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
                kind = "Risky Trading Report" if risky_mode else "Trading Report"
                embed = discord.Embed(
                    title=f"📊 {period.title()} {kind} - {filter_label.title()} Only",
                    description=f"No {filter_label} signals found for the {period_label}",
                    color=0xFFA500,
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            if risky_mode and not filter_normalized and total_signals == 0:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Risky Trading Report",
                    description=f"No risky signals found for the {period_label}",
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

            report_kind = "Risky Trading Report" if risky_mode else "Trading Report"
            embed = discord.Embed(
                title=f"📊 {period.title()} {report_kind}{title_suffix}",
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
                limits = signal.limits
                if limits:
                    first_limit = format_price(limits[0].price_level, signal.instrument)
                    limit_display = (
                        f"{first_limit}, +{len(limits) - 1} more"
                        if len(limits) > 1
                        else first_limit
                    )
                else:
                    limit_display = "N/A"
                tp_label = self._tp_distance_label(signal)
                direction_seg = signal.direction.upper()
                if tp_label:
                    direction_seg = f"{direction_seg} | {tp_label}"
                return (
                    f"#{signal.signal_id} | {signal.instrument} | "
                    f"{limit_display} | {direction_seg} 🟢"
                )

            def sl_line(signal):
                sl_value = (
                    format_price(signal.stop_loss, signal.instrument)
                    if signal.stop_loss
                    else "N/A"
                )
                sl_label = self._sl_distance_label(signal)
                direction_seg = signal.direction.upper()
                if sl_label:
                    direction_seg = f"{direction_seg} | {sl_label}"
                return (
                    f"#{signal.signal_id} | {signal.instrument} | "
                    f"SL: {sl_value} | {direction_seg} 🛑"
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

            # Cumulative gain: pips across forex, dollars across gold. Profit
            # signals add their TP distance; stop losses subtract their SL
            # distance. Other asset classes (indices, oil, crypto, stocks) are
            # ignored.
            forex_pips = 0.0
            gold_dollars = 0.0
            for stats in group_stats.values():
                for signal in stats["profit"]:
                    raw = self._tp_distance_raw(signal)
                    if raw is None:
                        continue
                    asset_class = self.tp_config.determine_asset_class(signal.instrument)
                    if asset_class in ("forex", "forex_jpy"):
                        forex_pips += raw
                    elif asset_class == "metals":
                        gold_dollars += raw
                for signal in stats["sl"]:
                    raw = self._sl_distance_raw(signal)
                    if raw is None:
                        continue
                    asset_class = self.tp_config.determine_asset_class(signal.instrument)
                    if asset_class in ("forex", "forex_jpy"):
                        forex_pips += raw
                    elif asset_class == "metals":
                        gold_dollars += raw

            forex_sign = "+" if forex_pips >= 0 else "-"
            gold_sign = "+" if gold_dollars >= 0 else "-"
            embed.add_field(
                name="Profit",
                value=(
                    f"Forex: {forex_sign}{abs(forex_pips):.1f} pips\n"
                    f"Gold: {gold_sign}${abs(gold_dollars):.2f}"
                ),
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
