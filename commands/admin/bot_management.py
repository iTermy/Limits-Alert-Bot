"""
Bot Management Commands - ping, help, health, feeds, price, reload, shutdown, clear, cleanalerts, goldtollssl
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

import discord
from discord.ext import commands

from core.parser.pattern_parsers import (
    get_gold_tolls_sl_offset,
    get_risky_gold_sl_offset,
    invalidate_gold_tolls_sl_cache,
    invalidate_risky_gold_sl_cache,
)
from database import report_queries
from database.config_history_ops import log_config_change
from utils.config_loader import load_channels_config, load_settings, save_settings

from .._base import BaseCog


@dataclass(frozen=True)
class _SlOffsetSpec:
    """Everything that differs between the gold-tolls and risky-gold SL commands."""

    label: str  # embed titles, e.g. "Gold Tolls"
    icon: str  # embed emoji
    command_name: str  # for the usage hint
    settings_attr: str  # BotSettings attribute + config_history key
    read_offset: Callable[[], float]
    invalidate_cache: Callable[[], None]
    channel_ids_attr: str  # AlertSystem attribute holding the channel-id set
    channel_matches: Callable[[str], bool]  # fallback channels.json name predicate
    empty_channels_note: str


_GOLD_TOLLS_SL_SPEC = _SlOffsetSpec(
    label="Gold Tolls",
    icon="🛑",
    command_name="goldtollssl",
    settings_attr="gold_tolls_sl_offset",
    read_offset=get_gold_tolls_sl_offset,
    invalidate_cache=invalidate_gold_tolls_sl_cache,
    channel_ids_attr="toll_channel_ids",
    # Risky-gold has its own independent SL offset (!riskygoldsl), so it is
    # deliberately excluded here — only true toll channels update.
    channel_matches=lambda name: "toll" in name and name != "general-tolls",
    empty_channels_note="No gold-toll channels found — offset saved but no signals updated.",
)

_RISKY_GOLD_SL_SPEC = _SlOffsetSpec(
    label="Risky Gold",
    icon="🎲",
    command_name="riskygoldsl",
    settings_attr="risky_gold_sl_offset",
    read_offset=get_risky_gold_sl_offset,
    invalidate_cache=invalidate_risky_gold_sl_cache,
    channel_ids_attr="risky_channel_ids",
    channel_matches=lambda name: name == "risky-gold",
    empty_channels_note="No risky-gold channel found — offset saved but no signals updated.",
)


# Topic pages for !help <topic>. Aliases map to the same builder.
def _help_cancel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🚫 Cancel Command — Detailed Help",
        description="Cancel one signal or bulk-cancel groups of signals.",
        color=0xFF6600,
    )
    embed.add_field(
        name="Cancel a specific signal",
        value="`!cancel <id>` — cancel signal by its ID",
        inline=False,
    )
    embed.add_field(
        name="Cancel Gold signals by type",
        value=(
            "`!cancel gold longs setups` — cancel active Gold long setups\n"
            "`!cancel gold shorts setups` — cancel active Gold short setups\n"
            "`!cancel gold both setups` — cancel active Gold long & short setups\n"
            "`!cancel gold longs pa` — cancel active Gold long price action\n"
            "`!cancel gold shorts pa` — cancel active Gold short price action\n"
            "`!cancel gold both pa` — cancel active Gold long & short price action\n"
            "`!cancel gold longs tolls` — cancel active Gold long tolls\n"
            "`!cancel gold shorts tolls` — cancel active Gold short tolls\n"
            "`!cancel gold both tolls` — cancel active Gold long & short tolls\n"
            "`!cancel gold longs everything` — cancel ALL active Gold longs\n"
            "`!cancel gold shorts everything` — cancel ALL active Gold shorts\n"
            "`!cancel gold both everything` — cancel ALL active Gold signals\n\n"
            "**Order doesn't matter:** `!cancel gold longs setups` and `!cancel gold setups longs` are equivalent."
        ),
        inline=False,
    )
    embed.add_field(
        name="Cancel by instrument pair",
        value="`!cancel all EURUSD` — cancel all active signals for EURUSD",
        inline=False,
    )
    embed.add_field(
        name="Cancel by currency",
        value=(
            "`!cancel all EUR` — cancel all active signals whose instrument contains EUR\n"
            "`!cancel all USD` — cancel all active signals whose instrument contains USD"
        ),
        inline=False,
    )
    embed.set_footer(
        text="All bulk cancels only affect signals with status 'active' or 'hit'."
    )
    return embed


def _help_tp_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Take-Profit Command — Detailed Help",
        description=(
            "View and manage auto take-profit thresholds.\n"
            "Every TP value is scoped to a **signal type**: `standard`, `scalp`, `swing`, "
            "`toll`, `pa`, `1-1`, `risky`."
        ),
        color=0x00BFFF,
    )
    embed.add_field(
        name="Show config",
        value=(
            "`!tp config` — show all type defaults at a glance\n"
            "`!tp config <type>` — show one type's full config (e.g. `!tp config swing`)\n"
            "`!tp config <symbol> [--type=X]` — effective TP for a symbol "
            "(e.g. `!tp config XAUUSD --type=swing`)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Set asset-class default (admin)",
        value=(
            "`!tp set <type> <class> <value> [pips|dollars]` — per-type asset-class default\n"
            "  • `!tp set swing metals 15 dollars` — gold swings → $15\n"
            "  • `!tp set standard metals 5 dollars` — gold standard → $5\n"
            "  • `!tp set scalp forex 3 pips` — forex scalps → 3 pips\n\n"
            "`!tp set <class> <value>` — shortcut for `standard` (e.g. `!tp set metals 5`)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Set per-symbol override (admin)",
        value=(
            "`!tp set <symbol> <value> [pips|dollars] [--type=X]`\n"
            "  • `!tp set XAUUSD 8 dollars --type=swing` — gold swings, $8 on XAUUSD only\n"
            "  • `!tp set XAUUSD 5 dollars` — standard override on XAUUSD\n\n"
            "Per-symbol overrides take priority over asset-class defaults."
        ),
        inline=False,
    )
    embed.add_field(
        name="Remove override (admin)",
        value=(
            "`!tp remove <symbol> [--type=X]` — drop a per-symbol override, reverting to "
            "the asset-class default (e.g. `!tp remove XAUUSD --type=swing`)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Reference",
        value=(
            "**Signal types:** standard, scalp, swing, toll, pa, 1-1, risky\n"
            "**Asset classes:** forex, forex_jpy, metals, indices, stocks, crypto, oil"
        ),
        inline=False,
    )
    embed.set_footer(
        text="Auto-TP triggers when the last limit hits the threshold and earlier limits are combined breakeven."
    )
    return embed


def _help_alertdist_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Alert Distance Command — Detailed Help",
        description="View and manage the approaching-alert distance thresholds.\nThis controls how close price must get to a limit before an 'approaching' alert fires.",
        color=0x00BFFF,
    )
    embed.add_field(
        name="Show config",
        value=(
            "`!alertdist config` — show all asset-class defaults and per-symbol overrides\n"
            "`!alertdist config <symbol>` — show config for a specific symbol (e.g. `!alertdist config XAUUSD`)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Set threshold (admin)",
        value=(
            "`!alertdist set <class> <value>` — set asset-class default (e.g. `!alertdist set metals 8`)\n"
            "`!alertdist set <symbol> <value>` — set per-symbol override (e.g. `!alertdist set XAUUSD 5`)\n"
            "`!alertdist set <target> <value> pips` — force pips type\n"
            "`!alertdist set <target> <value> dollars` — force dollars type\n"
            "`!alertdist set <target> <value> percentage` — force percentage type\n\n"
            "Valid asset classes: forex, forex_jpy, metals, indices, stocks, crypto, oil\n"
            "Aliases: `!alertdistance`, `!adist`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Remove per-symbol override (admin)",
        value="`!alertdist remove <symbol>` — remove override, reverting to asset-class default (e.g. `!alertdist remove XAUUSD`)",
        inline=False,
    )
    embed.add_field(
        name="Distance types",
        value=(
            "**pips** — used for forex pairs (e.g. 10 pips)\n"
            "**dollars** — used for metals, oil (e.g. $8.00)\n"
            "**percentage** — used for indices, crypto (e.g. 0.5%)"
        ),
        inline=False,
    )
    embed.set_footer(
        text="If no type is specified, the existing type for that target is preserved."
    )
    return embed


def _help_news_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📰 News Command — Detailed Help",
        description="Schedule news windows that auto-cancel signals hit during the window.",
        color=0x5865F2,
    )
    embed.add_field(
        name="Schedule a news window",
        value=(
            "`!news <category> <time> [window] [tz:<tz>] [date:<date>]`\n"
            "Example: `!news USD 12:30pm 15` — USD news at 12:30 PM EST, ±15 min window\n"
            "Example: `!news gold 8:30am tz:UTC` — Gold news at 8:30 AM UTC\n"
            "Example: `!news all 14:00 30 date:2025-06-20` — All pairs on a specific date"
        ),
        inline=False,
    )
    embed.add_field(
        name="Client-only dry run",
        value=(
            "Add `dryrun` anywhere to pause affected client trading without changing "
            "the alert bot's signals.\n"
            "Example: `!news USD 12:30pm 15 dryrun`\n"
            "Example: `!news now gold dryrun`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Immediate / open-ended window",
        value=(
            "`!news now` / `!news on` — activate news mode immediately for ALL pairs\n"
            "`!news now USD` — activate immediately for USD pairs only\n"
            "`!news off` — deactivate all open-ended windows"
        ),
        inline=False,
    )
    embed.add_field(
        name="Tags (optional)",
        value=(
            "`tz:<timezone>` — timezone for the time (default: EST)\n"
            "  e.g. `tz:UTC`  `tz:GMT`  `tz:CET`  `tz:London`  `tz:JST`\n"
            "`date:<date>` — specific date (default: today)\n"
            "  e.g. `date:2025-06-15`  `date:06/15`  `date:tomorrow`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Categories",
        value=(
            "Any currency code: `USD`, `EUR`, `GBP`, `JPY`, etc.\n"
            "Named: `gold`, `oil`, `btc`, `eth`, `crypto`\n"
            "`all` — affects every instrument"
        ),
        inline=False,
    )
    embed.add_field(
        name="Managing events",
        value=(
            "`!newslist` — show all scheduled and active events\n"
            "`!newsclear <id>` — remove a specific event\n"
            "`!newsclear` — remove all events"
        ),
        inline=False,
    )
    embed.set_footer(
        text="Window is ±N minutes around the news time. Default window is 10 minutes."
    )
    return embed


_HELP_TOPICS = {
    "cancel": _help_cancel_embed,
    "tp": _help_tp_embed,
    "alertdist": _help_alertdist_embed,
    "alertdistance": _help_alertdist_embed,
    "adist": _help_alertdist_embed,
    "news": _help_news_embed,
}


class BotManagementCog(BaseCog):
    """Bot management and utility commands"""

    # ==================== GENERAL COMMANDS ====================

    @commands.command(name="help")
    async def help_command(self, ctx: commands.Context, *, topic: Optional[str] = None):
        """Show available commands, or detailed help for a topic (e.g. !help cancel)"""
        if topic:
            builder = _HELP_TOPICS.get(topic.lower())
            if builder:
                await ctx.send(embed=builder())
                return

        embed = discord.Embed(
            title="📚 Bot Commands",
            description="Available commands for the trading bot",
            color=0x00BFFF,
        )
        embed.add_field(
            name="Signals",
            value=(
                "`!active [instrument] [sort:method]` - Show active signals\n"
                "  └ Sort options: recent, oldest, distance, progress\n"
                "`!info <id>` - Signal details\n"
                "`!futures <id>` - Convert a gold signal to GCZ26_CFD futures prices\n"
                "`!report [day/week/month] [risky]` - Trading report (add `risky` for risky trades)\n"
                "`!profit <id>` - Mark as profit\n"
                "`!sl <id>` - Mark as stop loss\n"
                "`!cancel` - Cancel signals — see `!help cancel` for all options\n"
                "`!news` - News mode — see `!help news` for all options\n"
                "`!health` - Bot health check"
            ),
            inline=False,
        )
        embed.add_field(
            name="Configuration",
            value=(
                "`!tp` - Take-profit thresholds — see `!help tp` for all options\n"
                "`!alertdist` - Alert distance thresholds — see `!help alertdist` for all options\n"
                "`!nmconfig` - Near-miss auto-cancel config"
            ),
            inline=False,
        )

        if self.is_admin(ctx.author):
            embed.set_footer(text="Use !admin to see admin commands.")

        await ctx.send(embed=embed)

    # ==================== HEALTH COMMAND ====================

    @commands.command(name="health")
    async def health_check(self, ctx: commands.Context):
        """Bot health check — uptime, latency, feeds, database, modes"""
        now = datetime.utcnow()
        embed = discord.Embed(title="Bot Health", color=0x00FF00)

        # Uptime + latency
        delta = now - self.bot.start_time
        h = int(delta.total_seconds() // 3600)
        m = int((delta.total_seconds() % 3600) // 60)
        uptime_str = f"{h}h {m}m"
        embed.add_field(
            name="System",
            value=f"Uptime: {uptime_str}  ·  Latency: {round(self.bot.latency * 1000)}ms",
            inline=False,
        )

        # Database
        try:
            stats = await report_queries.get_statistics(self.signal_db.db)
            active = stats.get("tracking_count", 0)
            total = stats.get("total_signals", 0)
            embed.add_field(
                name="Database",
                value=f"🟢 Connected — {active} active / {total} total",
                inline=False,
            )
        except Exception as e:
            embed.add_field(name="Database", value=f"🔴 Error: {str(e)[:60]}", inline=False)
            embed.color = 0xFF4500

        # Feeds — per-feed symbol count + last update
        if self.services.monitor:
            health_monitor = getattr(self.services.monitor, "health_monitor", None)
            stream_manager = self.services.stream_manager
            feed_lines = []

            for feed_name in ["icmarkets", "oanda", "binance", "exness"]:
                if feed_name not in stream_manager.feed_status:
                    continue
                is_connected = stream_manager.feed_status.get(feed_name, False)
                emoji = "🟢" if is_connected else "🔴"
                line = f"{emoji} **{feed_name.upper()}**"

                if health_monitor:
                    feed_last_seen = health_monitor.last_seen.get(feed_name, {})
                    if feed_last_seen:
                        sym_count = len(feed_last_seen)
                        newest = max(feed_last_seen.values())
                        secs = int((now - newest).total_seconds())
                        line += f"  {sym_count} symbols · {secs}s ago"

                feed_lines.append(line)

            total_subs = len(getattr(stream_manager, "subscribed_symbols", set()))
            feed_lines.append(f"Subscriptions: {total_subs}")
            embed.add_field(name="Feeds", value="\n".join(feed_lines), inline=False)
        else:
            embed.add_field(name="Feeds", value="🔴 Monitor not running", inline=False)
            embed.color = 0xFFA500

        # Modes
        spread_active = False
        if self.services.monitor:
            spread_active = self.services.monitor._is_spread_hour()
        news_active = self.bot.news_manager.is_news_active_for("all")
        embed.add_field(
            name="Modes",
            value=f"Spread Hour: {'Yes' if spread_active else 'No'}  ·  News: {'On' if news_active else 'Off'}",
            inline=False,
        )

        # Discord write budget — the learned per-channel allowance and what the
        # current embed load costs. Answers "are we near the cap?" empirically,
        # since Discord does not publish these numbers.
        alert_system = self.services.alert_system
        if alert_system:
            embed.add_field(
                name="Discord Budget",
                value=self._budget_summary(alert_system),
                inline=False,
            )

        embed.set_footer(text=f"Generated at {now.strftime('%H:%M:%S')} UTC")
        await ctx.send(embed=embed)

    @staticmethod
    def _budget_summary(alert_system) -> str:
        """Per-channel write allowance and the sweep interval it implies."""
        budget = alert_system._channel_budget

        live_per_channel: dict[int, int] = {}
        for signal_id in alert_system._live_embeds:
            message = alert_system.signal_messages.get(signal_id)
            if message:
                live_per_channel[message.channel.id] = (
                    live_per_channel.get(message.channel.id, 0) + 1
                )

        if not live_per_channel:
            return "No live embeds"

        lines = []
        for channel_id, count in sorted(
            live_per_channel.items(), key=lambda item: item[1], reverse=True
        ):
            channel = alert_system.bot.get_channel(channel_id) if alert_system.bot else None
            name = f"#{channel.name}" if channel else str(channel_id)
            sweep = budget.refresh_interval_for(channel_id, count)
            lines.append(
                f"**{name}** — {count} embed(s) · "
                f"{budget.cosmetic_allowance(channel_id)}/"
                f"{budget.limit_for(channel_id)} slots per "
                f"{budget.window_for(channel_id):.0f}s · sweep ~{sweep:.0f}s"
            )
        return "\n".join(lines)

    # ==================== ADMIN COMMANDS ====================

    @commands.command(name="cleanalerts", aliases=["clearalerts", "purgealerts"])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def clean_alert_channels(self, ctx: commands.Context):
        """Run the full Friday cleanup: purge alert channels + monitored channels (Admin only)"""
        if not self.bot.channel_cleaner:
            await ctx.send("❌ Channel cleaner not available")
            return

        confirm_msg = await ctx.send(
            "⚠️ **WARNING**: Run the full weekly purge for the past **7 days**?\n"
            "• Alert channels: `alert`, `pa-alert`, `toll-alert`, `general-tolls-alert`, `legends-trade-alert`, `risky-gold-alert` "
            "(all messages deleted)\n"
            "• Monitored channels (except `price-action-trades`): orphans + non-active signal messages deleted\n"
            "React with ✅ to confirm or ❌ to cancel (30s timeout)"
        )

        await confirm_msg.add_reaction("✅")
        await confirm_msg.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == confirm_msg.id
            )

        try:
            reaction, _ = await self.bot.wait_for("reaction_add", timeout=30.0, check=check)

            if str(reaction.emoji) == "✅":
                await confirm_msg.edit(content="🧹 Running weekly purge… this may take a moment.")
                await confirm_msg.clear_reactions()

                try:
                    await self.bot.channel_cleaner._purge_alert_channels()
                    await self.bot.channel_cleaner._purge_monitored_channels()
                    await confirm_msg.edit(
                        content=f"✅ Weekly purge complete (past 7 days) — triggered by {ctx.author.name}"
                    )
                    self.logger.info(f"Weekly purge manually triggered by {ctx.author.name}")
                except Exception as e:
                    await confirm_msg.edit(content=f"❌ Purge failed: {e!s}")
                    self.logger.error(f"Manual weekly purge error: {e}", exc_info=True)
            else:
                await confirm_msg.edit(content="❌ Purge cancelled")
                await confirm_msg.clear_reactions()

        except asyncio.TimeoutError:
            await confirm_msg.edit(content="⏱️ Purge timed out")
            await confirm_msg.clear_reactions()

    @commands.command(name="goldtollssl", aliases=["gtsl", "goldtollsl"])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def gold_tolls_sl(self, ctx: commands.Context, value: Optional[float] = None):
        """
        Get or set the gold-tolls stop-loss offset (dollars from the nearest limit).
        Usage:  !goldtollssl          → show current value
                !goldtollssl 10       → set offset to $10
                !goldtollssl 5        → reset to default $5
        """
        await self._update_sl_offset(ctx, value, _GOLD_TOLLS_SL_SPEC)

    @commands.command(name="riskygoldsl", aliases=["rgsl", "riskysl"])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def risky_gold_sl(self, ctx: commands.Context, value: Optional[float] = None):
        """
        Get or set the risky-gold stop-loss offset (dollars from the nearest limit).
        Independent of the gold-tolls offset.
        Usage:  !riskygoldsl          → show current value
                !riskygoldsl 10       → set offset to $10
                !riskygoldsl 5        → reset to default $5
        """
        await self._update_sl_offset(ctx, value, _RISKY_GOLD_SL_SPEC)

    async def _update_sl_offset(
        self, ctx: commands.Context, value: float, spec: _SlOffsetSpec
    ) -> None:
        """Shared body for the auto-SL offset commands: show, persist, and
        retroactively re-derive the SL of every active signal in the spec's
        channels."""
        if value is None:
            current = spec.read_offset()
            embed = discord.Embed(
                title=f"{spec.icon} {spec.label} SL Offset",
                description=(
                    f"**Current offset:** `${current:.2f}` from the nearest limit\n\n"
                    f"{spec.label} stop losses are automatically placed this many dollars "
                    "beyond the nearest limit.\n\n"
                    f"**Long signals:** SL = lowest limit − `${current:.2f}`\n"
                    f"**Short signals:** SL = highest limit + `${current:.2f}`\n\n"
                    f"_Use `!{spec.command_name} <value>` to change it._"
                ),
                color=discord.Color.blue(),
            )
            await ctx.send(embed=embed)
            return

        if value <= 0:
            await ctx.send("❌ Offset must be a positive number.")
            return

        try:
            settings = load_settings()
            old_value = getattr(settings, spec.settings_attr)
            setattr(settings, spec.settings_attr, value)
            save_settings(settings)
            spec.invalidate_cache()
        except Exception as e:
            await ctx.send(f"❌ Failed to save setting: {e}")
            self.logger.error(f"Failed to save {spec.settings_attr}: {e}", exc_info=True)
            return

        self.logger.info(f"{spec.settings_attr} changed {old_value} to {value} by {ctx.author}")
        await log_config_change(
            self.signal_db.db, "settings", spec.settings_attr, old_value, value, ctx.author.name,
        )

        loading_msg = await ctx.send(f"🔄 Updating active {spec.label.lower()} signals…")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        try:
            monitor = getattr(self.bot, "monitor", None)
            alert_system = monitor.alert_system if monitor else None

            channel_ids: set[str] = set()
            if alert_system:
                channel_ids = set(getattr(alert_system, spec.channel_ids_attr, set()))

            if not channel_ids:
                channels_cfg = load_channels_config()
                monitored = channels_cfg.get("monitored_channels", {})
                channel_ids = {
                    str(ch_id)
                    for ch_name, ch_id in monitored.items()
                    if ch_id and spec.channel_matches(ch_name.lower())
                }

            if channel_ids:
                updated_list, skipped_count, error_count = await self.signal_db.bulk_update_toll_sl(
                    value, list(channel_ids)
                )
                updated_count = len(updated_list)

                for entry in updated_list:
                    sig_id = entry["id"]
                    if monitor and sig_id in monitor.active_signals:
                        monitor.active_signals[sig_id].stop_loss = entry["new_sl"]
                    if alert_system:
                        try:
                            await alert_system.update_embed_for_signal_id(sig_id, "edited")
                        except Exception as _ue:
                            self.logger.warning(
                                f"Could not update embed for signal {sig_id}: {_ue}"
                            )

                await loading_msg.delete()
            else:
                await loading_msg.edit(content=f"⚠️ {spec.empty_channels_note}")

        except Exception as bulk_err:
            self.logger.error(
                f"Retroactive {spec.label.lower()} SL update failed: {bulk_err}", exc_info=True
            )
            await loading_msg.edit(content=f"⚠️ Retroactive update encountered an error: {bulk_err}")

        retro_lines = []
        if updated_count:
            retro_lines.append(f"✅ **{updated_count}** active signal(s) SL updated retroactively")
        if skipped_count:
            retro_lines.append(f"⏭️ **{skipped_count}** signal(s) skipped (no change / no limits)")
        if error_count:
            retro_lines.append(f"⚠️ **{error_count}** signal(s) failed to update (see logs)")
        if not retro_lines:
            retro_lines.append(f"ℹ️ No active {spec.label.lower()} signals found to update")

        embed = discord.Embed(
            title=f"✅ {spec.label} SL Offset Updated",
            description=(
                f"**Old offset:** `${old_value:.2f}`\n"
                f"**New offset:** `${value:.2f}`\n\n"
                f"**Long signals:** SL = lowest limit − `${value:.2f}`\n"
                f"**Short signals:** SL = highest limit + `${value:.2f}`\n\n"
                + "\n".join(retro_lines)
            ),
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @commands.command(name="deletemessage", aliases=["delmsg", "deletemsg"])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def delete_message(self, ctx: commands.Context, message_id: Optional[int] = None):
        """Delete a message by ID (Admin only).

        Searches the current channel first, then every text channel in the guild.
        Usage:  !deletemessage <message_id>
        """
        if message_id is None:
            await ctx.send("❌ Usage: `!deletemessage <message_id>`")
            return

        channels = [ctx.channel] + [
            ch for ch in ctx.guild.text_channels if ch.id != ctx.channel.id
        ]
        for channel in channels:
            try:
                msg = await channel.fetch_message(message_id)
            except (discord.NotFound, discord.Forbidden):
                continue
            except Exception as e:
                self.logger.warning(f"Error fetching message {message_id} in #{channel.name}: {e}")
                continue

            try:
                await msg.delete()
            except Exception as e:
                await ctx.send(f"❌ Found the message in #{channel.name} but could not delete it: {e}")
                return

            self.logger.info(
                f"Message {message_id} in #{channel.name} deleted by {ctx.author}"
            )
            await ctx.send(f"✅ Deleted message `{message_id}` from #{channel.name}")
            return

        await ctx.send(f"❌ Message `{message_id}` not found in any channel.")

    @commands.command(name="admin")
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def admin_commands_list(self, ctx: commands.Context):
        """List all admin commands"""
        embed = discord.Embed(title="🔒 Admin Commands", color=0xFF6600)
        embed.add_field(
            name="Bot",
            value=(
                "`!cleanalerts` — Purge past 7 days from all alert channels\n"
                "`!goldtollssl [value]` — Get/set gold tolls SL offset ($)\n"
                "  Aliases: `!gtsl`, `!goldtollsl`\n"
                "`!riskygoldsl [value]` — Get/set risky-gold SL offset ($)\n"
                "  Aliases: `!rgsl`, `!riskysl`\n"
                "`!deletemessage <message_id>` — Delete a message by ID\n"
                "  Aliases: `!delmsg`, `!deletemsg`"
            ),
            inline=False,
        )
        embed.add_field(
            name="Licenses",
            value=(
                "`!activate` — Get a license key (requires Signal Subscriber role)\n"
                "`!setkeys @user <n>` — Set max license keys for a user\n"
                "`!grantkey @user <mt5>` — Issue a license key directly\n"
                "`!revoke @user [mt5]` — Revoke a user's license\n"
                "`!licenses [@user]` — List all active licenses"
            ),
            inline=False,
        )
        await ctx.send(embed=embed)
