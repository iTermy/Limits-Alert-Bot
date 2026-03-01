"""
Bot Commands - Essential bot management and utility commands
"""
from discord.ext import commands
import discord
import asyncio
from .base_command import BaseCog
from datetime import datetime


class BotCommands(BaseCog):
    """Bot management and utility commands"""

    # ==================== GENERAL COMMANDS ====================

    @commands.command(name='ping')
    async def ping(self, ctx: commands.Context):
        """Check bot latency"""
        latency = round(self.bot.latency * 1000)
        await ctx.send(f"Latency: {latency}ms")

    @commands.command(name='help')
    async def help_command(self, ctx: commands.Context, *, topic: str = None):
        """Show available commands, or detailed help for a topic (e.g. !help cancel)"""

        # ── Subcommand: !help cancel ──
        if topic and topic.lower() == "cancel":
            embed = discord.Embed(
                title="🚫 Cancel Command — Detailed Help",
                description="Cancel one signal or bulk-cancel groups of signals.",
                color=0xFF6600
            )
            embed.add_field(
                name="Cancel a specific signal",
                value="`!cancel <id>` — cancel signal by its ID",
                inline=False
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
                    "`!cancel gold both everything` — cancel ALL active Gold signals"
                ),
                inline=False
            )
            embed.add_field(
                name="Cancel by instrument pair",
                value="`!cancel all EURUSD` — cancel all active signals for EURUSD",
                inline=False
            )
            embed.add_field(
                name="Cancel by currency",
                value=(
                    "`!cancel all EUR` — cancel all active signals whose instrument contains EUR\n"
                    "`!cancel all USD` — cancel all active signals whose instrument contains USD"
                ),
                inline=False
            )
            embed.set_footer(text="All bulk cancels only affect signals with status 'active' or 'hit'.")
            await ctx.send(embed=embed)
            return

        if topic and topic.lower() == "tp":
            embed = discord.Embed(
                title="Take-Profit Command — Detailed Help",
                description="View and manage auto take-profit thresholds.",
                color=0x00BFFF
            )
            embed.add_field(
                name="Show config",
                value=(
                    "`!tp config` — show all defaults and per-symbol overrides\n"
                    "`!tp config <symbol>` — show TP config for a specific symbol (e.g. `!tp config XAUUSD`)"
                ),
                inline=False
            )
            embed.add_field(
                name="Set TP threshold (admin)",
                value=(
                    "`!tp set <class> <value>` — set asset-class default (e.g. `!tp set metals 5`)\n"
                    "`!tp set <symbol> <value>` — set per-symbol override (e.g. `!tp set XAUUSD 5`)\n"
                    "`!tp set <target> <value> pips` — specify pips (e.g. `!tp set forex 10 pips`)\n"
                    "`!tp set <target> <value> dollars` — specify dollars (e.g. `!tp set XAUUSD 5 dollars`)\n\n"
                    "Valid asset classes: forex, forex_jpy, metals, indices, stocks, crypto, oil"
                ),
                inline=False
            )
            embed.add_field(
                name="Remove override (admin)",
                value="`!tp remove <symbol>` — remove per-symbol override, reverting to asset-class default (e.g. `!tp remove XAUUSD`)",
                inline=False
            )
            embed.set_footer(text="Auto-TP triggers when the last limit hits the threshold and earlier limits are combined breakeven.")
            await ctx.send(embed=embed)
            return

        if topic and topic.lower() in ("alertdist", "alertdistance", "adist"):
            embed = discord.Embed(
                title="Alert Distance Command — Detailed Help",
                description="View and manage the approaching-alert distance thresholds.\nThis controls how close price must get to a limit before an 'approaching' alert fires.",
                color=0x00BFFF
            )
            embed.add_field(
                name="Show config",
                value=(
                    "`!alertdist config` — show all asset-class defaults and per-symbol overrides\n"
                    "`!alertdist config <symbol>` — show config for a specific symbol (e.g. `!alertdist config XAUUSD`)"
                ),
                inline=False
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
                inline=False
            )
            embed.add_field(
                name="Remove per-symbol override (admin)",
                value="`!alertdist remove <symbol>` — remove override, reverting to asset-class default (e.g. `!alertdist remove XAUUSD`)",
                inline=False
            )
            embed.add_field(
                name="Distance types",
                value=(
                    "**pips** — used for forex pairs (e.g. 10 pips)\n"
                    "**dollars** — used for metals, oil (e.g. $8.00)\n"
                    "**percentage** — used for indices, crypto (e.g. 0.5%)"
                ),
                inline=False
            )
            embed.set_footer(text="If no type is specified, the existing type for that target is preserved.")
            await ctx.send(embed=embed)
            return

        if topic and topic.lower() in ("news",):
            embed = discord.Embed(
                title="📰 News Command — Detailed Help",
                description="Schedule news windows that auto-cancel signals hit during the window.",
                color=0x5865F2
            )
            embed.add_field(
                name="Schedule a news window",
                value=(
                    "`!news <category> <time> [window] [tz:<tz>] [date:<date>]`\n"
                    "Example: `!news USD 12:30pm 15` — USD news at 12:30 PM EST, ±15 min window\n"
                    "Example: `!news gold 8:30am tz:UTC` — Gold news at 8:30 AM UTC\n"
                    "Example: `!news all 14:00 30 date:2025-06-20` — All pairs on a specific date"
                ),
                inline=False
            )
            embed.add_field(
                name="Immediate / open-ended window",
                value=(
                    "`!news now` — activate news mode immediately for ALL pairs\n"
                    "`!news now USD` — activate immediately for USD pairs only\n"
                    "`!news off` — deactivate all open-ended windows"
                ),
                inline=False
            )
            embed.add_field(
                name="Tags (optional)",
                value=(
                    "`tz:<timezone>` — timezone for the time (default: EST)\n"
                    "  e.g. `tz:UTC`  `tz:GMT`  `tz:CET`  `tz:London`  `tz:JST`\n"
                    "`date:<date>` — specific date (default: today)\n"
                    "  e.g. `date:2025-06-15`  `date:06/15`  `date:tomorrow`"
                ),
                inline=False
            )
            embed.add_field(
                name="Categories",
                value=(
                    "Any currency code: `USD`, `EUR`, `GBP`, `JPY`, etc.\n"
                    "Named: `gold`, `oil`, `btc`, `eth`, `crypto`\n"
                    "`all` — affects every instrument"
                ),
                inline=False
            )
            embed.add_field(
                name="Managing events",
                value=(
                    "`!newslist` — show all scheduled and active events\n"
                    "`!newsclear <id>` — remove a specific event\n"
                    "`!newsclear` — remove all events"
                ),
                inline=False
            )
            embed.set_footer(text="Window is ±N minutes around the news time. Default window is 10 minutes.")
            await ctx.send(embed=embed)
            return

        # ── Default: main help page ──
        embed = discord.Embed(
            title="📚 Bot Commands",
            description="Available commands for the trading bot",
            color=0x00BFFF
        )

        # Signal commands
        signal_cmds = (
            "`!active [instrument] [sort:method]` - Show active signals\n"
            "  └ Sort options: recent, oldest, distance, progress\n"
            "`!signal` - Add manual signal\n"
            "`!delete <id>` - Delete signal\n"
            "`!info <id>` - Signal details\n"
            "`!report [day/week/month]` - Trading report\n"
            "`!tolls [day/week/month]` - Tolls trading report\n"
            "`!profit <id>` - Mark as profit\n"
            "`!sl <id>` - Mark as stop loss\n"
            "`!cancel` - Cancel signals — see `!help cancel` for all options\n"
            "`!tp` - Take-profit config — see `!help tp` for all options\n"
            "`!alertdist` - Alert distance config — see `!help alertdist` for all options"
        )
        embed.add_field(name="Signal Commands", value=signal_cmds, inline=False)

        # News commands
        news_cmds = (
            "`!news <category> <time> [window] [tz:<tz>] [date:<date>]` - Schedule news window\n"
            "`!news now [category]` - Activate immediate open-ended news window\n"
            "`!news off` - Deactivate all open-ended windows\n"
            "`!newslist` - Show all scheduled/active news events\n"
            "`!newsclear [id]` - Remove a news event (or all)\n"
            "  └ See `!help news` for full details and tag options"
        )
        embed.add_field(name="📰 News Mode", value=news_cmds, inline=False)

        # Bot commands
        bot_cmds = (
            "`!ping` - Check bot latency\n"
            "`!health` - Complete bot health check\n"
            "`!price <symbol>` - Check current price\n"
            "`!feeds` - Feed status overview"
        )
        embed.add_field(name="Bot Commands", value=bot_cmds, inline=False)

        # Admin commands
        if self.is_admin(ctx.author):
            admin_cmds = (
                "`!clear` - Clear all signals\n"
                "`!reload` - Reload configuration\n"
                "`!shutdown` - Shutdown bot"
            )
            embed.add_field(name="Admin Commands", value=admin_cmds, inline=False)

        await ctx.send(embed=embed)

    # ==================== PRICE & FEED COMMANDS ====================

    @commands.command(name='price', aliases=['cp', 'checkprice'])
    async def check_price(self, ctx: commands.Context, symbol: str):
        """Check current price for a symbol"""
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.send("❌ Price monitoring not available")
            return

        if not hasattr(self.bot.monitor, 'stream_manager'):
            await ctx.send("❌ Stream manager not available")
            return

        try:
            symbol_upper = symbol.upper()
            price_data = await self.bot.monitor.stream_manager.get_latest_price(symbol_upper)

            if not price_data:
                await ctx.send(f"❌ No price data available for {symbol_upper}")
                return

            embed = discord.Embed(
                title=f"{symbol_upper} Price",
                color=0x00FF00
            )

            embed.add_field(name="Bid", value=f"{price_data['bid']:.5f}", inline=True)
            embed.add_field(name="Ask", value=f"{price_data['ask']:.5f}", inline=True)

            spread = price_data['ask'] - price_data['bid']
            embed.add_field(name="Spread", value=f"{spread:.5f}", inline=True)

            if price_data.get('timestamp'):
                timestamp = price_data['timestamp']
                if isinstance(timestamp, (int, float)):
                    embed.set_footer(text=f"Updated: {datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}")

            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Error fetching price: {str(e)}")
            self.logger.error(f"Error in check_price: {e}")

    @commands.command(name='feeds', aliases=['feedstatus'])
    async def feed_status(self, ctx: commands.Context):
        """Show feed connection status"""
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.send("❌ Monitor not running")
            return

        if not hasattr(self.bot.monitor, 'stream_manager'):
            await ctx.send("❌ Stream manager not available")
            return

        stream_manager = self.bot.monitor.stream_manager

        embed = discord.Embed(
            title="📡 Feed Status",
            color=0x00BFFF
        )

        # Get feed statuses
        feeds_info = []
        for feed_name in ['icmarkets', 'oanda', 'binance']:
            if hasattr(stream_manager, f'{feed_name}_connected'):
                is_connected = getattr(stream_manager, f'{feed_name}_connected', False)
                status = "🟢 Connected" if is_connected else "🔴 Disconnected"
                feeds_info.append(f"**{feed_name.upper()}**: {status}")

        if feeds_info:
            embed.add_field(name="Feed Connections", value="\n".join(feeds_info), inline=False)

        # Active subscriptions
        if hasattr(stream_manager, 'subscribed_symbols'):
            active_subs = len(stream_manager.subscribed_symbols)
            embed.add_field(name="Active Subscriptions", value=str(active_subs), inline=True)

        await ctx.send(embed=embed)

    @commands.command(name='health')
    async def health_check(self, ctx: commands.Context):
        """Complete bot health check - feeds, database, monitoring"""
        embed = discord.Embed(
            title="Bot Health Check",
            description="Complete system status",
            color=0x00FF00
        )

        # Bot status
        uptime = datetime.utcnow() - self.bot.start_time if hasattr(self.bot, 'start_time') else None
        if uptime:
            hours = int(uptime.total_seconds() // 3600)
            minutes = int((uptime.total_seconds() % 3600) // 60)
            embed.add_field(name="⏱Uptime", value=f"{hours}h {minutes}m", inline=True)

        embed.add_field(name="Latency", value=f"{round(self.bot.latency * 1000)}ms", inline=True)

        # Database status
        try:
            stats = await self.signal_db.get_statistics()
            total_signals = stats.get('total_signals', 0)
            tracking = stats.get('tracking_count', 0)
            embed.add_field(
                name="Database",
                value=f"🟢 Connected\n{tracking} active / {total_signals} total",
                inline=True
            )
        except Exception as e:
            embed.add_field(name="Database", value=f"🔴 Error: {str(e)[:50]}", inline=True)

        # Monitor status
        if hasattr(self.bot, 'monitor') and self.bot.monitor:
            if hasattr(self.bot.monitor, 'stream_manager'):
                stream_manager = self.bot.monitor.stream_manager

                # Feed connections
                feed_status = []
                for feed_name in ['icmarkets', 'oanda', 'binance']:
                    if hasattr(stream_manager, f'{feed_name}_connected'):
                        is_connected = getattr(stream_manager, f'{feed_name}_connected', False)
                        emoji = "🟢" if is_connected else "🔴"
                        feed_status.append(f"{emoji} {feed_name.upper()}")

                if feed_status:
                    embed.add_field(
                        name="Feeds",
                        value="\n".join(feed_status),
                        inline=True
                    )

                # Subscriptions
                if hasattr(stream_manager, 'subscribed_symbols'):
                    active_subs = len(stream_manager.subscribed_symbols)
                    embed.add_field(name="Subscriptions", value=str(active_subs), inline=True)

                # Recent updates
                if hasattr(stream_manager, 'last_price_update'):
                    last_update = stream_manager.last_price_update
                    if last_update:
                        seconds_ago = (datetime.utcnow() - last_update).total_seconds()
                        status = "🟢 Active" if seconds_ago < 60 else "🟡 Slow"
                        embed.add_field(name="🔄 Last Update", value=f"{status}\n{int(seconds_ago)}s ago", inline=True)
            else:
                embed.add_field(name="Monitor", value="🔴 Stream manager unavailable", inline=False)
        else:
            embed.add_field(name="Monitor", value="🔴 Not running", inline=False)

        # Overall health color
        if all([
            hasattr(self.bot, 'monitor'),
            self.bot.monitor,
            hasattr(self.bot.monitor, 'stream_manager')
        ]):
            embed.color = 0x00FF00  # Green - healthy
        else:
            embed.color = 0xFFA500  # Orange - degraded

        embed.set_footer(text=f"Generated at {datetime.utcnow().strftime('%H:%M:%S')} UTC")
        await ctx.send(embed=embed)

    # ==================== ADMIN COMMANDS ====================

    def cog_check_admin(self, ctx: commands.Context) -> bool:
        """Check for admin commands"""
        return self.is_admin(ctx.author)

    @commands.command(name='clear')
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def clear_all_signals(self, ctx: commands.Context):
        """Clear all signals from database (Admin only)"""
        stats = await self.signal_db.get_statistics()
        total_signals = stats.get('total_signals', 0)

        if total_signals == 0:
            await ctx.send("✅ Database is already empty")
            return

        confirm_msg = await ctx.send(
            f"⚠️ **WARNING**: Delete **{total_signals}** signals?\n"
            f"React with ✅ to confirm or ❌ to cancel (30s timeout)"
        )

        await confirm_msg.add_reaction("✅")
        await confirm_msg.add_reaction("❌")

        def check(reaction, user):
            return (user == ctx.author and
                    str(reaction.emoji) in ["✅", "❌"] and
                    reaction.message.id == confirm_msg.id)

        try:
            reaction, user = await self.bot.wait_for('reaction_add', timeout=30.0, check=check)

            if str(reaction.emoji) == "✅":
                from database import db
                async with db.get_connection() as conn:
                    await conn.execute("DELETE FROM status_changes")
                    await conn.execute("DELETE FROM limits")
                    await conn.execute("DELETE FROM signals")

                await confirm_msg.edit(
                    content=f"✅ Deleted {total_signals} signals | Cleared by {ctx.author.name}"
                )
                self.logger.info(f"Database cleared by {ctx.author.name}")
            else:
                await confirm_msg.edit(content="❌ Clear cancelled")

            await confirm_msg.clear_reactions()

        except asyncio.TimeoutError:
            await confirm_msg.edit(content="⏱️ Clear timed out")
            await confirm_msg.clear_reactions()

    @commands.command(name='reload')
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def reload_config(self, ctx: commands.Context):
        """Reload bot configuration (Admin only)"""
        try:
            # Reload alert distances if available
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                if hasattr(self.bot.monitor, 'alert_config'):
                    self.bot.monitor.alert_config.reload_config()
                if hasattr(self.bot.monitor, 'tp_config'):
                    self.bot.monitor.tp_config.reload_config()
                    self.bot.monitor.tp_monitor.tp_config = self.bot.monitor.tp_config

            await ctx.send("✅ Configuration reloaded")
            self.logger.info(f"Config reloaded by {ctx.author.name}")
        except Exception as e:
            await ctx.send(f"❌ Error reloading config: {str(e)}")
            self.logger.error(f"Error reloading config: {e}")

    @commands.command(name='shutdown')
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def shutdown(self, ctx: commands.Context):
        """Shutdown the bot (Admin only)"""
        await ctx.send("👋 Shutting down...")
        self.logger.info(f"Bot shutdown initiated by {ctx.author.name}")
        await self.bot.close()


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(BotCommands(bot))