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
            "`!alertdist` - Alert distance config — see `!help alertdist` for all options\n"
            "`!news` - News mode — see `!help news` for all options"
        )
        embed.add_field(name="Signal Commands", value=signal_cmds, inline=False)

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
                "`!shutdown` - Shutdown bot\n"
                "`!goldtollssl [value]` - Get/set gold tolls SL offset ($)"
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

            # Bust the gold-tolls SL offset cache so the parser picks up any
            # manual edits to settings.json
            from core.parser.pattern_parsers import invalidate_gold_tolls_sl_cache
            invalidate_gold_tolls_sl_cache()

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

    @commands.command(name='goldtollssl', aliases=['gtsl', 'goldtollsl'])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def gold_tolls_sl(self, ctx: commands.Context, value: float = None):
        """
        Get or set the gold-tolls stop-loss offset (dollars from the nearest limit).
        Usage:  !goldtollssl          → show current value
                !goldtollssl 10       → set offset to $10
                !goldtollssl 5        → reset to default $5
        """
        from utils.config_loader import load_settings, save_settings
        from core.parser.pattern_parsers import get_gold_tolls_sl_offset, invalidate_gold_tolls_sl_cache

        if value is None:
            # Show current setting
            current = get_gold_tolls_sl_offset()
            embed = discord.Embed(
                title="🛑 Gold Tolls SL Offset",
                description=(
                    f"**Current offset:** `${current:.2f}` from the nearest limit\n\n"
                    "Gold-tolls stop losses are automatically placed this many dollars "
                    "beyond the nearest limit.\n\n"
                    f"**Long signals:** SL = lowest limit − `${current:.2f}`\n"
                    f"**Short signals:** SL = highest limit + `${current:.2f}`\n\n"
                    "_Use `!goldtollssl <value>` to change it._"
                ),
                color=discord.Color.blue(),
            )
            await ctx.send(embed=embed)
            return

        if value <= 0:
            await ctx.send("❌ Offset must be a positive number.")
            return

        # Save to settings.json
        try:
            settings = load_settings()
            old_value = settings.get("gold_tolls_sl_offset", 5.0)
            settings["gold_tolls_sl_offset"] = value
            save_settings(settings)
            invalidate_gold_tolls_sl_cache()
        except Exception as e:
            await ctx.send(f"❌ Failed to save setting: {e}")
            self.logger.error(f"Failed to save gold_tolls_sl_offset: {e}", exc_info=True)
            return

        self.logger.info(
            f"gold_tolls_sl_offset changed {old_value} → {value} by {ctx.author}"
        )

        # ── Retroactively update all active gold toll signals ─────────────────
        loading_msg = await ctx.send("🔄 Updating active gold toll signals…")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        try:
            monitor = getattr(self.bot, "monitor", None)
            alert_system = monitor.alert_system if monitor else None

            # Identify gold-toll channel IDs from the alert system
            toll_channel_ids: set = set()
            if alert_system and hasattr(alert_system, "toll_channel_ids"):
                toll_channel_ids = alert_system.toll_channel_ids  # already str

            if not toll_channel_ids:
                # Fallback: derive directly from channels config
                from utils.config_loader import load_channels_config
                channels_cfg = load_channels_config()
                monitored = channels_cfg.get("monitored_channels", {})
                for ch_name, ch_id in monitored.items():
                    if ch_id and "toll" in ch_name.lower() and ch_name.lower() != "general-tolls":
                        toll_channel_ids.add(str(ch_id))

            if not toll_channel_ids:
                await loading_msg.edit(content="⚠️ No gold-toll channels found — offset saved but no signals updated.")
            else:
                from database import db

                # Fetch all active/hit gold-toll signals with their pending limits
                toll_ch_list = list(toll_channel_ids)
                placeholders = ", ".join(f"${i+1}" for i in range(len(toll_ch_list)))
                query = f"""
                    SELECT
                        s.id,
                        s.direction,
                        s.stop_loss,
                        s.channel_id,
                        ARRAY_AGG(l.price_level ORDER BY l.sequence_number) FILTER (WHERE l.id IS NOT NULL) AS all_price_levels
                    FROM signals s
                    LEFT JOIN limits l ON l.signal_id = s.id
                    WHERE s.status IN ('active', 'hit')
                      AND CAST(s.channel_id AS TEXT) IN ({placeholders})
                    GROUP BY s.id, s.direction, s.stop_loss, s.channel_id
                """
                async with db.get_connection() as conn:
                    rows = await conn.fetch(query, *toll_ch_list)

                for row in rows:
                    sig_id = row["id"]
                    direction = row["direction"]
                    price_levels = row["all_price_levels"] or []

                    if not price_levels:
                        skipped_count += 1
                        continue

                    # Recompute SL with new offset
                    if direction == "long":
                        new_sl = min(price_levels) - value
                    else:
                        new_sl = max(price_levels) + value

                    # Skip if SL hasn't changed (avoid unnecessary DB writes / embed edits)
                    old_sl = row["stop_loss"]
                    if old_sl is not None and abs(float(old_sl) - new_sl) < 0.001:
                        skipped_count += 1
                        continue

                    try:
                        # 1. Persist new SL to DB
                        async with db.get_connection() as conn:
                            await conn.execute(
                                "UPDATE signals SET stop_loss = $1 WHERE id = $2",
                                new_sl, sig_id
                            )

                        # 2. Update streaming monitor in-memory state so price
                        #    checks use the new SL immediately
                        if monitor and hasattr(monitor, "active_signals"):
                            mem_sig = monitor.active_signals.get(sig_id)
                            if mem_sig:
                                mem_sig["stop_loss"] = new_sl

                        # 3. Update the persistent embed (only if one exists)
                        if alert_system:
                            await alert_system.update_embed_for_signal_id(
                                sig_id, "edited"
                            )

                        updated_count += 1
                        self.logger.info(
                            f"Retroactively updated SL for gold-toll signal {sig_id}: "
                            f"{old_sl} → {new_sl} (offset={value}, dir={direction})"
                        )

                    except Exception as sig_err:
                        error_count += 1
                        self.logger.error(
                            f"Failed to update SL for gold-toll signal {sig_id}: {sig_err}",
                            exc_info=True,
                        )

                await loading_msg.delete()

        except Exception as bulk_err:
            self.logger.error(f"Retroactive gold-toll SL update failed: {bulk_err}", exc_info=True)
            await loading_msg.edit(content=f"⚠️ Retroactive update encountered an error: {bulk_err}")

        # ── Final confirmation embed ──────────────────────────────────────────
        retro_lines = []
        if updated_count:
            retro_lines.append(f"✅ **{updated_count}** active signal(s) SL updated retroactively")
        if skipped_count:
            retro_lines.append(f"⏭️ **{skipped_count}** signal(s) skipped (no change / no limits)")
        if error_count:
            retro_lines.append(f"⚠️ **{error_count}** signal(s) failed to update (see logs)")
        if not retro_lines:
            retro_lines.append("ℹ️ No active gold-toll signals found to update")

        embed = discord.Embed(
            title="✅ Gold Tolls SL Offset Updated",
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

    # ==================== LICENSE COMMANDS ====================

    # The Discord role that grants access to the Auto-Limits-Adder bot.
    # Must exactly match the role name in your server (case-sensitive).
    LICENSE_ROLE_NAME = "Signal Subscriber"

    # Seconds to wait for the user to reply with their MT5 account in DM.
    LICENSE_DM_TIMEOUT = 120

    @staticmethod
    def _generate_license_key() -> str:
        import secrets
        return secrets.token_hex(16)

    @commands.command(name="activate")
    async def activate(self, ctx: commands.Context):
        """Get an Auto-Limits-Adder license key (requires Signal Subscriber role)"""
        # Delete the public command message so the channel stays clean
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        member = ctx.author

        # Role check
        role_names = [r.name for r in getattr(member, "roles", [])]
        if not any(self.LICENSE_ROLE_NAME in r for r in role_names):
            try:
                await member.send(
                    f"❌ You need the **{self.LICENSE_ROLE_NAME}** role to activate a license.\n"
                    "If you are a subscriber, please contact an admin."
                )
            except discord.Forbidden:
                pass
            return

        # Open DM channel
        try:
            await member.send(
                "👋 **Auto-Limits-Adder License Activation**\n\n"
                "To activate your license, please reply here with your **MT5 account number** "
                "(the login number shown in MetaTrader 5).\n\n"
                "_Type `cancel` at any time to abort._"
            )
        except discord.Forbidden:
            try:
                notice = await ctx.send(
                    f"❌ {member.mention} I can't DM you. "
                    "Please **enable DMs from server members** in your Privacy Settings, "
                    "then run `!activate` again."
                )
                await asyncio.sleep(20)
                await notice.delete()
            except discord.HTTPException:
                pass
            return

        def dm_check(m: discord.Message) -> bool:
            return m.author.id == member.id and isinstance(m.channel, discord.DMChannel)

        try:
            reply = await self.bot.wait_for("message", check=dm_check, timeout=self.LICENSE_DM_TIMEOUT)
        except asyncio.TimeoutError:
            await member.send(
                f"⏰ Activation timed out after {self.LICENSE_DM_TIMEOUT}s. "
                "Run `!activate` in the server again when you're ready."
            )
            return

        if reply.content.strip().lower() == "cancel":
            await member.send("❌ Activation cancelled.")
            return

        mt5_account = reply.content.strip()
        if not mt5_account:
            await member.send("❌ No account number received. Please run `!activate` again.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            # User already has one or more active keys
            existing_keys = await conn.fetch(
                "SELECT mt5_account, license_key FROM licenses WHERE discord_id = $1 AND status = 'active'",
                str(member.id),
            )
            if existing_keys:
                accounts = ", ".join(f"`{r['mt5_account']}`" for r in existing_keys)
                await member.send(
                    f"ℹ️ You already have an active license for MT5 account(s): {accounts}.\n\n"
                    "If you need to register a **different** MT5 account, contact an admin "
                    "to revoke your existing key first.\n"
                    "If you've **lost your key**, ask an admin to re-issue it with `!grantkey`."
                )
                return

            # MT5 account already registered to someone else
            existing_owner = await conn.fetchrow(
                "SELECT discord_id FROM licenses WHERE mt5_account = $1 AND status = 'active'",
                mt5_account,
            )
            if existing_owner:
                await member.send(
                    f"❌ MT5 account `{mt5_account}` is already registered to another user.\n"
                    "If you believe this is an error, please contact an admin."
                )
                return

            license_key = self._generate_license_key()
            await conn.execute(
                """
                INSERT INTO licenses (discord_id, mt5_account, license_key, status, created_at)
                VALUES ($1, $2, $3, 'active', NOW())
                """,
                str(member.id), mt5_account, license_key,
            )

        self.logger.info(f"License issued — user={member} ({member.id}), mt5={mt5_account}")
        await member.send(
            f"✅ **License activated!**\n\n"
            f"**Your license key:**\n```\n{license_key}\n```\n\n"
            f"**Setup:**\n"
            f"1. Open `config.json` in your Auto-Limits-Adder folder.\n"
            f"2. Add your key to the \"license\" section:\n"
            f"```json\n\"license\": {{\n    \"key\": \"{license_key}\"\n}}\n```\n"
            f"3. Save and start the bot — it validates on startup.\n\n"
            f"⚠️ Keep this key private. It is locked to MT5 account `{mt5_account}`."
        )

    @commands.command(name="setkeys")
    async def setkeys(self, ctx: commands.Context, member: discord.Member, max_keys: int):
        """Admin: set how many license keys a user may hold. Usage: !setkeys @user <n>"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return
        if max_keys < 1:
            await ctx.send("❌ max_keys must be at least 1.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO license_allowances (discord_id, max_keys)
                VALUES ($1, $2)
                ON CONFLICT (discord_id) DO UPDATE SET max_keys = EXCLUDED.max_keys
                """,
                str(member.id), max_keys,
            )

        self.logger.info(f"Allowance updated — user={member} ({member.id}), max_keys={max_keys}")
        await ctx.send(f"✅ **{member.display_name}** can now hold up to **{max_keys}** license key(s).")

    @commands.command(name="grantkey")
    async def grantkey(self, ctx: commands.Context, member: discord.Member, mt5_account: str):
        """Admin: issue a license key for a specific MT5 account. Usage: !grantkey @user <mt5_account>"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            existing = await conn.fetchrow(
                "SELECT discord_id FROM licenses WHERE mt5_account = $1 AND status = 'active'",
                mt5_account,
            )
            if existing:
                await ctx.send(
                    f"❌ MT5 account `{mt5_account}` is already registered "
                    f"to Discord user ID `{existing['discord_id']}`."
                )
                return

            active_count = await conn.fetchval(
                "SELECT COUNT(*) FROM licenses WHERE discord_id = $1 AND status = 'active'",
                str(member.id),
            )
            max_keys = await conn.fetchval(
                "SELECT max_keys FROM license_allowances WHERE discord_id = $1",
                str(member.id),
            ) or 1

            if active_count >= max_keys:
                await ctx.send(
                    f"❌ {member.display_name} already has {active_count}/{max_keys} active license(s). "
                    f"Use `!setkeys @{member.display_name} {active_count + 1}` to raise their limit first."
                )
                return

            license_key = self._generate_license_key()
            await conn.execute(
                """
                INSERT INTO licenses (discord_id, mt5_account, license_key, status, created_at)
                VALUES ($1, $2, $3, 'active', NOW())
                """,
                str(member.id), mt5_account, license_key,
            )

        self.logger.info(f"License granted by admin — user={member} ({member.id}), mt5={mt5_account}, by={ctx.author}")
        await ctx.send(f"✅ License issued for **{member.display_name}** (MT5: `{mt5_account}`). Key sent to their DMs.")

        try:
            await member.send(
                f"🔑 An admin has issued you an **Auto-Limits-Adder** license key.\n\n"
                f"**Your license key:**\n```\n{license_key}\n```\n\n"
                f"Add this to `config.json` under the `\"license\"` section:\n"
                f"```json\n\"license\": {{\n    \"key\": \"{license_key}\"\n}}\n```\n"
                f"This key is locked to MT5 account `{mt5_account}`. Keep it private."
            )
        except discord.Forbidden:
            await ctx.send(
                f"⚠️ Could not DM {member.display_name} — share the key manually:\n```\n{license_key}\n```"
            )

    @commands.command(name="revoke")
    async def revoke(self, ctx: commands.Context, member: discord.Member, mt5_account: str = None):
        """Admin: revoke a license. Usage: !revoke @user [mt5_account]"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            if mt5_account:
                row = await conn.fetchrow(
                    "SELECT id FROM licenses WHERE discord_id = $1 AND mt5_account = $2 AND status = 'active'",
                    str(member.id), mt5_account,
                )
                if not row:
                    await ctx.send(f"❌ No active license found for {member.display_name} with MT5 account `{mt5_account}`.")
                    return
                await conn.execute(
                    "UPDATE licenses SET status = 'revoked', revoked_at = NOW() WHERE id = $1",
                    row["id"],
                )
                await ctx.send(f"✅ License revoked for **{member.display_name}** (MT5: `{mt5_account}`).")
            else:
                rows = await conn.fetch(
                    "SELECT id, mt5_account FROM licenses WHERE discord_id = $1 AND status = 'active'",
                    str(member.id),
                )
                if len(rows) == 0:
                    await ctx.send(f"❌ {member.display_name} has no active licenses.")
                    return
                if len(rows) > 1:
                    accounts = ", ".join(f"`{r['mt5_account']}`" for r in rows)
                    await ctx.send(
                        f"❌ {member.display_name} has {len(rows)} active licenses ({accounts}). "
                        f"Specify the MT5 account: `!revoke @user <mt5_account>`"
                    )
                    return
                row = rows[0]
                await conn.execute(
                    "UPDATE licenses SET status = 'revoked', revoked_at = NOW() WHERE id = $1",
                    row["id"],
                )
                await ctx.send(f"✅ License revoked for **{member.display_name}** (MT5: `{row['mt5_account']}`).")

        self.logger.info(f"License revoked — user={member} ({member.id}), mt5={mt5_account or 'auto'}, by={ctx.author}")

    @commands.command(name="licenses")
    async def licenses(self, ctx: commands.Context, member: discord.Member = None):
        """Admin: list licenses. Usage: !licenses  OR  !licenses @user"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            if member:
                rows = await conn.fetch(
                    "SELECT mt5_account, license_key, status, created_at FROM licenses "
                    "WHERE discord_id = $1 ORDER BY created_at DESC",
                    str(member.id),
                )
                max_keys = await conn.fetchval(
                    "SELECT max_keys FROM license_allowances WHERE discord_id = $1", str(member.id)
                ) or 1
                active_count = sum(1 for r in rows if r["status"] == "active")

                embed = discord.Embed(
                    title=f"🔑 Licenses — {member.display_name}",
                    description=f"Allowance: **{active_count}/{max_keys}** active keys",
                    color=discord.Color.blue(),
                )
                if not rows:
                    embed.add_field(name="No licenses found", value="\u200b", inline=False)
                for row in rows:
                    status_emoji = "✅" if row["status"] == "active" else "❌"
                    created = row["created_at"].strftime("%Y-%m-%d") if row["created_at"] else "?"
                    embed.add_field(
                        name=f"{status_emoji} MT5: `{row['mt5_account']}`",
                        value=f"Key: `{row['license_key'][:8]}...` | Created: {created} | {row['status']}",
                        inline=False,
                    )
                await ctx.send(embed=embed)

            else:
                rows = await conn.fetch(
                    """
                    SELECT l.discord_id, l.mt5_account, l.license_key, l.created_at,
                           COALESCE(la.max_keys, 1) AS max_keys
                    FROM licenses l
                    LEFT JOIN license_allowances la ON la.discord_id = l.discord_id
                    WHERE l.status = 'active'
                    ORDER BY l.discord_id, l.created_at
                    """
                )
                if not rows:
                    await ctx.send("ℹ️ No active licenses.")
                    return

                from collections import defaultdict
                grouped: dict = defaultdict(list)
                for row in rows:
                    grouped[row["discord_id"]].append(row)

                embed = discord.Embed(
                    title="🔑 All Active Licenses",
                    description=f"{len(rows)} license(s) across {len(grouped)} user(s)",
                    color=discord.Color.green(),
                )
                for discord_id, user_rows in grouped.items():
                    guild_member = ctx.guild.get_member(int(discord_id)) if ctx.guild else None
                    display_name = guild_member.display_name if guild_member else f"ID:{discord_id}"
                    max_k = user_rows[0]["max_keys"]
                    lines = [
                        f"• MT5 `{r['mt5_account']}` — key `{r['license_key'][:8]}...` — "
                        f"{r['created_at'].strftime('%Y-%m-%d') if r['created_at'] else '?'}"
                        for r in user_rows
                    ]
                    embed.add_field(
                        name=f"{display_name} ({len(user_rows)}/{max_k})",
                        value="\n".join(lines),
                        inline=False,
                    )
                    if len(embed.fields) >= 25:
                        embed.set_footer(text="Showing first 25 users — use !licenses @user for details.")
                        break

                await ctx.send(embed=embed)

    # ==================== LICENSE AUTO-MANAGEMENT ====================

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """
        Auto-revoke licenses when Signal Subscriber role is removed.
        Auto-reactivate licenses (if previously revoked by this system) when role is re-added.
        """
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}

        had_role = self.LICENSE_ROLE_NAME in before_roles
        has_role = self.LICENSE_ROLE_NAME in after_roles

        if had_role == has_role:
            return  # No change in the relevant role

        if had_role and not has_role:
            # Role was REMOVED — revoke all active licenses
            await self._auto_revoke_licenses(after, reason="role_removed")

        elif not had_role and has_role:
            # Role was ADDED (or re-added) — reactivate any role-revoked licenses
            await self._auto_reactivate_licenses(after)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Auto-revoke licenses when a member leaves the server."""
        await self._auto_revoke_licenses(member, reason="left_server")

    async def _auto_revoke_licenses(self, member: discord.Member, reason: str) -> None:
        """Revoke all active licenses for *member* and notify them by DM."""
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                rows = await conn.fetch(
                    "SELECT id, mt5_account FROM licenses WHERE discord_id = $1 AND status = 'active'",
                    str(member.id),
                )
                if not rows:
                    return

                for row in rows:
                    await conn.execute(
                        """
                        UPDATE licenses
                        SET status = 'revoked', revoked_at = NOW(),
                            revoked_reason = $1
                        WHERE id = $2
                        """,
                        reason, row["id"],
                    )

            self.logger.info(
                f"Auto-revoked {len(rows)} license(s) for {member} ({member.id}) — reason={reason}"
            )

            # Notify the user by DM (best-effort)
            if reason == "role_removed":
                msg = (
                    "⚠️ **Your Auto-Limits-Adder license has been revoked.**\n\n"
                    f"Your **{self.LICENSE_ROLE_NAME}** role was removed, so your license key "
                    "has been automatically deactivated.\n\n"
                    "If you regain subscriber access your license will be reactivated automatically. "
                    "Contact an admin if you believe this is a mistake."
                )
            else:
                msg = (
                    "⚠️ **Your Auto-Limits-Adder license has been revoked.**\n\n"
                    "Your license was deactivated because you left the server. "
                    "If you rejoin and regain subscriber access, contact an admin to restore your license."
                )

            try:
                await member.send(msg)
            except discord.Forbidden:
                pass  # User has DMs disabled — log only

        except Exception as e:
            self.logger.error(f"Failed to auto-revoke licenses for {member} ({member.id}): {e}", exc_info=True)

    async def _auto_reactivate_licenses(self, member: discord.Member) -> None:
        """
        Reactivate licenses previously revoked by the auto-revoke system
        (i.e. revoked_reason = 'role_removed') when the subscriber role is restored.
        """
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, mt5_account, license_key
                    FROM licenses
                    WHERE discord_id = $1
                      AND status = 'revoked'
                      AND revoked_reason = 'role_removed'
                    ORDER BY revoked_at DESC
                    """,
                    str(member.id),
                )
                if not rows:
                    return

                for row in rows:
                    await conn.execute(
                        """
                        UPDATE licenses
                        SET status = 'active', revoked_at = NULL, revoked_reason = NULL
                        WHERE id = $1
                        """,
                        row["id"],
                    )

            self.logger.info(
                f"Auto-reactivated {len(rows)} license(s) for {member} ({member.id})"
            )

            # Notify the user by DM
            keys_info = "\n".join(
                f"• MT5 `{r['mt5_account']}` → key `{r['license_key'][:8]}…`"
                for r in rows
            )
            try:
                await member.send(
                    f"✅ **Your Auto-Limits-Adder license has been reactivated!**\n\n"
                    f"Your **{self.LICENSE_ROLE_NAME}** role was restored, so your license key(s) "
                    f"are active again:\n{keys_info}\n\n"
                    "No changes needed in your config — just (re)start the Auto-Limits-Adder bot."
                )
            except discord.Forbidden:
                pass

        except Exception as e:
            self.logger.error(f"Failed to auto-reactivate licenses for {member} ({member.id}): {e}", exc_info=True)


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(BotCommands(bot))