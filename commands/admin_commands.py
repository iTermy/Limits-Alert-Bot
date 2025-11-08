"""
Admin Commands - Administrative commands for bot management
Updated to work with streaming architecture
"""
from discord.ext import commands
import discord
import asyncio
from typing import Optional
from .base_command import BaseCog
from utils.embed_factory import EmbedFactory
from utils.config_loader import config


class AdminCommands(BaseCog):
    """Administrative commands for bot management"""

    def cog_check(self, ctx: commands.Context) -> bool:
        """Check if user has admin permissions for all commands in this cog"""
        return self.is_admin(ctx.author)

    @commands.command(name='clear')
    async def clear_all_signals(self, ctx: commands.Context):
        """Clear all signals from the database (Admin only)"""

        # Get current stats before clearing
        stats = await self.signal_db.get_statistics()
        total_signals = stats.get('total_signals', 0)

        if total_signals == 0:
            await ctx.reply("Database is already empty.")
            return

        # Ask for confirmation
        confirm_msg = await ctx.reply(
            f"⚠️ **WARNING**: This will delete **{total_signals}** signal(s) from the database.\n"
            f"React with ✅ to confirm or ❌ to cancel."
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
                # Clear all signals using the new method
                success = await self.clear_database()

                if success:
                    embed = discord.Embed(
                        title="🗑️ Database Cleared",
                        description=f"Successfully deleted {total_signals} signal(s)",
                        color=discord.Color.orange()
                    )
                    embed.set_footer(text=f"Cleared by {ctx.author.name}")
                    await confirm_msg.edit(content="", embed=embed)
                else:
                    await confirm_msg.edit(content="❌ Failed to clear database.")
            else:
                await confirm_msg.edit(content="❌ Clear operation cancelled.")

            # Remove reactions
            await confirm_msg.clear_reactions()

        except asyncio.TimeoutError:
            await confirm_msg.edit(content="❌ Clear operation timed out.")
            await confirm_msg.clear_reactions()

    async def clear_database(self) -> bool:
        """
        Clear all signals and limits from database

        Returns:
            Success status
        """
        try:
            # Import db from database module
            from database import db

            # Use the database execute method directly for clearing
            async with db.get_connection() as conn:
                # Delete all status changes first
                await conn.execute("DELETE FROM status_changes")
                # Delete all limits
                await conn.execute("DELETE FROM limits")
                # Delete all signals
                await conn.execute("DELETE FROM signals")
                await conn.commit()

            self.logger.info("Cleared all signals from database")
            return True

        except Exception as e:
            self.logger.error(f"Error clearing database: {e}", exc_info=True)
            return False

    @commands.command(name='expire')
    async def expire_signals(self, ctx: commands.Context):
        """Manually trigger expiry check for old signals"""
        try:
            count = await asyncio.wait_for(
                self.signal_db.expire_old_signals(),
                timeout=10.0  # Allow up to 10 seconds
            )

            if count > 0:
                embed = discord.Embed(
                    title="⏰ Signals Expired",
                    description=f"Expired {count} signal(s) past their expiry time",
                    color=discord.Color.orange()
                )
            else:
                embed = discord.Embed(
                    title="⏰ No Expired Signals",
                    description="No signals needed to be expired",
                    color=discord.Color.green()
                )

            await ctx.send(embed=embed)

        except asyncio.TimeoutError:
            await ctx.reply("❌ Expire operation timed out.")

    @commands.command(name='reload')
    async def reload_config(self, ctx: commands.Context):
        """Reload configuration files"""
        try:
            config.reload_all()
            self.bot.channels_config = config.load("channels.json")
            self.bot.settings = config.load("settings.json")

            # Update monitored channels
            self.bot.monitored_channels.clear()
            for channel_name, channel_id in self.bot.channels_config.get("monitored_channels", {}).items():
                if channel_id:
                    self.bot.monitored_channels.add(int(channel_id))

            # Update alert channel
            alert_id = self.bot.channels_config.get("alert_channel")
            if alert_id:
                self.bot.alert_channel_id = int(alert_id)

            # Update command channel
            command_id = self.bot.channels_config.get("command_channel")
            if command_id:
                self.bot.command_channel_id = int(command_id)

            embed = EmbedFactory.success(
                title="✅ Configuration Reloaded",
                description="All configuration files have been reloaded successfully"
            )
            await ctx.send(embed=embed)
            self.logger.info("Configuration reloaded by user")

        except Exception as e:
            embed = EmbedFactory.error(
                title="Reload Failed",
                description=f"Error: {str(e)}"
            )
            await ctx.send(embed=embed)
            self.logger.error(f"Configuration reload failed: {e}")

    @commands.command(name='shutdown')
    async def shutdown(self, ctx: commands.Context):
        """Gracefully shutdown the bot"""
        embed = EmbedFactory.warning(
            title="Bot Shutdown",
            description="Shutting down bot..."
        )
        await ctx.send(embed=embed)

        self.logger.info(f"Bot shutdown initiated by {ctx.author.name}")
        await self.bot.close()

    @commands.command(name='logs')
    async def show_logs(self, ctx: commands.Context, lines: int = 10):
        """Show recent log entries"""
        if lines > 50:
            lines = 50  # Limit to prevent spam

        try:
            with open('data/logs/bot.log', 'r') as f:
                log_lines = f.readlines()[-lines:]

            log_text = ''.join(log_lines[-lines:])

            # Truncate if too long for Discord
            if len(log_text) > 1900:
                log_text = log_text[-1900:]

            embed = discord.Embed(
                title=f"📋 Recent Logs ({lines} lines)",
                description=f"```{log_text}```",
                color=discord.Color.light_gray()
            )
            await ctx.send(embed=embed)

        except FileNotFoundError:
            await ctx.reply("❌ Log file not found.")
        except Exception as e:
            await ctx.reply(f"❌ Error reading logs: {str(e)}")

    @commands.command(name='monitor')
    async def monitor_control(self, ctx, action: str = None):
        """Control price monitoring

        Usage:
            !monitor status - Show monitoring status
            !monitor start - Start monitoring
            !monitor stop - Stop monitoring
            !monitor stats - Show statistics
        """
        if not self.bot.monitor:
            await ctx.send("❌ Price monitor not initialized")
            return

        if action == 'status':
            status = "🟢 Running" if self.bot.monitor.running else "🔴 Stopped"
            await ctx.send(f"Monitor Status: {status}")

        elif action == 'start':
            if self.bot.monitor.running:
                await ctx.send("Monitor already running")
            else:
                await self.bot.monitor.start()
                await ctx.send("✅ Price monitoring started")

        elif action == 'stop':
            if not self.bot.monitor.running:
                await ctx.send("Monitor already stopped")
            else:
                await self.bot.monitor.stop()
                await ctx.send("✅ Price monitoring stopped")

        elif action == 'stats':
            stats = self.bot.monitor.get_stats()

            embed = discord.Embed(
                title="📊 Monitoring Statistics",
                color=0x00FF00 if stats['running'] else 0xFF0000
            )

            embed.add_field(
                name="Status",
                value="🟢 Running" if stats['running'] else "🔴 Stopped",
                inline=True
            )
            embed.add_field(
                name="Checks Performed",
                value=f"{stats['checks_performed']:,}",
                inline=True
            )
            embed.add_field(
                name="Alerts Sent",
                value=f"{stats['alerts_sent']:,}",
                inline=True
            )
            embed.add_field(
                name="Limits Hit",
                value=f"{stats['limits_hit']:,}",
                inline=True
            )
            embed.add_field(
                name="Errors",
                value=f"{stats['errors']:,}",
                inline=True
            )

            # Add streaming stats
            if 'stream_manager' in stats:
                stream_stats = stats['stream_manager']
                embed.add_field(
                    name="📡 Streaming",
                    value=f"Subscriptions: {stream_stats.get('subscribed_symbols', 0)}\n"
                          f"Updates: {stream_stats.get('updates_received', 0):,}\n"
                          f"Reconnections: {stream_stats.get('reconnections', 0)}",
                    inline=False
                )

            await ctx.send(embed=embed)

        else:
            await ctx.send("Usage: !monitor [status|start|stop|stats]")

    @commands.command(name='refresh')
    async def refresh_streams(self, ctx: commands.Context):
        """Refresh all streaming connections (reconnect all feeds)"""
        if not self.bot.monitor:
            await ctx.send("❌ Price monitor not initialized")
            return

        await ctx.send("🔄 Refreshing all streaming connections...")

        try:
            # Reconnect all feeds
            if hasattr(self.bot.monitor, 'stream_manager'):
                await self.bot.monitor.stream_manager.reconnect_all()

                embed = discord.Embed(
                    title="✅ Streams Refreshed",
                    description="All streaming connections have been refreshed",
                    color=discord.Color.green()
                )

                # Get updated feed status
                feed_status = self.bot.monitor.stream_manager.feed_status
                status_str = "\n".join([
                    f"{'✅' if connected else '❌'} {feed.upper()}"
                    for feed, connected in feed_status.items()
                ])

                embed.add_field(
                    name="Feed Status",
                    value=status_str,
                    inline=False
                )

                await ctx.send(embed=embed)
            else:
                await ctx.send("❌ Stream manager not available")

        except Exception as e:
            self.logger.error(f"Error refreshing streams: {e}")
            await ctx.send(f"❌ Error refreshing streams: {str(e)}")

    @commands.command(name='reconnect')
    async def reconnect_feed(self, ctx: commands.Context, feed_name: str = None):
        """Reconnect a specific feed

        Usage: !reconnect <feed_name>
        Valid feeds: icmarkets, oanda, binance
        """
        if not feed_name:
            await ctx.reply("Please specify a feed: icmarkets, oanda, or binance")
            return

        feed_name = feed_name.lower()
        valid_feeds = ['icmarkets', 'oanda', 'binance']

        if feed_name not in valid_feeds:
            await ctx.reply(f"❌ Invalid feed. Valid options: {', '.join(valid_feeds)}")
            return

        if not self.bot.monitor or not hasattr(self.bot.monitor, 'stream_manager'):
            await ctx.send("❌ Stream manager not initialized")
            return

        try:
            feed = self.bot.monitor.stream_manager.feeds.get(feed_name)
            if not feed:
                await ctx.send(f"❌ Feed '{feed_name}' not found")
                return

            await ctx.send(f"🔄 Reconnecting {feed_name.upper()}...")

            await feed.reconnect()

            # Check if reconnection was successful
            if feed.connected:
                embed = discord.Embed(
                    title=f"✅ {feed_name.upper()} Reconnected",
                    description=f"Successfully reconnected to {feed_name.upper()}",
                    color=discord.Color.green()
                )

                # Show subscribed symbols
                subscribed = feed.get_subscribed_symbols()
                if subscribed:
                    embed.add_field(
                        name="Subscribed Symbols",
                        value=f"{len(subscribed)} symbols",
                        inline=True
                    )

                await ctx.send(embed=embed)
            else:
                await ctx.send(f"⚠️ Reconnection initiated but {feed_name.upper()} not yet connected")

        except Exception as e:
            self.logger.error(f"Error reconnecting {feed_name}: {e}")
            await ctx.send(f"❌ Error reconnecting {feed_name}: {str(e)}")

    @commands.command(name='streamhealth')
    async def stream_health(self, ctx: commands.Context):
        """Show detailed health metrics for all streaming feeds"""
        if not self.bot.monitor or not hasattr(self.bot.monitor, 'stream_manager'):
            await ctx.send("❌ Stream manager not initialized")
            return

        try:
            stream_manager = self.bot.monitor.stream_manager
            stats = stream_manager.get_stats()

            embed = discord.Embed(
                title="📡 Streaming Health Dashboard",
                description="Real-time health metrics for all price feeds",
                color=discord.Color.blue()
            )

            # Overall statistics
            embed.add_field(
                name="📊 Overall Statistics",
                value=f"**Subscribed Symbols:** {stats.get('subscribed_symbols', 0)}\n"
                      f"**Connected Feeds:** {stats.get('connected_feeds', 0)}/3\n"
                      f"**Total Updates:** {stats.get('updates_received', 0):,}\n"
                      f"**Total Reconnections:** {stats.get('reconnections', 0)}",
                inline=False
            )

            # Individual feed health
            feed_icons = {
                'icmarkets': '🏦',
                'oanda': '💱',
                'binance': '₿'
            }

            for feed_name, feed in stream_manager.feeds.items():
                icon = feed_icons.get(feed_name, '📈')
                connected = feed.connected
                status_emoji = '🟢' if connected else '🔴'

                subscribed = feed.get_subscribed_symbols()

                field_value = f"{status_emoji} **Status:** {'Connected' if connected else 'Disconnected'}\n"
                field_value += f"📊 **Subscriptions:** {len(subscribed)}\n"

                # Add feed-specific stats if available
                if hasattr(feed, 'get_stats'):
                    feed_stats = feed.get_stats()
                    if 'updates_received' in feed_stats:
                        field_value += f"📥 **Updates:** {feed_stats['updates_received']:,}\n"
                    if 'reconnections' in feed_stats:
                        field_value += f"🔄 **Reconnections:** {feed_stats['reconnections']}\n"

                embed.add_field(
                    name=f"{icon} {feed_name.upper()}",
                    value=field_value,
                    inline=True
                )

            embed.set_footer(text=f"Monitor running: {self.bot.monitor.running}")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error getting stream health: {e}")
            await ctx.send(f"❌ Error getting stream health: {str(e)}")

    @commands.command(name='testmonitor')
    async def test_monitor(self, ctx, signal_id: int):
        """Test monitoring for a specific signal

        Usage: !testmonitor <signal_id>
        """
        if not self.bot.monitor:
            await ctx.send("❌ Price monitor not initialized")
            return

        await ctx.send(f"Testing monitor for signal #{signal_id}...")


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(AdminCommands(bot))