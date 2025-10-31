"""
Admin Commands - Administrative commands for bot management
Fixed to work with new enhanced database structure
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
        if not self.bot.price_monitor:
            await ctx.send("❌ Price monitor not initialized")
            return

        if action == 'status':
            status = "🟢 Running" if self.bot.price_monitor.running else "🔴 Stopped"
            await ctx.send(f"Monitor Status: {status}")

        elif action == 'start':
            if self.bot.price_monitor.running:
                await ctx.send("Monitor already running")
            else:
                await self.bot.price_monitor.start()
                await ctx.send("✅ Price monitoring started")

        elif action == 'stop':
            if not self.bot.price_monitor.running:
                await ctx.send("Monitor already stopped")
            else:
                await self.bot.price_monitor.stop()
                await ctx.send("✅ Price monitoring stopped")

        elif action == 'stats':
            stats = self.bot.price_monitor.get_stats()

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
            embed.add_field(
                name="Loop Time",
                value=f"{stats['last_loop_time']:.3f}s",
                inline=True
            )

            # Add cache stats if available
            if 'cache_stats' in stats and stats['cache_stats']:
                cache = stats['cache_stats']
                embed.add_field(
                    name="Cache Performance",
                    value=f"Hit Rate: {cache.get('overall_hit_rate', 0):.1%}\n"
                          f"Entries: {cache.get('total_entries', 0)}",
                    inline=False
                )

            await ctx.send(embed=embed)

        else:
            await ctx.send("Usage: !monitor [status|start|stop|stats]")

    @commands.command(name='testmonitor')
    async def test_monitor(self, ctx, signal_id: int):
        """Test monitoring for a specific signal

        Usage: !testmonitor <signal_id>
        """
        if not self.bot.price_monitor:
            await ctx.send("❌ Price monitor not initialized")
            return

        await ctx.send(f"Testing monitor for signal #{signal_id}...")


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(AdminCommands(bot))