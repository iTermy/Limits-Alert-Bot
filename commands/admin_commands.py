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

    @commands.command(name='spreadbuffer')
    async def spread_buffer_toggle(self, ctx: commands.Context, action: str = None):
        """
        Toggle spread buffer on/off or check status

        The spread buffer prevents false alerts caused by bid-ask spread.
        When enabled, alerts are triggered when price is within the spread of the limit.

        Usage:
            !spreadbuffer on - Enable spread buffer
            !spreadbuffer off - Disable spread buffer
            !spreadbuffer status - Check current status
        """
        if not action:
            await ctx.send("Usage: `!spreadbuffer on/off/status`")
            return

        action = action.lower()

        # Load current settings
        from utils.config_loader import load_settings, save_settings

        try:
            settings = load_settings()
        except Exception as e:
            self.logger.error(f"Error loading settings: {e}")
            await ctx.send("❌ Error loading settings. Please check configuration.")
            return

        if action == 'status':
            status = settings.get('spread_buffer_enabled', True)
            status_text = "✅ ENABLED" if status else "❌ DISABLED"

            embed = discord.Embed(
                title="📊 Spread Buffer Status",
                description=f"Current status: **{status_text}**",
                color=discord.Color.green() if status else discord.Color.red()
            )

            # Add details
            config = settings.get('spread_buffer_config', {})
            embed.add_field(
                name="Configuration",
                value=f"**Approaching alerts:** {config.get('apply_to_approaching', True)}\n"
                      f"**Hit alerts:** {config.get('apply_to_hit', True)}\n"
                      f"**Stop loss:** {config.get('apply_to_stop_loss', False)}",
                inline=False
            )

            # Add explanation
            embed.add_field(
                name="ℹ️ How it works",
                value="When enabled, the buffer accounts for bid-ask spread to prevent false alerts.\n"
                      "• **Long signals:** Alert when `ask ≤ limit + spread`\n"
                      "• **Short signals:** Alert when `bid ≥ limit - spread`\n"
                      "• **Stop loss:** Always uses exact prices (no buffer)",
                inline=False
            )

            # Show monitor stats if available
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                stats = self.bot.monitor.get_stats()
                if 'buffer_allowed_alerts' in stats:
                    embed.add_field(
                        name="📈 Statistics",
                        value=f"**Alerts allowed by buffer:** {stats['buffer_allowed_alerts']}\n"
                              f"**Total signals checked:** {stats['signals_checked']:,}",
                        inline=False
                    )

            await ctx.send(embed=embed)

        elif action == 'on':
            settings['spread_buffer_enabled'] = True
            try:
                save_settings(settings)
            except Exception as e:
                self.logger.error(f"Error saving settings: {e}")
                await ctx.send("❌ Error saving settings. Changes not persisted.")
                return

            embed = discord.Embed(
                title="✅ Spread Buffer Enabled",
                description="Spread buffer is now active for all approaching and hit alerts.\n\n"
                            "This helps prevent false alerts caused by bid-ask spread fluctuations.",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Next Steps",
                value="The buffer will take effect immediately for all active signals.\n"
                      "Monitor will reload settings within 30 seconds.",
                inline=False
            )
            await ctx.send(embed=embed)
            self.logger.info(f"Spread buffer enabled by {ctx.author.name}")

        elif action == 'off':
            settings['spread_buffer_enabled'] = False
            try:
                save_settings(settings)
            except Exception as e:
                self.logger.error(f"Error saving settings: {e}")
                await ctx.send("❌ Error saving settings. Changes not persisted.")
                return

            embed = discord.Embed(
                title="❌ Spread Buffer Disabled",
                description="Spread buffer is now inactive.\n\n"
                            "Alerts will use exact price comparisons without accounting for spread.",
                color=discord.Color.red()
            )
            embed.add_field(
                name="⚠️ Warning",
                value="Disabling the buffer may result in more frequent alerts during volatile periods.",
                inline=False
            )
            await ctx.send(embed=embed)
            self.logger.info(f"Spread buffer disabled by {ctx.author.name}")

        else:
            await ctx.send("Invalid action. Use: `!spreadbuffer on/off/status`")

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
            from utils.config_loader import config
            config.reload_all()

            # Reload bot configs if they exist
            if hasattr(self.bot, 'channels_config'):
                from utils.config_loader import get_config
                self.bot.channels_config = get_config("channels.json")
            if hasattr(self.bot, 'settings'):
                from utils.config_loader import load_settings
                self.bot.settings = load_settings()

            embed = discord.Embed(
                title="✅ Configuration Reloaded",
                description="All configuration files have been reloaded successfully",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
            self.logger.info("Configuration reloaded by admin")

        except Exception as e:
            embed = discord.Embed(
                title="❌ Reload Failed",
                description=f"Error: {str(e)}",
                color=discord.Color.red()
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
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
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
                name="Price Updates",
                value=f"{stats.get('price_updates', 0):,}",
                inline=True
            )
            embed.add_field(
                name="Signals Checked",
                value=f"{stats.get('signals_checked', 0):,}",
                inline=True
            )
            embed.add_field(
                name="Limits Hit",
                value=f"{stats.get('limits_hit', 0):,}",
                inline=True
            )
            embed.add_field(
                name="Stop Losses",
                value=f"{stats.get('stop_losses_hit', 0):,}",
                inline=True
            )
            embed.add_field(
                name="Errors",
                value=f"{stats.get('errors', 0):,}",
                inline=True
            )

            # Add spread buffer stats if available
            if 'buffer_allowed_alerts' in stats:
                embed.add_field(
                    name="🔧 Spread Buffer",
                    value=f"Enabled: {stats.get('spread_buffer_enabled', 'Unknown')}\n"
                          f"Alerts allowed: {stats.get('buffer_allowed_alerts', 0):,}",
                    inline=False
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

    @commands.command(name='setalertdistance')
    @commands.has_permissions(administrator=True)
    async def set_alert_distance(self, ctx: commands.Context, symbol: str = None,
                                 value: float = None, distance_type: str = None):
        """
        Set custom alert distance for a symbol

        Usage:
            !setalertdistance USDJPY 30 pips
            !setalertdistance XAUUSD 15 dollars
            !setalertdistance NAS100USD 1.5 percentage

        Types:
            - pips: For forex pairs
            - dollars: For metals, indices, stocks, crypto
            - percentage: For any asset (as % of price)
        """
        if not symbol or value is None or not distance_type:
            embed = discord.Embed(
                title="⚙️ Set Alert Distance",
                description="Set a custom alert distance for a symbol",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Usage",
                value="```!setalertdistance <symbol> <value> <type>```",
                inline=False
            )
            embed.add_field(
                name="Examples",
                value="```!setalertdistance USDJPY 30 pips\n"
                      "!setalertdistance XAUUSD 15 dollars\n"
                      "!setalertdistance NAS100USD 1.5 percentage```",
                inline=False
            )
            embed.add_field(
                name="Types",
                value="**pips** - For forex pairs\n"
                      "**dollars** - For metals, indices, stocks, crypto\n"
                      "**percentage** - For any asset (as % of price)",
                inline=False
            )
            await ctx.send(embed=embed)
            return

        # Validate type
        distance_type = distance_type.lower()
        if distance_type not in ['pips', 'dollars', 'percentage']:
            await ctx.send(f"❌ Invalid type. Must be: pips, dollars, or percentage")
            return

        # Validate value
        if value <= 0:
            await ctx.send(f"❌ Value must be greater than 0")
            return

        # Get alert config - UPDATED FOR PHASE 2
        if hasattr(self.bot, 'monitor') and hasattr(self.bot.monitor, 'alert_config'):
            alert_config = self.bot.monitor.alert_config
        else:
            from price_feeds.alert_config import AlertDistanceConfig
            alert_config = AlertDistanceConfig()

        # Set override
        success = alert_config.set_override(
            symbol=symbol,
            value=value,
            distance_type=distance_type,
            set_by=ctx.author.name
        )

        if success:
            embed = discord.Embed(
                title="✅ Alert Distance Set",
                description=f"Custom alert distance configured for **{symbol.upper()}**",
                color=discord.Color.green()
            )
            embed.add_field(name="Distance", value=f"{value} {distance_type}", inline=True)
            embed.add_field(name="Set By", value=ctx.author.name, inline=True)

            # Show what this means in practical terms
            config = alert_config._get_config_for_symbol(symbol)
            asset_class = alert_config._determine_asset_class(symbol)
            embed.add_field(name="Asset Class", value=asset_class.replace('_', ' ').title(), inline=True)

            embed.set_footer(text="Use !removealertdistance to remove this override")

            await ctx.send(embed=embed)
            self.logger.info(f"Alert distance set: {symbol.upper()} = {value} {distance_type} by {ctx.author.name}")
        else:
            await ctx.send("❌ Failed to set alert distance. Check logs for details.")

    @commands.command(name='removealertdistance')
    @commands.has_permissions(administrator=True)
    async def remove_alert_distance(self, ctx: commands.Context, symbol: str = None):
        """
        Remove custom alert distance override for a symbol

        Usage:
            !removealertdistance USDJPY
        """
        if not symbol:
            await ctx.send("Usage: `!removealertdistance <symbol>`\nExample: `!removealertdistance USDJPY`")
            return

        # Get alert config - UPDATED FOR PHASE 2
        if hasattr(self.bot, 'monitor') and hasattr(self.bot.monitor, 'alert_config'):
            alert_config = self.bot.monitor.alert_config
        else:
            from price_feeds.alert_config import AlertDistanceConfig
            alert_config = AlertDistanceConfig()

        # Remove override
        success = alert_config.remove_override(symbol)

        if success:
            # Get the default that will now be used
            config = alert_config._get_config_for_symbol(symbol)

            embed = discord.Embed(
                title="✅ Override Removed",
                description=f"Custom alert distance removed for **{symbol.upper()}**",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Default Applied",
                value=f"{symbol.upper()} will now use default: {config['value']} {config['type']}",
                inline=False
            )
            embed.set_footer(text="Use !showalertdistances to see current defaults")

            await ctx.send(embed=embed)
            self.logger.info(f"Alert distance override removed: {symbol.upper()} by {ctx.author.name}")
        else:
            await ctx.send(f"❌ No custom alert distance found for **{symbol.upper()}**")

    @commands.command(name='showalertdistances', aliases=['alertdistances', 'alertconfig'])
    async def show_alert_distances(self, ctx: commands.Context, symbol: str = None):
        """
        Show alert distance configuration

        Usage:
            !showalertdistances - Show all defaults and overrides
            !showalertdistances USDJPY - Show config for specific symbol
        """
        # Get alert config - UPDATED FOR PHASE 2
        if hasattr(self.bot, 'monitor') and hasattr(self.bot.monitor, 'alert_config'):
            alert_config = self.bot.monitor.alert_config
        else:
            from price_feeds.alert_config import AlertDistanceConfig
            alert_config = AlertDistanceConfig()

        if symbol:
            # Show specific symbol
            config = alert_config.get_config_display(symbol)

            embed = discord.Embed(
                title=f"⚙️ Alert Distance: {config['symbol']}",
                color=discord.Color.blue()
            )

            # Distance info
            distance_str = f"{config['value']} {config['type']}"
            embed.add_field(name="Distance", value=distance_str, inline=True)
            embed.add_field(name="Asset Class", value=config['asset_class'].replace('_', ' ').title(), inline=True)

            # Override status
            if config['is_override']:
                embed.add_field(name="Type", value="🔧 Custom Override", inline=True)
                embed.add_field(name="Set By", value=config.get('set_by', 'Unknown'), inline=True)
                embed.add_field(name="Set At", value=config.get('set_at', 'Unknown')[:19], inline=True)
                embed.set_footer(text="Use !removealertdistance to remove this override")
            else:
                embed.add_field(name="Type", value="📋 Default", inline=True)
                embed.set_footer(text="Use !setalertdistance to set a custom override")

            await ctx.send(embed=embed)

        else:
            # Show all configuration
            config = alert_config.get_config_display()

            embed = discord.Embed(
                title="⚙️ Alert Distance Configuration",
                description="Default alert distances and custom overrides",
                color=discord.Color.blue()
            )

            # Defaults
            defaults_text = []
            for asset_class, settings in config['defaults'].items():
                name = asset_class.replace('_', ' ').title()
                value = settings['value']
                type_str = settings['type']
                defaults_text.append(f"**{name}**: {value} {type_str}")

            embed.add_field(
                name="📋 Defaults",
                value="\n".join(defaults_text),
                inline=False
            )

            # Overrides
            if config['overrides']:
                overrides_text = []
                for symbol, settings in config['overrides'].items():
                    value = settings['value']
                    type_str = settings['type']
                    overrides_text.append(f"**{symbol}**: {value} {type_str}")

                # Limit display to 10 items
                display_count = min(len(overrides_text), 10)
                embed.add_field(
                    name=f"🔧 Custom Overrides ({len(config['overrides'])})",
                    value="\n".join(overrides_text[:display_count]),
                    inline=False
                )

                if len(config['overrides']) > 10:
                    embed.set_footer(
                        text=f"Showing 10 of {len(config['overrides'])} overrides. Use !showalertdistances <symbol> for details.")
            else:
                embed.add_field(
                    name="🔧 Custom Overrides",
                    value="None configured",
                    inline=False
                )

            if not config['overrides']:
                embed.add_field(
                    name="Commands",
                    value="`!setalertdistance <symbol> <value> <type>` - Set override\n"
                          "`!removealertdistance <symbol>` - Remove override\n"
                          "`!showalertdistances <symbol>` - Show specific symbol",
                    inline=False
                )

            await ctx.send(embed=embed)

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