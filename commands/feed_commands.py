"""
Feed management commands for Discord bot
Updated to work with streaming architecture
Provides commands to check feed status, test feeds, and manage streaming connections
"""

import discord
from discord.ext import commands
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class FeedCommands(commands.Cog):
    """Commands for managing and monitoring price feeds"""

    def __init__(self, bot):
        self.bot = bot
        self.monitor = None

    @commands.Cog.listener()
    async def on_ready(self):
        """Get reference to monitor when bot is ready"""
        if hasattr(self.bot, 'monitor'):
            self.monitor = self.bot.monitor
            logger.info("Feed commands connected to monitor")

    @commands.command(name='feeds', aliases=['feedstatus', 'fs'])
    async def feed_status(self, ctx):
        """
        Check the status of all streaming price feeds

        Usage: !feeds
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        if not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        try:
            stream_manager = self.monitor.stream_manager

            # Create embed
            embed = discord.Embed(
                title="📡 Streaming Feed Status",
                description="Real-time status of all streaming price feeds",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            # Feed icons
            feed_icons = {
                'icmarkets': '🏦',
                'oanda': '💱',
                'binance': '₿'
            }

            # Get feed status
            feed_status = stream_manager.feed_status

            # Add field for each feed
            for feed_name, connected in feed_status.items():
                icon = feed_icons.get(feed_name, '📈')
                status_emoji = '🟢' if connected else '🔴'

                feed = stream_manager.feeds.get(feed_name)
                if not feed:
                    continue

                # Build field value
                field_value = f"{status_emoji} **Status:** {'Connected' if connected else 'Disconnected'}"

                # Get subscribed symbols
                subscribed = feed.get_subscribed_symbols()
                field_value += f"\n📊 **Subscriptions:** {len(subscribed)}"

                # Add feed-specific stats if available
                if hasattr(feed, 'get_stats'):
                    feed_stats = feed.get_stats()

                    if 'updates_received' in feed_stats:
                        field_value += f"\n📥 **Updates:** {feed_stats['updates_received']:,}"

                    if 'reconnections' in feed_stats:
                        field_value += f"\n🔄 **Reconnections:** {feed_stats['reconnections']}"

                    if 'last_update' in feed_stats and feed_stats['last_update']:
                        time_ago = (datetime.now() - feed_stats['last_update']).total_seconds()
                        if time_ago < 60:
                            field_value += f"\n⏰ **Last update:** {int(time_ago)}s ago"
                        else:
                            field_value += f"\n⏰ **Last update:** {int(time_ago / 60)}m ago"

                embed.add_field(
                    name=f"{icon} {feed_name.upper()}",
                    value=field_value,
                    inline=True
                )

            # Add overall statistics
            stats = stream_manager.get_stats()
            embed.add_field(
                name="📈 Overall Statistics",
                value=(
                    f"🎯 **Total subscriptions:** {stats.get('subscribed_symbols', 0)}\n"
                    f"✅ **Connected feeds:** {stats.get('connected_feeds', 0)}/3\n"
                    f"📥 **Total updates:** {stats.get('updates_received', 0):,}\n"
                    f"🔄 **Total reconnections:** {stats.get('reconnections', 0)}"
                ),
                inline=False
            )

            # Show some subscribed symbols
            subscribed_symbols = stream_manager.subscribed_symbols
            if subscribed_symbols:
                symbol_list = sorted(list(subscribed_symbols))[:10]
                symbols_str = ", ".join(symbol_list)
                if len(subscribed_symbols) > 10:
                    symbols_str += f" (+{len(subscribed_symbols) - 10} more)"

                embed.add_field(
                    name="🎯 Monitored Symbols (sample)",
                    value=symbols_str,
                    inline=False
                )

            embed.set_footer(text="Use !streamhealth for detailed health metrics")
            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error getting feed status: {e}", exc_info=True)
            await ctx.reply(f"❌ Error getting feed status: {str(e)}")

    @commands.command(name='testfeeds', aliases=['tf'])
    async def test_feeds(self, ctx):
        """
        Test all configured streaming feeds

        Usage: !testfeeds
        Admin only
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        if not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        await ctx.reply("🔄 Testing all streaming feeds...")

        try:
            stream_manager = self.monitor.stream_manager
            test_results = {}

            # Test each feed
            for feed_name, feed in stream_manager.feeds.items():
                try:
                    # Check if feed is connected
                    if feed.connected:
                        # Try to get a sample price to verify stream is working
                        subscribed = feed.get_subscribed_symbols()
                        if subscribed:
                            # Feed is connected and has subscriptions
                            test_results[feed_name] = (True, f"Connected with {len(subscribed)} subscriptions")
                        else:
                            test_results[feed_name] = (True, "Connected but no active subscriptions")
                    else:
                        test_results[feed_name] = (False, "Not connected")

                except Exception as e:
                    test_results[feed_name] = (False, f"Error: {str(e)}")

            # Create embed
            embed = discord.Embed(
                title="🧪 Streaming Feed Test Results",
                description="Connection test for all streaming price feeds",
                color=discord.Color.green() if all(r[0] for r in test_results.values()) else discord.Color.orange(),
                timestamp=datetime.now()
            )

            # Add results
            for feed_name, (success, message) in test_results.items():
                icon = "✅" if success else "❌"
                embed.add_field(
                    name=f"{feed_name.upper()}",
                    value=f"{icon} {message}",
                    inline=False
                )

            # Add recommendation if any failed
            failed_feeds = [name for name, (success, _) in test_results.items() if not success]
            if failed_feeds:
                embed.add_field(
                    name="💡 Recommendation",
                    value=f"Try reconnecting failed feeds with: `!reconnect <feed_name>`",
                    inline=False
                )

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error testing feeds: {e}", exc_info=True)
            await ctx.reply(f"❌ Error testing feeds: {str(e)}")

    @commands.command(name='checkprice', aliases=['cp', 'price'])
    async def check_price(self, ctx, symbol: str):
        """
        Check current price for a symbol from streaming feeds

        Usage: !checkprice EURUSD
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        if not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        symbol = symbol.upper()

        try:
            stream_manager = self.monitor.stream_manager

            # Get latest price from stream manager
            price_data = await stream_manager.get_latest_price(symbol)

            if not price_data:
                await ctx.reply(f"❌ Could not get price for {symbol}. Symbol may not be subscribed.")
                return

            # Create embed
            embed = discord.Embed(
                title=f"💹 {symbol} Price",
                color=discord.Color.green(),
                timestamp=price_data.get('timestamp', datetime.now())
            )

            # Price information
            embed.add_field(name="📊 Bid", value=f"{price_data['bid']:.5f}", inline=True)
            embed.add_field(name="📈 Ask", value=f"{price_data['ask']:.5f}", inline=True)

            spread = price_data['ask'] - price_data['bid']
            embed.add_field(name="📉 Spread", value=f"{spread:.5f}", inline=True)

            # Feed information
            feed_used = price_data.get('feed', 'unknown')
            embed.add_field(name="🔌 Feed", value=feed_used.upper(), inline=True)

            # Streaming status
            is_subscribed = symbol in stream_manager.subscribed_symbols
            embed.add_field(
                name="📡 Status",
                value="🟢 Subscribed" if is_subscribed else "⚪ Not Subscribed",
                inline=True
            )

            # Additional info if available
            if 'tradeable' in price_data:
                tradeable = "✅" if price_data['tradeable'] else "❌"
                embed.add_field(name="📊 Tradeable", value=tradeable, inline=True)

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error checking price for {symbol}: {e}", exc_info=True)
            await ctx.reply(f"❌ Error checking price: {str(e)}")

    @commands.command(name='subscriptions', aliases=['subs'])
    async def show_subscriptions(self, ctx):
        """
        Show all currently subscribed symbols across all feeds

        Usage: !subscriptions
        """
        if not self.monitor or not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        try:
            stream_manager = self.monitor.stream_manager
            subscribed_symbols = stream_manager.subscribed_symbols

            if not subscribed_symbols:
                embed = discord.Embed(
                    title="📡 Active Subscriptions",
                    description="No symbols currently subscribed",
                    color=discord.Color.orange()
                )
                await ctx.reply(embed=embed)
                return

            # Sort symbols
            sorted_symbols = sorted(list(subscribed_symbols))

            # Create embed
            embed = discord.Embed(
                title="📡 Active Subscriptions",
                description=f"Currently monitoring {len(sorted_symbols)} symbols via streaming",
                color=discord.Color.blue()
            )

            # Group symbols by feed
            symbol_to_feed = stream_manager.symbol_to_feed

            feeds_data = {}
            for symbol in sorted_symbols:
                feed_name = symbol_to_feed.get(symbol, 'unknown')
                if feed_name not in feeds_data:
                    feeds_data[feed_name] = []
                feeds_data[feed_name].append(symbol)

            # Add field for each feed
            feed_icons = {
                'icmarkets': '🏦',
                'oanda': '💱',
                'binance': '₿'
            }

            for feed_name, symbols in feeds_data.items():
                icon = feed_icons.get(feed_name, '📈')

                # Limit display to prevent overflow
                display_symbols = symbols[:20]
                symbols_str = ", ".join(display_symbols)
                if len(symbols) > 20:
                    symbols_str += f"\n... +{len(symbols) - 20} more"

                embed.add_field(
                    name=f"{icon} {feed_name.upper()} ({len(symbols)})",
                    value=symbols_str,
                    inline=False
                )

            embed.set_footer(text=f"Total: {len(sorted_symbols)} symbols")
            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error showing subscriptions: {e}", exc_info=True)
            await ctx.reply(f"❌ Error: {str(e)}")

    @commands.command(name='feedstats', aliases=['fstats'])
    async def feed_stats(self, ctx):
        """
        Show detailed streaming performance statistics

        Usage: !feedstats
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        if not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        try:
            stats = self.monitor.get_stats()
            stream_stats = stats.get('stream_manager', {})

            # Create embed
            embed = discord.Embed(
                title="📊 Streaming Performance Statistics",
                description="Detailed performance metrics for streaming price feeds",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            # Monitor stats
            embed.add_field(
                name="🔄 Monitor Status",
                value=(
                    f"**Running:** {'✅' if stats.get('running') else '❌'}\n"
                    f"**Checks performed:** {stats.get('checks_performed', 0):,}\n"
                    f"**Limits hit:** {stats.get('limits_hit', 0)}\n"
                    f"**Alerts sent:** {stats.get('alerts_sent', 0)}\n"
                    f"**Errors:** {stats.get('errors', 0)}"
                ),
                inline=False
            )

            # Stream manager stats
            if stream_stats:
                embed.add_field(
                    name="📡 Streaming Performance",
                    value=(
                        f"**Subscribed symbols:** {stream_stats.get('subscribed_symbols', 0)}\n"
                        f"**Connected feeds:** {stream_stats.get('connected_feeds', 0)}/3\n"
                        f"**Updates received:** {stream_stats.get('updates_received', 0):,}\n"
                        f"**Reconnections:** {stream_stats.get('reconnections', 0)}"
                    ),
                    inline=True
                )

            # Individual feed stats
            if hasattr(self.monitor.stream_manager, 'feeds'):
                feed_icons = {
                    'icmarkets': '🏦',
                    'oanda': '💱',
                    'binance': '₿'
                }

                for feed_name, feed in self.monitor.stream_manager.feeds.items():
                    icon = feed_icons.get(feed_name, '📈')

                    if hasattr(feed, 'get_stats'):
                        feed_stats = feed.get_stats()
                        subscribed = len(feed.get_subscribed_symbols())

                        field_value = f"**Subscriptions:** {subscribed}\n"
                        if 'updates_received' in feed_stats:
                            field_value += f"**Updates:** {feed_stats['updates_received']:,}\n"
                        if 'reconnections' in feed_stats:
                            field_value += f"**Reconnections:** {feed_stats['reconnections']}"

                        embed.add_field(
                            name=f"{icon} {feed_name.upper()}",
                            value=field_value,
                            inline=True
                        )

            # Alert latency if available
            if 'average_check_time' in stats:
                embed.add_field(
                    name="⚡ Performance",
                    value=f"Avg check time: {stats['average_check_time']:.3f}s",
                    inline=True
                )

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error getting feed stats: {e}", exc_info=True)
            await ctx.reply(f"❌ Error getting stats: {str(e)}")

    @commands.command(name='feedinfo', aliases=['fi'])
    async def feed_info(self, ctx, feed_name: str = None):
        """
        Show detailed information about a specific feed

        Usage: !feedinfo <feed_name>
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

        if not self.monitor or not hasattr(self.monitor, 'stream_manager'):
            await ctx.reply("❌ Stream manager not available")
            return

        try:
            stream_manager = self.monitor.stream_manager
            feed = stream_manager.feeds.get(feed_name)

            if not feed:
                await ctx.reply(f"❌ Feed '{feed_name}' not found")
                return

            # Feed icons
            feed_icons = {
                'icmarkets': '🏦',
                'oanda': '💱',
                'binance': '₿'
            }
            icon = feed_icons.get(feed_name, '📈')

            # Create embed
            embed = discord.Embed(
                title=f"{icon} {feed_name.upper()} Feed Information",
                color=discord.Color.green() if feed.connected else discord.Color.red(),
                timestamp=datetime.now()
            )

            # Connection status
            status_emoji = '🟢' if feed.connected else '🔴'
            embed.add_field(
                name="Connection Status",
                value=f"{status_emoji} {'Connected' if feed.connected else 'Disconnected'}",
                inline=True
            )

            # Subscribed symbols
            subscribed = feed.get_subscribed_symbols()
            embed.add_field(
                name="Subscriptions",
                value=f"📊 {len(subscribed)} symbols",
                inline=True
            )

            # Feed-specific stats
            if hasattr(feed, 'get_stats'):
                feed_stats = feed.get_stats()

                if 'updates_received' in feed_stats:
                    embed.add_field(
                        name="Updates Received",
                        value=f"📥 {feed_stats['updates_received']:,}",
                        inline=True
                    )

                if 'reconnections' in feed_stats:
                    embed.add_field(
                        name="Reconnections",
                        value=f"🔄 {feed_stats['reconnections']}",
                        inline=True
                    )

                if 'last_update' in feed_stats and feed_stats['last_update']:
                    time_ago = (datetime.now() - feed_stats['last_update']).total_seconds()
                    if time_ago < 60:
                        last_update_str = f"{int(time_ago)}s ago"
                    elif time_ago < 3600:
                        last_update_str = f"{int(time_ago / 60)}m ago"
                    else:
                        last_update_str = f"{int(time_ago / 3600)}h ago"

                    embed.add_field(
                        name="Last Update",
                        value=f"⏰ {last_update_str}",
                        inline=True
                    )

            # Show sample of subscribed symbols
            if subscribed:
                sample_size = min(15, len(subscribed))
                sample = sorted(list(subscribed))[:sample_size]
                symbols_str = ", ".join(sample)
                if len(subscribed) > sample_size:
                    symbols_str += f"\n... +{len(subscribed) - sample_size} more"

                embed.add_field(
                    name="Subscribed Symbols (sample)",
                    value=symbols_str,
                    inline=False
                )

            embed.set_footer(text=f"Use !reconnect {feed_name} to reconnect this feed")
            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error getting feed info: {e}", exc_info=True)
            await ctx.reply(f"❌ Error: {str(e)}")


async def setup(bot):
    """Setup function for loading the cog"""
    await bot.add_cog(FeedCommands(bot))