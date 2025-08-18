"""
Feed management commands for Discord bot
Provides commands to check feed status, test feeds, and manage multi-feed system
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
        Check the status of all price feeds

        Usage: !feeds
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        try:
            # Get feed status
            feed_status = await self.monitor.get_feed_health()

            # Create embed
            embed = discord.Embed(
                title="📊 Price Feed Status",
                description="Current status of all configured price feeds",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            # Feed icons
            feed_icons = {
                'icmarkets': '🏦',
                'oanda': '💱',
                'binance': '₿'
            }

            # Status colors
            status_colors = {
                'connected': '🟢',
                'disconnected': '🔴',
                'error': '🟡',
                'not_configured': '⚫'
            }

            # Add field for each feed
            for feed_name, info in feed_status.items():
                icon = feed_icons.get(feed_name, '📈')
                status = info['status']
                color = status_colors.get(status, '⚪')

                # Build field value
                field_value = f"{color} **Status:** {status.title()}"

                # Add blacklist info if applicable
                if info.get('blacklisted'):
                    field_value += "\n⚠️ Temporarily blacklisted"

                # Add health metrics if available
                if 'health' in info:
                    health = info['health']
                    field_value += f"\n📊 Success rate: {health.get('fetch_success_rate', 'N/A')}"
                    field_value += f"\n🔄 Total fetches: {health.get('successful_fetches', 0)}"

                    if health.get('last_successful_fetch'):
                        last_fetch = health['last_successful_fetch']
                        if isinstance(last_fetch, datetime):
                            time_ago = (datetime.now() - last_fetch).total_seconds()
                            if time_ago < 60:
                                field_value += f"\n⏰ Last fetch: {int(time_ago)}s ago"
                            else:
                                field_value += f"\n⏰ Last fetch: {int(time_ago / 60)}m ago"

                embed.add_field(
                    name=f"{icon} {feed_name.upper()}",
                    value=field_value,
                    inline=True
                )

            # Add feed manager stats
            stats = self.monitor.get_stats()
            if 'feed_manager' in stats:
                fm_stats = stats['feed_manager']

                # Overall statistics
                embed.add_field(
                    name="📈 Overall Statistics",
                    value=(
                        f"✅ Success rate: {fm_stats.get('success_rate', 'N/A')}\n"
                        f"📊 Total requests: {fm_stats.get('total_requests', 0)}\n"
                        f"🔄 Fallbacks used: {fm_stats.get('fallback_used', 0)}"
                    ),
                    inline=False
                )

                # Feed usage distribution
                if fm_stats.get('feeds_used'):
                    usage_str = "\n".join([
                        f"{feed_icons.get(f, '📊')} {f}: {count}"
                        for f, count in fm_stats['feeds_used'].items()
                    ])
                    embed.add_field(
                        name="📊 Feed Usage",
                        value=usage_str or "No data yet",
                        inline=True
                    )

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error getting feed status: {e}", exc_info=True)
            await ctx.reply(f"❌ Error getting feed status: {str(e)}")

    @commands.command(name='testfeeds', aliases=['tf'])
    @commands.has_permissions(administrator=True)
    async def test_feeds(self, ctx):
        """
        Test all configured price feeds

        Usage: !testfeeds
        Admin only
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        await ctx.reply("🔄 Testing all configured feeds...")

        try:
            # Run tests
            test_results = await self.monitor.test_all_feeds()

            # Create embed
            embed = discord.Embed(
                title="🧪 Feed Test Results",
                description="Connection test for all price feeds",
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

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error testing feeds: {e}", exc_info=True)
            await ctx.reply(f"❌ Error testing feeds: {str(e)}")

    @commands.command(name='checkprice', aliases=['cp', 'price'])
    async def check_price(self, ctx, symbol: str):
        """
        Check current price for a symbol from best available feed

        Usage: !checkprice EURUSD
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        symbol = symbol.upper()

        try:
            # Get price from feed manager
            from price_feeds.smart_cache import Priority
            price_data = await self.monitor.feed_manager.get_price(symbol, Priority.CRITICAL)

            if not price_data:
                await ctx.reply(f"❌ Could not get price for {symbol} from any feed")
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

            # Additional info if available
            if 'tradeable' in price_data:
                tradeable = "✅" if price_data['tradeable'] else "❌"
                embed.add_field(name="📊 Tradeable", value=tradeable, inline=True)

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error checking price for {symbol}: {e}", exc_info=True)
            await ctx.reply(f"❌ Error checking price: {str(e)}")

    @commands.command(name='feedstats', aliases=['fstats'])
    async def feed_stats(self, ctx):
        """
        Show detailed feed performance statistics

        Usage: !feedstats
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        try:
            stats = self.monitor.get_stats()

            # Create embed
            embed = discord.Embed(
                title="📊 Feed Performance Statistics",
                description="Detailed performance metrics for price feeds",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            # Monitor stats
            embed.add_field(
                name="🔄 Monitor Status",
                value=(
                    f"Running: {'✅' if stats.get('running') else '❌'}\n"
                    f"Checks performed: {stats.get('checks_performed', 0)}\n"
                    f"Limits hit: {stats.get('limits_hit', 0)}\n"
                    f"Errors: {stats.get('errors', 0)}\n"
                    f"Last loop time: {stats.get('last_loop_time', 0):.3f}s"
                ),
                inline=False
            )

            # Feed manager stats
            if 'feed_manager' in stats:
                fm = stats['feed_manager']
                embed.add_field(
                    name="📈 Feed Manager",
                    value=(
                        f"Success rate: {fm.get('success_rate', 'N/A')}\n"
                        f"Total requests: {fm.get('total_requests', 0)}\n"
                        f"Successful: {fm.get('successful_requests', 0)}\n"
                        f"Failed: {fm.get('failed_requests', 0)}\n"
                        f"Fallbacks: {fm.get('fallback_used', 0)}"
                    ),
                    inline=True
                )

            # Cache stats
            if 'cache_stats' in stats:
                cache = stats['cache_stats']
                embed.add_field(
                    name="💾 Cache Performance",
                    value=(
                        f"Hit rate: {cache.get('hit_rate', 'N/A')}\n"
                        f"Cache size: {cache.get('cache_size', 0)}\n"
                        f"Total fetches: {cache.get('total_fetches', 0)}\n"
                        f"Cache hits: {cache.get('cache_hits', 0)}\n"
                        f"Cache misses: {cache.get('cache_misses', 0)}"
                    ),
                    inline=True
                )

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error getting feed stats: {e}", exc_info=True)
            await ctx.reply(f"❌ Error getting stats: {str(e)}")

    @commands.command(name='switchfeed', aliases=['sf'])
    @commands.has_permissions(administrator=True)
    async def switch_feed(self, ctx, symbol: str, feed: Optional[str] = None):
        """
        Test which feed would be used for a symbol

        Usage: !switchfeed EURUSD [feed_name]
        Admin only
        """
        if not self.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        symbol = symbol.upper()

        try:
            # Get available feeds for symbol
            available = self.monitor.feed_manager._get_available_feeds_for_symbol(symbol)

            if not available:
                await ctx.reply(f"❌ No feeds available for {symbol}")
                return

            # Create response
            embed = discord.Embed(
                title=f"🔄 Feed Selection for {symbol}",
                color=discord.Color.blue()
            )

            # Show feed priority
            embed.add_field(
                name="📊 Available Feeds (in priority order)",
                value="\n".join([f"{i + 1}. {f}" for i, f in enumerate(available)]),
                inline=False
            )

            # Show what would be used
            embed.add_field(
                name="✅ Primary Feed",
                value=available[0].upper(),
                inline=True
            )

            if len(available) > 1:
                embed.add_field(
                    name="🔄 Fallback Feed",
                    value=available[1].upper() if len(available) > 1 else "None",
                    inline=True
                )

            # Test specific feed if requested
            if feed and feed.lower() in available:
                from price_feeds.smart_cache import Priority
                price = await self.monitor.feed_manager.feeds[feed.lower()].get_price(
                    self.monitor.symbol_mapper.get_feed_symbol(symbol, feed.lower()),
                    Priority.CRITICAL
                )

                if price:
                    embed.add_field(
                        name=f"✅ Test: {feed.upper()}",
                        value=f"Bid: {price['bid']:.5f}\nAsk: {price['ask']:.5f}",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"❌ Test: {feed.upper()}",
                        value="Failed to get price",
                        inline=False
                    )

            await ctx.reply(embed=embed)

        except Exception as e:
            logger.error(f"Error checking feed for {symbol}: {e}", exc_info=True)
            await ctx.reply(f"❌ Error: {str(e)}")


async def setup(bot):
    """Setup function for loading the cog"""
    await bot.add_cog(FeedCommands(bot))
