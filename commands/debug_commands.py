"""
Debug Commands - For diagnosing streaming feed issues
Add these to your admin_commands.py or as a separate cog
"""

from discord.ext import commands
import discord
import logging
import asyncio

logger = logging.getLogger(__name__)


class DebugCommands(commands.Cog):
    """Commands for debugging streaming issues"""

    def __init__(self, bot):
        self.bot = bot

    @commands.command(name='debugfeed')
    async def debug_feed(self, ctx, feed_name: str = None):
        """
        Debug a specific feed's status and subscriptions

        Usage: !debugfeed <icmarkets|oanda|binance>
        """
        if not feed_name:
            await ctx.reply("Usage: !debugfeed <icmarkets|oanda|binance>")
            return

        feed_name = feed_name.lower()
        if feed_name not in ['icmarkets', 'oanda', 'binance']:
            await ctx.reply("❌ Invalid feed. Use: icmarkets, oanda, or binance")
            return

        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        stream_manager = self.bot.monitor.stream_manager

        embed = discord.Embed(
            title=f"🔍 Debug: {feed_name.upper()}",
            color=discord.Color.blue()
        )

        # Check if feed exists
        if feed_name not in stream_manager.feeds:
            embed.add_field(name="Status", value="❌ Feed not initialized", inline=False)
            await ctx.send(embed=embed)
            return

        feed = stream_manager.feeds[feed_name]

        # Connection status
        connected = feed.connected if hasattr(feed, 'connected') else 'Unknown'
        embed.add_field(name="Connected", value=f"{'✅' if connected else '❌'} {connected}", inline=True)

        # Feed status
        feed_status = stream_manager.feed_status.get(feed_name, False)
        embed.add_field(name="Feed Status", value=f"{'✅' if feed_status else '❌'} {feed_status}", inline=True)

        # Subscribed symbols
        subscribed = feed.get_subscribed_symbols() if hasattr(feed, 'get_subscribed_symbols') else set()
        embed.add_field(name="Subscribed Symbols", value=f"{len(subscribed)}", inline=True)

        # Show sample of subscribed symbols
        if subscribed:
            sample = sorted(list(subscribed))[:10]
            symbols_str = ", ".join(sample)
            if len(subscribed) > 10:
                symbols_str += f" (+{len(subscribed) - 10} more)"
            embed.add_field(name="Symbols", value=symbols_str, inline=False)

        # Feed-specific stats
        if hasattr(feed, 'get_stats'):
            stats = feed.get_stats()
            embed.add_field(
                name="Stats",
                value=f"Updates: {stats.get('updates_received', 0)}\n"
                      f"Reconnections: {stats.get('reconnections', 0)}",
                inline=False
            )

        # Check latest prices
        prices_for_feed = []
        for symbol, feed_name_check in stream_manager.symbol_to_feed.items():
            if feed_name_check == feed_name:
                prices_for_feed.append(symbol)

        embed.add_field(
            name="Symbols Mapped to This Feed",
            value=f"{len(prices_for_feed)} symbols",
            inline=True
        )

        await ctx.send(embed=embed)

    @commands.command(name='testprice')
    async def test_price(self, ctx, symbol: str):
        """
        Test fetching a price for a specific symbol
        Shows detailed debugging info

        Usage: !testprice EURUSD
        """
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        symbol = symbol.upper()
        stream_manager = self.bot.monitor.stream_manager

        embed = discord.Embed(
            title=f"🧪 Price Test: {symbol}",
            color=discord.Color.blue()
        )

        # Check subscription
        is_subscribed = symbol in stream_manager.subscribed_symbols
        embed.add_field(name="Subscribed", value="✅ Yes" if is_subscribed else "❌ No", inline=True)

        # Check which feed
        feed_name = stream_manager.symbol_to_feed.get(symbol, "Not assigned")
        embed.add_field(name="Feed", value=feed_name, inline=True)

        # Check if feed is connected
        if feed_name in stream_manager.feeds:
            feed = stream_manager.feeds[feed_name]
            connected = feed.connected if hasattr(feed, 'connected') else 'Unknown'
            embed.add_field(name="Feed Connected", value=f"{'✅' if connected else '❌'} {connected}", inline=True)

        # Try to get price
        await ctx.send("🔄 Fetching price...")

        try:
            price_data = await stream_manager.get_latest_price(symbol)

            if price_data:
                embed.add_field(name="✅ Price Retrieved", value="Success", inline=False)
                embed.add_field(name="Bid", value=f"{price_data.get('bid', 'N/A')}", inline=True)
                embed.add_field(name="Ask", value=f"{price_data.get('ask', 'N/A')}", inline=True)
                embed.add_field(name="Feed Used", value=price_data.get('feed', 'Unknown'), inline=True)

                if 'timestamp' in price_data:
                    embed.add_field(name="Timestamp", value=str(price_data['timestamp']), inline=False)
            else:
                embed.add_field(name="❌ No Price", value="Could not retrieve price", inline=False)

                # Additional diagnostics
                if not is_subscribed:
                    embed.add_field(name="Issue", value="Symbol not subscribed", inline=False)
                elif feed_name not in stream_manager.feeds:
                    embed.add_field(name="Issue", value=f"Feed '{feed_name}' not initialized", inline=False)
                elif not connected:
                    embed.add_field(name="Issue", value=f"Feed '{feed_name}' not connected", inline=False)
                else:
                    embed.add_field(name="Issue", value="Unknown - feed is connected and symbol is subscribed",
                                    inline=False)

        except Exception as e:
            embed.add_field(name="❌ Error", value=str(e), inline=False)
            logger.error(f"Error testing price for {symbol}: {e}", exc_info=True)

        await ctx.send(embed=embed)

    @commands.command(name='pricehistory')
    async def price_history(self, ctx, symbol: str = None):
        """
        Show all symbols we have prices for

        Usage: !pricehistory [symbol]
        """
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        stream_manager = self.bot.monitor.stream_manager

        embed = discord.Embed(
            title="💾 Price Cache Status",
            color=discord.Color.blue()
        )

        # Get all symbols with prices
        symbols_with_prices = list(stream_manager.latest_prices.keys())

        embed.add_field(
            name="Cached Prices",
            value=f"{len(symbols_with_prices)} symbols have price data",
            inline=False
        )

        if symbol:
            symbol = symbol.upper()
            if symbol in stream_manager.latest_prices:
                price_data = stream_manager.latest_prices[symbol]
                embed.add_field(name=f"📊 {symbol}",
                                value=f"Bid: {price_data.get('bid')}\nAsk: {price_data.get('ask')}\nFeed: {price_data.get('feed')}",
                                inline=False)
            else:
                embed.add_field(name=f"❌ {symbol}", value="No cached price", inline=False)
        else:
            # Show sample
            if symbols_with_prices:
                sample = sorted(symbols_with_prices)[:20]
                symbols_str = ", ".join(sample)
                if len(symbols_with_prices) > 20:
                    symbols_str += f"\n... +{len(symbols_with_prices) - 20} more"
                embed.add_field(name="Symbols", value=symbols_str, inline=False)

        await ctx.send(embed=embed)

    @commands.command(name='forcesubscribe')
    async def force_subscribe(self, ctx, symbol: str):
        """
        Force subscribe to a symbol for testing

        Usage: !forcesubscribe EURUSD
        """
        if not hasattr(self.bot, 'monitor') or not self.bot.monitor:
            await ctx.reply("❌ Monitor not initialized")
            return

        symbol = symbol.upper()

        await ctx.send(f"🔄 Subscribing to {symbol}...")

        try:
            await self.bot.monitor.stream_manager.subscribe_symbol(symbol)
            await ctx.send(f"✅ Subscribed to {symbol}")

            # Wait a moment and check for price
            await asyncio.sleep(2)

            price = await self.bot.monitor.stream_manager.get_latest_price(symbol)
            if price:
                await ctx.send(f"✅ Received price: Bid={price.get('bid')}, Ask={price.get('ask')}")
            else:
                await ctx.send(f"⚠️ Subscribed but no price received yet. Check !debugfeed to see feed status.")

        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}")
            logger.error(f"Error subscribing to {symbol}: {e}", exc_info=True)


async def setup(bot):
    """Setup function for loading the cog"""
    await bot.add_cog(DebugCommands(bot))