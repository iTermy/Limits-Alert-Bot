"""
General Commands - Basic bot commands available to all users
"""
from discord.ext import commands
import discord
from .base_command import BaseCog
from utils.embed_factory import EmbedFactory


class GeneralCommands(BaseCog):
    """General commands available to all users"""

    @commands.command(name='ping')
    async def ping(self, ctx: commands.Context):
        """Test command to check bot responsiveness"""
        latency = round(self.bot.latency * 1000)
        embed = EmbedFactory.success(
            title="🏓 Pong!",
            description=f"Latency: {latency}ms"
        )
        await ctx.send(embed=embed)

    @commands.command(name='status')
    async def status(self, ctx: commands.Context):
        """Show bot status with statistics"""
        # Get statistics from database
        stats = await self.signal_db.get_statistics()

        # Prepare bot info
        bot_info = {
            'version': "1.0.0 (Stage 2 Enhanced)",
            'guilds': len(self.bot.guilds),
            'latency': round(self.bot.latency * 1000),
            'monitored_channels': len(self.bot.monitored_channels),
            'db_connected': True,  # We're here, so it must be connected
            'debug_mode': self.bot.settings.get("debug_mode", False)
        }

        embed = EmbedFactory.bot_status(stats, bot_info)
        await ctx.send(embed=embed)

    @commands.command(name='help')
    async def help_command(self, ctx: commands.Context, category: str = None):
        """Show help information"""

        # Convert category to lowercase for matching
        category = category.lower() if category else None

        if category == "admin":
            # Admin commands page
            embed = discord.Embed(
                title="🔐 Admin Commands",
                description="Commands only available to bot administrators",
                color=discord.Color.red()
            )
            embed.add_field(name="!clear", value="Clear all signals from the database (requires confirmation)",
                            inline=False)
            embed.add_field(name="!expire", value="Manually trigger expiry check for old signals", inline=False)
            embed.add_field(name="!reload", value="Reload configuration files", inline=False)
            embed.add_field(name="!shutdown", value="Gracefully shutdown the bot", inline=False)
            embed.add_field(name="!logs [lines]", value="Show recent log entries (default: 10 lines)", inline=False)
            embed.add_field(name="!test_signal [text]", value="Test signal parsing with custom or sample text",
                            inline=False)
            embed.set_footer(text="Use !help to see Signal Commands or !help general for General Commands")
            await ctx.send(embed=embed)

        elif category == "general":
            # General commands page
            embed = discord.Embed(
                title="⚙️ General Commands",
                description="Useful commands for all users",
                color=discord.Color.green()
            )
            embed.add_field(name="!ping", value="Check bot responsiveness and latency", inline=False)
            embed.add_field(name="!status", value="Show bot status with statistics", inline=False)
            embed.add_field(name="!help [category]", value="Show this help message. Categories: admin, general",
                            inline=False)
            embed.set_footer(text="Use !help to see Signal Commands or !help admin for Admin Commands")
            await ctx.send(embed=embed)

        else:
            # Default page - Signal Commands
            embed = discord.Embed(
                title="📊 Signal Commands",
                description="Commands for viewing and managing trading signals",
                color=discord.Color.blue()
            )
            embed.add_field(name="!active [instrument]", value="Show active trading signals (ACTIVE and HIT status)",
                            inline=False)
            embed.add_field(name="!all [status]",
                            value="Show all signals or filter by status\nValid statuses: active, hit, profit, breakeven, stop_loss, cancelled",
                            inline=False)
            embed.add_field(name="!add <signal text>", value="Manually add a signal from the command channel",
                            inline=False)
            embed.add_field(name="!delete <signal_id>", value="Delete a specific signal by ID", inline=False)
            embed.add_field(name="!info <signal_id>", value="Show detailed information about a specific signal",
                            inline=False)
            embed.add_field(name="!stats", value="Show detailed statistics about signals", inline=False)
            embed.add_field(name="!setexpiry <signal_id> <day/week/month/vth/YYYY-MM-DD>",
                value=(
                    "Set a signal's expiry time.\n"
                    "`day` → End of today (5:00 PM EST)\n"
                    "`week` → End of Friday (5:00 PM EST)\n"
                    "`month` → End of last trading day of the month\n"
                    "`no_expiry` → No expiry\n"
                    "`YYYY-MM-DD HH-NN` → Custom date and time (Example: 2025-08-13 14:30)"
                ),
                inline=False
            )
            embed.add_field(name="!setstatus <signal_id> <status>",
                            value="Manually set a signal's status\nValid statuses: active, hit, profit, breakeven, stop_loss, cancelled, cancel",
                            inline=False)
            embed.add_field(name="!profit <signal_id>", value="Set a signal's status to PROFIT", inline=False)
            embed.add_field(name="!breakeven <signal_id>", value="Set a signal's status to BREAKEVEN", inline=False)
            embed.add_field(name="!hit <signal_id>",value="Set a signal's status to HIT", inline=False)
            embed.add_field(name="!stoploss <signal_id>", value="Set a signal's status to STOP LOSS (alias: !sl)", inline=False)
            embed.add_field(name="!cancel <signal_id>", value="Set a signal's status to CANCELLED", inline=False)
            embed.set_footer(text="Tip: Type !help admin or !help general to see more commands")
            await ctx.send(embed=embed)


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(GeneralCommands(bot))