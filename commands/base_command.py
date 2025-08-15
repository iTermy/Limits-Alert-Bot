"""
Base Command Cog - Foundation for all command groups
"""
from discord.ext import commands
import discord
from typing import Optional


class BaseCog(commands.Cog):
    """Base class for all command cogs"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.signal_db = bot.signal_db

    def is_admin(self, user: discord.User) -> bool:
        """Check if user is an admin"""
        if hasattr(user, 'guild_permissions'):
            return user.id in self.bot.admin_ids or user.guild_permissions.administrator
        return user.id in self.bot.admin_ids

    def is_command_channel(self, channel: discord.TextChannel) -> bool:
        """Check if channel is the designated command channel"""
        if not self.bot.command_channel_id:
            return True  # No restriction if command channel not set
        return channel.id == self.bot.command_channel_id

    async def cog_check(self, ctx: commands.Context) -> bool:
        """Global check for all commands in this cog"""
        # You can add global checks here that apply to all commands
        # Return True to allow command execution
        return True

    async def cog_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Error handler for commands in this cog"""
        if isinstance(error, commands.CommandNotFound):
            return

        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You don't have permission to use this command.")
            return

        if isinstance(error, commands.CheckFailure):
            await ctx.send("❌ This command cannot be used here.")
            return

        # Log unexpected errors
        self.logger.error(f"Command error in {ctx.command}: {error}")
        await ctx.send(f"❌ An error occurred: {str(error)}")

    def get_channel_name(self, channel_id: int) -> Optional[str]:
        """Get channel name from configuration"""
        for name, ch_id in self.bot.channels_config.get("monitored_channels", {}).items():
            if int(ch_id) == channel_id:
                return name
        return None