"""
Base Command Cog - Foundation for all command groups
"""

from typing import Optional

import discord
from discord.ext import commands

from utils.formatting import get_channel_name as _get_channel_name


class BaseCog(commands.Cog):
    """Base class for all command cogs"""

    def __init__(self, bot):
        self.bot = bot
        self.logger = bot.logger
        self.services = bot.services
        self.signal_db = bot.services.signal_db

    def is_admin(self, user: discord.User) -> bool:
        if hasattr(user, "guild_permissions"):
            return user.id in self.bot.admin_ids or user.guild_permissions.administrator
        return user.id in self.bot.admin_ids

    def is_command_channel(self, channel: discord.TextChannel) -> bool:
        """Check if channel is the designated command channel"""
        if not self.bot.command_channel_id:
            return True
        return channel.id == self.bot.command_channel_id

    async def cog_check(self, ctx: commands.Context) -> bool:
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

        self.logger.error(f"Command error in {ctx.command}: {error}")
        await ctx.send(f"❌ An error occurred: {error!s}")

    def get_channel_name(self, channel_id: int) -> Optional[str]:
        return _get_channel_name(self.bot.channels_config, channel_id)
