from .bot_management import BotManagementCog
from .license import LicenseCog


async def setup(bot):
    await bot.add_cog(BotManagementCog(bot))
    await bot.add_cog(LicenseCog(bot))
