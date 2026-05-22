from .lifecycle import LifecycleCog
from .news import NewsCog
from .reports import ReportsCog


async def setup(bot):
    await bot.add_cog(LifecycleCog(bot))
    await bot.add_cog(ReportsCog(bot))
    await bot.add_cog(NewsCog(bot))
