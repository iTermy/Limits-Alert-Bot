from .thresholds import ThresholdsCog


async def setup(bot):
    await bot.add_cog(ThresholdsCog(bot))
