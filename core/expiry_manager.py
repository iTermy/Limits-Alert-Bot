import asyncio
from discord.ext import tasks
from utils.logger import get_logger


class ExpiryManager:
    """Manages automatic expiry of trading signals."""

    def __init__(self, bot):
        self.bot = bot
        self.logger = get_logger("expiry_manager")

        # Start background loop
        self.check_expiry.start()

    @tasks.loop(minutes=5)
    async def check_expiry(self):
        """Check and expire signals past expiry_time."""
        try:
            count = await self.bot.signal_db.expire_old_signals()
            if count > 0:
                self.logger.info(f"Expired {count} signals")
        except Exception as e:
            self.logger.error(f"Error in expiry loop: {e}")

    @check_expiry.before_loop
    async def before_check_expiry(self):
        """Wait until bot is ready before starting."""
        await self.bot.wait_until_ready()

    def stop(self):
        """Stop the expiry loop (optional for cleanup)."""
        self.check_expiry.cancel()
