"""TradeMaster Alert Bot - entry point."""

import asyncio
import os
import sys

from dotenv import load_dotenv

# Load .env from the project root before any other imports that may read env vars.
_here = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(_here, ".env"))

from core.bot import TradingBot
from utils.logger import logger

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    logger.error("DISCORD_BOT_TOKEN not found in .env")
    sys.exit(1)


async def main():
    """Main entry point for the bot"""
    bot = TradingBot()

    try:
        logger.info("Starting TradeMaster Alert Bot...")
        await bot.start(DISCORD_TOKEN)
    finally:
        await bot.close()
        logger.info("Bot shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
