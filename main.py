"""
Discord Trading Alert Bot - Main Entry Point
Stage 2 Enhanced: Modular Architecture
"""
import asyncio
import os
import sys
from dotenv import load_dotenv

from utils.logger import logger
from core.bot import TradingBot

# Load environment variables
load_dotenv()

# Get bot token
DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
if not DISCORD_TOKEN:
    logger.error("DISCORD_BOT_TOKEN not found in environment variables!")
    sys.exit(1)


async def main():
    """Main entry point for the bot"""
    bot = TradingBot()

    try:
        logger.info("Starting Discord Trading Alert Bot (Stage 2 - Modular)...")
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await bot.close()
        logger.info("Bot shutdown complete")


if __name__ == "__main__":
    try:
        # Run the bot
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)