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

# Auto-restart backoff: start small, grow on repeated failures, reset after a
# healthy run so a one-off disconnect restarts promptly.
_RESTART_BASE_DELAY = 5
_RESTART_MAX_DELAY = 300
_HEALTHY_RUN_SECONDS = 120


def _log_loop_exception(loop, context):
    """Capture unhandled exceptions from background tasks that would otherwise
    vanish, so a crash that ends the bot leaves a real traceback in the log."""
    exc = context.get("exception")
    message = context.get("message", "unknown error")
    if exc:
        logger.error("Unhandled asyncio exception: %s", message, exc_info=exc)
    else:
        logger.error("Unhandled asyncio error: %s", message)


async def _run_once():
    """Start one bot instance and run until it stops. Returns when start()
    returns; propagates any exception it raised."""
    bot = TradingBot()
    try:
        logger.info("Starting TradeMaster Alert Bot")
        await bot.start(DISCORD_TOKEN)
    finally:
        await bot.close()
        logger.info("Bot stopped")


async def main():
    """Supervise the bot: restart on unexpected exit, stop only on interrupt."""
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_log_loop_exception)

    delay = _RESTART_BASE_DELAY
    while True:
        started = loop.time()
        try:
            await _run_once()
            # A long-running bot's start() returning without an exception is
            # itself abnormal — treat it as a crash and restart.
            logger.error("bot.start() returned unexpectedly; restarting in %ss", delay)
        except Exception as e:
            logger.error("Bot crashed: %s; restarting in %ss", e, delay, exc_info=True)

        # Reset backoff if the instance stayed up long enough to be considered healthy.
        if loop.time() - started >= _HEALTHY_RUN_SECONDS:
            delay = _RESTART_BASE_DELAY

        await asyncio.sleep(delay)
        delay = min(delay * 2, _RESTART_MAX_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
