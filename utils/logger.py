"""
Logging configuration for the Trading Alert Bot
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


# Third-party loggers that would otherwise flood the root handlers at INFO.
_NOISY_LIBRARIES = (
    "discord",
    "aiohttp",
    "asyncio",
    "asyncpg",
    "httpx",
    "httpcore",
    "openai",
    "urllib3",
    "websockets",
)


def setup_logger(name: str = "trading_bot", log_dir: str = "data/logs") -> logging.Logger:
    """
    Attach the console and rotating-file handlers to the ROOT logger.

    Handlers go on the root rather than on `trading_bot` so that modules using
    the plain `logging.getLogger(__name__)` idiom — every price feed client, the
    trailing/excursion monitors, the config loaders — reach the log files too.
    With handlers on `trading_bot` alone their records fell through to
    `logging.lastResort`: INFO dropped entirely, WARNING+ to bare stderr, so a
    feed that failed to connect left no trace on disk.

    Args:
        name: Logger name to return for the caller's convenience
        log_dir: Directory for log files

    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()

    # Clear any existing handlers
    root.handlers.clear()

    # Set level from environment or default to INFO
    log_level = os.getenv("LOG_LEVEL", "INFO")
    root.setLevel(getattr(logging, log_level))

    for library in _NOISY_LIBRARIES:
        logging.getLogger(library).setLevel(logging.WARNING)

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    simple_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )

    # Console handler with UTF-8 encoding support
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # Force UTF-8 encoding for Windows console
    if sys.platform == "win32":
        import locale

        if locale.getpreferredencoding().upper() != "UTF-8":
            # Reconfigure stdout to handle UTF-8
            sys.stdout.reconfigure(encoding="utf-8")

    root.addHandler(console_handler)

    # File handler for all logs (with UTF-8 encoding)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "bot.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",  # Explicitly set UTF-8 encoding
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    root.addHandler(file_handler)

    # Error file handler (with UTF-8 encoding)
    error_handler = RotatingFileHandler(
        os.path.join(log_dir, "errors.log"),
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding="utf-8",  # Explicitly set UTF-8 encoding
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root.addHandler(error_handler)

    logger = logging.getLogger(name)

    # Add a custom exception handler to prevent logger crashes
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a child logger with the given name

    Args:
        name: Name for the child logger

    Returns:
        Logger instance
    """
    return logging.getLogger(f"trading_bot.{name}")


# Create main logger instance
logger = setup_logger()
