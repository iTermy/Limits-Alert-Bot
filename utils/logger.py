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

_CONSOLE_FORMAT = "%(asctime)s  %(levelname)-7s %(message)s"
_FILE_FORMAT = "%(asctime)s  %(levelname)-7s %(source)-20s %(message)s"


class _Formatter(logging.Formatter):
    """Compact single-line records; source location only where it earns its space.

    An INFO line is read, not debugged, so it carries the message alone. WARNING
    and above get `file:line` appended — that's when you actually need to find
    the code that emitted it.
    """

    def format(self, record: logging.LogRecord) -> str:
        # Loggers are named inconsistently across the codebase
        # (`trading_bot.bot`, `price_feeds.feeds.price_stream_manager`); the last
        # segment is the part that identifies the subsystem.
        record.source = record.name.rsplit(".", 1)[-1]
        line = super().format(record)
        if record.levelno >= logging.WARNING:
            line = f"{line}  ({record.filename}:{record.lineno})"
        return line


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

    # Console handler with UTF-8 encoding support
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(_Formatter(_CONSOLE_FORMAT, datefmt="%H:%M:%S"))

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
    file_handler.setFormatter(_Formatter(_FILE_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root.addHandler(file_handler)

    # Error file handler (with UTF-8 encoding)
    error_handler = RotatingFileHandler(
        os.path.join(log_dir, "errors.log"),
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding="utf-8",  # Explicitly set UTF-8 encoding
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(_Formatter(_FILE_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
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
