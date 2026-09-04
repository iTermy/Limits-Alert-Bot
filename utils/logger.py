"""
Logging configuration for the Trading Alert Bot
"""

import contextlib
import logging
import os
import queue
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
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

# Log sinks run on listener threads so a blocked Windows console (for example,
# a paused/selected console or a full redirected pipe) cannot freeze asyncio.
_active_listeners: list[QueueListener] = []
_active_sink_handlers: list[logging.Handler] = []


class _DroppingQueueHandler(QueueHandler):
    """Never block or write logging errors back to the blocked console."""

    def enqueue(self, record: logging.LogRecord) -> None:
        with contextlib.suppress(queue.Full):
            self.queue.put_nowait(record)
        # A permanently blocked sink must not turn into unbounded memory
        # growth. The file and console use independent queues, so a full
        # console queue does not discard the durable file copy.


def shutdown_logging() -> None:
    """Stop active listener threads and close their output handlers."""
    for listener in reversed(_active_listeners):
        listener.stop()
    _active_listeners.clear()

    for handler in _active_sink_handlers:
        handler.close()
    _active_sink_handlers.clear()


def setup_logger(name: str = "trading_bot", log_dir: str = "data/logs") -> logging.Logger:
    """
    Attach non-blocking console and rotating-file queues to the ROOT logger.

    Queue handlers go on the root rather than on `trading_bot` so modules using
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

    # setup_logger is normally called once, but keeping reconfiguration clean is
    # useful in tests and interactive sessions.
    shutdown_logging()
    for handler in root.handlers:
        handler.close()
    root.handlers.clear()

    # Set level from environment or default to INFO. Normalising the value makes
    # ``LOG_LEVEL=debug`` behave the same as ``DEBUG``; an invalid value falls
    # back safely instead of aborting the bot during import.
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    root.setLevel(log_level)

    for library in _NOISY_LIBRARIES:
        # Discord diagnostics are useful when the operator explicitly enables
        # DEBUG. Keep every other third-party library quiet so aiohttp/asyncpg
        # internals do not bury the gateway and rate-limit records we need.
        library_level = (
            logging.DEBUG
            if library == "discord" and log_level == logging.DEBUG
            else logging.WARNING
        )
        logging.getLogger(library).setLevel(library_level)

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
    console_handler.setLevel(logging.DEBUG if log_level == logging.DEBUG else logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # Force UTF-8 encoding for Windows console
    if sys.platform == "win32":
        import locale

        if locale.getpreferredencoding().upper() != "UTF-8":
            # Reconfigure stdout to handle UTF-8
            sys.stdout.reconfigure(encoding="utf-8")

    # File handler for all logs (with UTF-8 encoding)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "bot.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",  # Explicitly set UTF-8 encoding
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    # Error file handler (with UTF-8 encoding)
    error_handler = RotatingFileHandler(
        os.path.join(log_dir, "errors.log"),
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding="utf-8",  # Explicitly set UTF-8 encoding
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    # Console and disk have separate queues/listeners. A stuck console therefore
    # cannot delay bot.log, and neither sink can block the asyncio event loop.
    console_queue = queue.Queue(maxsize=1000)
    file_queue = queue.Queue(maxsize=10000)
    console_queue_handler = _DroppingQueueHandler(console_queue)
    console_queue_handler.setLevel(console_handler.level)
    file_queue_handler = _DroppingQueueHandler(file_queue)
    file_queue_handler.setLevel(logging.DEBUG)

    console_listener = QueueListener(
        console_queue, console_handler, respect_handler_level=True
    )
    file_listener = QueueListener(
        file_queue, file_handler, error_handler, respect_handler_level=True
    )
    _active_listeners.extend((console_listener, file_listener))
    _active_sink_handlers.extend((console_handler, file_handler, error_handler))

    root.addHandler(console_queue_handler)
    root.addHandler(file_queue_handler)
    console_listener.start()
    file_listener.start()

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
