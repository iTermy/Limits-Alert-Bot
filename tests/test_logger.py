"""Logging-level and non-blocking delivery tests."""

import logging
import os
import queue
import sys
import threading
import time
from logging.handlers import QueueHandler, QueueListener

import utils.logger as logger_module


def _restore_logging(monkeypatch, original_log_level, original_levels, original_excepthook):
    logger_module.shutdown_logging()
    if original_log_level is None:
        monkeypatch.delenv("LOG_LEVEL", raising=False)
    else:
        monkeypatch.setenv("LOG_LEVEL", original_log_level)
    logger_module.setup_logger()
    for name, level in original_levels.items():
        logging.getLogger(name).setLevel(level)
    sys.excepthook = original_excepthook


def test_debug_level_enables_discord_and_console(monkeypatch, tmp_path):
    root = logging.getLogger()
    original_log_level = os.getenv("LOG_LEVEL")
    original_levels = {
        "": root.level,
        "discord": logging.getLogger("discord").level,
        "aiohttp": logging.getLogger("aiohttp").level,
    }
    original_excepthook = sys.excepthook

    monkeypatch.setenv("LOG_LEVEL", "debug")
    try:
        logger_module.setup_logger("test_debug_logger", str(tmp_path))

        queue_handlers = [
            handler for handler in root.handlers if isinstance(handler, QueueHandler)
        ]
        console = next(
            handler
            for handler in logger_module._active_sink_handlers
            if type(handler) is logging.StreamHandler
        )
        assert len(queue_handlers) == 2
        assert root.level == logging.DEBUG
        assert console.level == logging.DEBUG
        assert logging.getLogger("discord").level == logging.DEBUG
        assert logging.getLogger("aiohttp").level == logging.WARNING
    finally:
        _restore_logging(
            monkeypatch, original_log_level, original_levels, original_excepthook
        )


def test_invalid_log_level_falls_back_to_info(monkeypatch, tmp_path):
    root = logging.getLogger()
    original_log_level = os.getenv("LOG_LEVEL")
    original_levels = {"": root.level}
    original_excepthook = sys.excepthook

    monkeypatch.setenv("LOG_LEVEL", "not-a-level")
    try:
        logger_module.setup_logger("test_invalid_logger", str(tmp_path))
        assert root.level == logging.INFO
    finally:
        _restore_logging(
            monkeypatch, original_log_level, original_levels, original_excepthook
        )


def test_blocked_stream_does_not_block_caller():
    write_started = threading.Event()
    release_write = threading.Event()

    class BlockingStream:
        def write(self, _message):
            write_started.set()
            release_write.wait(timeout=2)

        def flush(self):
            pass

    sink = logging.StreamHandler(BlockingStream())
    log_queue = queue.Queue(maxsize=10)
    handler = logger_module._DroppingQueueHandler(log_queue)
    listener = QueueListener(log_queue, sink)
    isolated_logger = logging.Logger("blocked-stream-test")
    isolated_logger.addHandler(handler)
    listener.start()

    try:
        started = time.monotonic()
        isolated_logger.warning("test warning")
        elapsed = time.monotonic() - started

        assert elapsed < 0.1
        assert write_started.wait(timeout=1)
    finally:
        release_write.set()
        listener.stop()
        handler.close()
        sink.close()
