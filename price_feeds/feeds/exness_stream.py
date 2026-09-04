"""
Exness MT5 Streaming Feed

Spawns a child process (exness_worker.py) that holds its own MT5 connection,
isolated from the ICMarkets connection in the main process. Communication
is via JSON lines over stdin/stdout.

All subprocess syscalls (spawn, kill, wait, blocking reads) run OFF the event
loop — the spawn/teardown via asyncio.to_thread and stdout consumption on a
dedicated reader thread — because on the Windows Proactor loop those calls block
the loop thread synchronously, where asyncio.wait_for guards cannot interrupt
them. A wedged spawn or reap there froze the whole bot; isolating them keeps the
loop responsive so the worst case is a failed reconnect, not a frozen process.
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_WORKER_PATH = str(Path(__file__).resolve().parent / "exness_worker.py")
_CONNECT_TIMEOUT = 30
# Bounds on child-process spawn and kill/reap. Even off-loop these are bounded so
# a stuck terminal can't tie up worker threads indefinitely.
_SPAWN_TIMEOUT = 15
_KILL_TIMEOUT = 5


class ExnessStream:
    def __init__(self):
        self.mt5_path = os.getenv("EXNESS_MT5_PATH", "")
        self.login = os.getenv("EXNESS_MT5_LOGIN", "")
        self.password = os.getenv("EXNESS_MT5_PASSWORD", "")
        self.server = os.getenv("EXNESS_MT5_SERVER", "")

        self.connected = False
        self.subscribed_symbols: set[str] = set()
        self.streaming = False

        # Plain (non-asyncio) Popen so its syscalls run on worker threads, never
        # the loop. A dedicated reader thread pumps parsed price ticks into an
        # asyncio.Queue that stream_prices() consumes.
        self._process: Optional[subprocess.Popen] = None
        self._queue: Optional[asyncio.Queue] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._reader_thread: Optional[threading.Thread] = None

        logger.debug("ExnessStream initialized")

    def _spawn(self) -> subprocess.Popen:
        """Blocking child-process spawn — always called via asyncio.to_thread."""
        return subprocess.Popen(
            [sys.executable, _WORKER_PATH, self.mt5_path, self.login, self.password, self.server],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    async def connect(self) -> bool:
        if not self.mt5_path:
            logger.error("EXNESS_MT5_PATH not set")
            return False

        logger.debug(
            "Starting Exness worker: path=%s login=%s server=%s",
            self.mt5_path, self.login, self.server,
        )

        try:
            self._process = await asyncio.wait_for(
                asyncio.to_thread(self._spawn), timeout=_SPAWN_TIMEOUT
            )

            first_line = await asyncio.wait_for(
                asyncio.to_thread(self._process.stdout.readline), timeout=_CONNECT_TIMEOUT
            )
            if not first_line:
                stderr_out = await self._read_stderr()
                logger.error(f"Exness worker exited without output. stderr: {stderr_out}")
                await self._kill_process()
                return False

            msg = json.loads(first_line)
            if "error" in msg:
                stderr_out = await self._read_stderr()
                logger.error(f"Exness worker failed: {msg['error']}. stderr: {stderr_out}")
                await self._kill_process()
                return False

            self.connected = True
            self._loop = asyncio.get_running_loop()
            self._queue = asyncio.Queue()
            self._reader_thread = threading.Thread(
                target=self._read_loop,
                args=(self._process, self._queue),
                name="exness-reader",
                daemon=True,
            )
            self._reader_thread.start()

            logger.debug(f"Connected to Exness MT5 - {msg.get('terminal', 'unknown')}")
            return True

        except asyncio.TimeoutError:
            logger.error("Exness worker connection timed out")
            await self._kill_process()
            return False
        except Exception as e:
            logger.error(f"Error connecting to Exness: {e}")
            await self._kill_process()
            return False

    def _read_loop(self, proc: subprocess.Popen, queue: asyncio.Queue):
        """Consume the worker's stdout on a dedicated thread and hand parsed
        ticks to the loop. Pushes a None sentinel on EOF (worker exited).

        proc/queue are passed explicitly so a reader left over from a previous
        connection posts its sentinel to its own queue, never the live one.
        """
        try:
            for raw in proc.stdout:
                line = raw.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "s" in msg:
                    data = (
                        msg["s"],
                        {
                            "bid": msg["b"],
                            "ask": msg["a"],
                            "timestamp": datetime.fromtimestamp(msg["t"], tz=timezone.utc),
                        },
                    )
                    self._loop.call_soon_threadsafe(queue.put_nowait, data)
        except Exception as e:
            logger.error(f"Error reading Exness stream: {e}")
        finally:
            self._loop.call_soon_threadsafe(queue.put_nowait, None)

    async def disconnect(self):
        self.streaming = False
        proc = self._process
        if proc and proc.poll() is None:
            try:
                self._send_command({"cmd": "shutdown"})
                await asyncio.to_thread(proc.wait, 5)
            except subprocess.TimeoutExpired:
                await self._kill_process()
            except Exception:
                await self._kill_process()
        self._process = None
        self.connected = False
        logger.debug("Disconnected from Exness MT5")

    async def reconnect(self):
        await self.disconnect()
        await asyncio.sleep(2)
        success = await self.connect()
        if success and self.subscribed_symbols:
            for symbol in list(self.subscribed_symbols):
                self._send_command({"cmd": "subscribe", "symbol": symbol})
            self.streaming = True
        return success

    async def subscribe(self, symbol: str):
        if not self.connected:
            raise Exception("Not connected to Exness MT5")
        self._send_command({"cmd": "subscribe", "symbol": symbol})
        self.subscribed_symbols.add(symbol)
        logger.debug(f"Subscribed to {symbol} on Exness MT5")

    async def unsubscribe(self, symbol: str):
        self.subscribed_symbols.discard(symbol)
        if self.connected:
            self._send_command({"cmd": "unsubscribe", "symbol": symbol})

    async def bulk_subscribe(self, symbols: list):
        for symbol in symbols:
            try:
                await self.subscribe(symbol)
            except Exception as e:
                logger.error(f"Failed to subscribe to {symbol}: {e}")

    async def stream_prices(self) -> AsyncIterator[tuple[str, dict]]:
        if not self.connected or self._queue is None:
            raise Exception("Not connected to Exness MT5")

        self.streaming = True

        while self.streaming:
            item = await self._queue.get()
            if item is None:
                # Worker exited (EOF). Mark down so the next stream_prices()
                # call raises and the stream manager reconnects.
                returncode = self._process.poll() if self._process else None
                stderr_out = await self._read_stderr()
                logger.error(
                    "Exness worker exited (code %s). stderr: %s", returncode, stderr_out
                )
                self.connected = False
                return
            yield item

    def get_subscribed_symbols(self) -> set[str]:
        return self.subscribed_symbols.copy()

    def _send_command(self, cmd: dict):
        proc = self._process
        if proc and proc.stdin:
            try:
                proc.stdin.write((json.dumps(cmd) + "\n").encode())
                proc.stdin.flush()
            except (OSError, ValueError):
                # Broken/closed pipe — the worker is gone; reconnect handles it.
                pass

    async def _read_stderr(self) -> str:
        proc = self._process
        if not proc or not proc.stderr:
            return "(no stderr)"
        try:
            data = await asyncio.wait_for(
                asyncio.to_thread(proc.stderr.read, 4096), timeout=2
            )
            return data.decode(errors="replace").strip() or "(empty)"
        except (asyncio.TimeoutError, Exception):
            return "(read failed)"

    async def _kill_process(self):
        proc = self._process
        if proc and proc.poll() is None:
            try:
                await asyncio.to_thread(proc.kill)
                await asyncio.to_thread(proc.wait, _KILL_TIMEOUT)
            except subprocess.TimeoutExpired:
                logger.error("Exness worker did not exit after kill within %ss", _KILL_TIMEOUT)
            except Exception:
                pass
