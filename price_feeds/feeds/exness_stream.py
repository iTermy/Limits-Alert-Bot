"""
Exness MT5 Streaming Feed

Spawns a child process (exness_worker.py) that holds its own MT5 connection,
isolated from the ICMarkets connection in the main process. Communication
is via JSON lines over stdin/stdout.
"""

import asyncio
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Set, Tuple

logger = logging.getLogger(__name__)

_WORKER_PATH = str(Path(__file__).resolve().parent / "exness_worker.py")
_CONNECT_TIMEOUT = 30


class ExnessStream:
    def __init__(self):
        self.mt5_path = os.getenv("EXNESS_MT5_PATH", "")
        self.login = os.getenv("EXNESS_MT5_LOGIN", "")
        self.password = os.getenv("EXNESS_MT5_PASSWORD", "")
        self.server = os.getenv("EXNESS_MT5_SERVER", "")

        self.connected = False
        self.subscribed_symbols: Set[str] = set()
        self.streaming = False
        self._process: asyncio.subprocess.Process = None

        logger.info("ExnessStream initialized")

    async def connect(self) -> bool:
        if not self.mt5_path:
            logger.error("EXNESS_MT5_PATH not set")
            return False

        logger.info(
            "Starting Exness worker: path=%s login=%s server=%s",
            self.mt5_path, self.login, self.server,
        )

        try:
            self._process = await asyncio.create_subprocess_exec(
                sys.executable,
                _WORKER_PATH,
                self.mt5_path,
                self.login,
                self.password,
                self.server,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            first_line = await asyncio.wait_for(
                self._process.stdout.readline(), timeout=_CONNECT_TIMEOUT
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
            logger.info(f"Connected to Exness MT5 - {msg.get('terminal', 'unknown')}")
            return True

        except asyncio.TimeoutError:
            logger.error("Exness worker connection timed out")
            await self._kill_process()
            return False
        except Exception as e:
            logger.error(f"Error connecting to Exness: {e}")
            await self._kill_process()
            return False

    async def disconnect(self):
        self.streaming = False
        if self._process and self._process.returncode is None:
            try:
                self._send_command({"cmd": "shutdown"})
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except (asyncio.TimeoutError, Exception):
                await self._kill_process()
        self._process = None
        self.connected = False
        logger.info("Disconnected from Exness MT5")

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
        logger.info(f"Subscribed to {symbol} on Exness MT5")

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

    async def stream_prices(self) -> AsyncIterator[Tuple[str, Dict]]:
        if not self.connected or not self._process:
            raise Exception("Not connected to Exness MT5")

        self.streaming = True

        while self.streaming:
            try:
                line = await self._process.stdout.readline()
                if not line:
                    if self._process.returncode is not None:
                        stderr_out = await self._read_stderr()
                        logger.error(
                            "Exness worker exited (code %s). stderr: %s",
                            self._process.returncode, stderr_out,
                        )
                        self.connected = False
                        return
                    continue

                msg = json.loads(line)

                if "s" in msg:
                    yield msg["s"], {
                        "bid": msg["b"],
                        "ask": msg["a"],
                        "timestamp": datetime.fromtimestamp(msg["t"], tz=timezone.utc),
                    }

            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading Exness stream: {e}")
                await asyncio.sleep(1)

    def get_subscribed_symbols(self) -> Set[str]:
        return self.subscribed_symbols.copy()

    def _send_command(self, cmd: dict):
        if self._process and self._process.stdin:
            data = json.dumps(cmd) + "\n"
            self._process.stdin.write(data.encode())

    async def _read_stderr(self) -> str:
        if not self._process or not self._process.stderr:
            return "(no stderr)"
        try:
            data = await asyncio.wait_for(self._process.stderr.read(4096), timeout=2)
            return data.decode(errors="replace").strip() or "(empty)"
        except (asyncio.TimeoutError, Exception):
            return "(read failed)"

    async def _kill_process(self):
        if self._process and self._process.returncode is None:
            self._process.kill()
            await self._process.wait()
