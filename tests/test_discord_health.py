"""Tests for Discord gateway and REST watchdog decisions."""

import asyncio
import os
from types import SimpleNamespace

# Importing core.bot constructs the process-global DB manager, but these unit
# tests never connect. Give it a syntactically valid inert DSN.
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://test:test@localhost/test")

import core.bot as bot_module
from core.bot import TradingBot


class FakeBotHealth:
    def __init__(self, *, ready: bool, fetch_result: str = "ok"):
        self.ready = ready
        self.fetch_result = fetch_result
        self.user = SimpleNamespace(id=123)
        self._discord_disconnected_since = None
        self._discord_last_probe = 0.0
        self._discord_rest_failures = 0
        self._discord_restart_task = None
        self.restart_reasons: list[str] = []
        self.logger = bot_module.logger

    def is_ready(self) -> bool:
        return self.ready

    async def fetch_user(self, _user_id: int):
        if self.fetch_result == "error":
            raise ConnectionResetError(10054, "connection reset by peer")
        if self.fetch_result == "hang":
            await asyncio.Event().wait()
        return self.user

    def _schedule_discord_restart(self, reason: str) -> None:
        self.restart_reasons.append(reason)


def test_gateway_disconnect_over_threshold_requests_restart():
    async def scenario():
        fake = FakeBotHealth(ready=False)
        fake._discord_disconnected_since = (
            asyncio.get_running_loop().time()
            - bot_module.DISCORD_DISCONNECT_RESTART_SECONDS
            - 1
        )
        await TradingBot._check_discord_health(fake)
        assert fake.restart_reasons == [
            f"gateway not ready for {bot_module.DISCORD_DISCONNECT_RESTART_SECONDS + 1}s"
        ]

    asyncio.run(scenario())


def test_rest_recovers_before_failure_threshold():
    async def scenario():
        fake = FakeBotHealth(ready=True, fetch_result="error")
        for _ in range(bot_module.DISCORD_REST_FAILURES_BEFORE_RESTART - 1):
            fake._discord_last_probe = 0.0
            await TradingBot._check_discord_health(fake)

        assert not fake.restart_reasons
        fake.fetch_result = "ok"
        fake._discord_last_probe = 0.0
        await TradingBot._check_discord_health(fake)
        assert fake._discord_rest_failures == 0
        assert not fake.restart_reasons

    asyncio.run(scenario())


def test_repeated_rest_resets_request_restart():
    async def scenario():
        fake = FakeBotHealth(ready=True, fetch_result="error")
        for _ in range(bot_module.DISCORD_REST_FAILURES_BEFORE_RESTART):
            fake._discord_last_probe = 0.0
            await TradingBot._check_discord_health(fake)

        assert fake.restart_reasons == [
            f"REST probe failed {bot_module.DISCORD_REST_FAILURES_BEFORE_RESTART} consecutive times"
        ]

    asyncio.run(scenario())


def test_half_open_rest_probe_times_out_and_requests_restart(monkeypatch):
    async def scenario():
        fake = FakeBotHealth(ready=True, fetch_result="hang")
        monkeypatch.setattr(bot_module, "DISCORD_REST_PROBE_TIMEOUT_SECONDS", 0.01)

        for _ in range(bot_module.DISCORD_REST_FAILURES_BEFORE_RESTART):
            fake._discord_last_probe = 0.0
            await TradingBot._check_discord_health(fake)

        assert fake.restart_reasons == [
            f"REST probe failed {bot_module.DISCORD_REST_FAILURES_BEFORE_RESTART} consecutive times"
        ]

    asyncio.run(scenario())
