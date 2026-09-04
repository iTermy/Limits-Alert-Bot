"""Discord REST rate-limit telemetry tests."""

import asyncio
import logging
from types import SimpleNamespace

from yarl import URL

from utils.discord_http_trace import _log_rate_limit_response, _safe_route


def test_safe_route_redacts_discord_tokens():
    webhook_url = URL("https://discord.com/api/v10/webhooks/123/secret-token/messages/456")
    interaction_url = URL("https://discord.com/api/v10/interactions/123/secret-token/callback")

    assert _safe_route(webhook_url) == "/api/v10/webhooks/123/<redacted>/messages/456"
    assert _safe_route(interaction_url) == "/api/v10/interactions/123/<redacted>/callback"


def test_debug_429_logs_rate_limit_headers(caplog):
    params = SimpleNamespace(
        method="PATCH",
        url=URL("https://discord.com/api/v10/channels/123/messages/456"),
        response=SimpleNamespace(
            status=429,
            headers={
                "X-RateLimit-Limit": "5",
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": "1788512400.25",
                "X-RateLimit-Reset-After": "5.28",
                "X-RateLimit-Scope": "shared",
                "Retry-After": "5.28",
                "X-RateLimit-Global": "false",
                "X-RateLimit-Bucket": "bucket-id",
            },
        ),
    )

    with caplog.at_level(logging.DEBUG, logger="trading_bot.discord_rate_limits"):
        asyncio.run(_log_rate_limit_response(None, None, params))

    message = caplog.messages[-1]
    assert "method=PATCH" in message
    assert "route=/api/v10/channels/123/messages/456" in message
    assert "limit=5" in message
    assert "remaining=0" in message
    assert "reset=1788512400.25" in message
    assert "reset_after_s=5.28" in message
    assert "scope=shared" in message
    assert "retry_after_s=5.28" in message
    assert "global=false" in message
    assert "bucket=bucket-id" in message


def test_non_429_is_not_logged(caplog):
    params = SimpleNamespace(
        method="GET",
        url=URL("https://discord.com/api/v10/channels/123"),
        response=SimpleNamespace(status=200, headers={"X-RateLimit-Remaining": "4"}),
    )

    with caplog.at_level(logging.DEBUG, logger="trading_bot.discord_rate_limits"):
        asyncio.run(_log_rate_limit_response(None, None, params))

    assert caplog.messages == []
