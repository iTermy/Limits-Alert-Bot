"""Discord REST rate-limit telemetry and budget-observation tests."""

import asyncio
import logging
from types import SimpleNamespace

from yarl import URL

from utils.discord_http_trace import ChannelWrite, _safe_route, build_discord_http_trace

_MESSAGE_HEADERS = {
    "X-RateLimit-Limit": "5",
    "X-RateLimit-Remaining": "0",
    "X-RateLimit-Reset": "1788512400.25",
    "X-RateLimit-Reset-After": "5.28",
    "X-RateLimit-Scope": "shared",
    "Retry-After": "5.28",
    "X-RateLimit-Global": "false",
    "X-RateLimit-Bucket": "bucket-id",
}


def _run_trace(params, observer=None):
    """Drive the real trace handler the client is built with."""
    trace = build_discord_http_trace(observer=observer)
    handler = trace.on_request_end[0]
    asyncio.run(handler(None, None, params))


def _params(method, path, status, headers):
    return SimpleNamespace(
        method=method,
        url=URL(f"https://discord.com{path}"),
        response=SimpleNamespace(status=status, headers=headers),
    )


def test_safe_route_redacts_discord_tokens():
    webhook_url = URL("https://discord.com/api/v10/webhooks/123/secret-token/messages/456")
    interaction_url = URL("https://discord.com/api/v10/interactions/123/secret-token/callback")

    assert _safe_route(webhook_url) == "/api/v10/webhooks/123/<redacted>/messages/456"
    assert _safe_route(interaction_url) == "/api/v10/interactions/123/<redacted>/callback"


def test_debug_429_logs_rate_limit_headers(caplog):
    params = _params("PATCH", "/api/v10/channels/123/messages/456", 429, _MESSAGE_HEADERS)

    with caplog.at_level(logging.DEBUG, logger="trading_bot.discord_rate_limits"):
        _run_trace(params)

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
    params = _params("GET", "/api/v10/channels/123", 200, {"X-RateLimit-Remaining": "4"})

    with caplog.at_level(logging.DEBUG, logger="trading_bot.discord_rate_limits"):
        _run_trace(params)

    assert caplog.messages == []


def test_the_observer_learns_from_a_successful_message_write():
    """Budget learning must not depend on hitting a rate limit first."""
    seen = []
    params = _params("PATCH", "/api/v10/channels/123/messages/456", 200, _MESSAGE_HEADERS)

    _run_trace(params, observer=seen.append)

    assert seen == [ChannelWrite(123, 5, 0, 5.28)]
    assert not seen[0].rate_limited


def test_the_observer_learns_the_retry_from_a_429():
    """A 429 is the only response that states the channel's real allowance."""
    seen = []
    params = _params("POST", "/api/v10/channels/123/messages", 429, _MESSAGE_HEADERS)

    _run_trace(params, observer=seen.append)

    assert seen == [ChannelWrite(123, 5, 0, 5.28, retry_after=5.28)]
    assert seen[0].rate_limited


def test_the_observer_ignores_non_message_routes():
    seen = []
    params = _params("PATCH", "/api/v10/channels/123", 200, _MESSAGE_HEADERS)

    _run_trace(params, observer=seen.append)

    assert seen == []


def test_a_trace_without_an_observer_still_works():
    params = _params("PATCH", "/api/v10/channels/123/messages/456", 200, _MESSAGE_HEADERS)

    _run_trace(params)  # must not raise
