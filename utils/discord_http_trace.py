"""Safe, opt-in diagnostics for Discord REST rate-limit responses."""

import logging
from typing import Any

import aiohttp

from utils.logger import get_logger

logger = get_logger("discord_rate_limits")


def _safe_route(url: Any) -> str:
    """Return a route without webhook or interaction tokens."""
    parts = str(url.path).split("/")
    for marker in ("webhooks", "interactions"):
        try:
            marker_index = parts.index(marker)
        except ValueError:
            continue

        token_index = marker_index + 2
        if token_index < len(parts):
            parts[token_index] = "<redacted>"

    return "/".join(parts)


async def _log_rate_limit_response(_session, _trace_config_ctx, params) -> None:
    """Log selected 429 metadata without headers containing credentials."""
    response = params.response
    if response.status != 429 or not logger.isEnabledFor(logging.DEBUG):
        return

    headers = response.headers
    logger.debug(
        "Discord HTTP 429 headers: method=%s route=%s limit=%s remaining=%s "
        "reset=%s reset_after_s=%s scope=%s retry_after_s=%s global=%s bucket=%s",
        params.method,
        _safe_route(params.url),
        headers.get("X-RateLimit-Limit", "<missing>"),
        headers.get("X-RateLimit-Remaining", "<missing>"),
        headers.get("X-RateLimit-Reset", "<missing>"),
        headers.get("X-RateLimit-Reset-After", "<missing>"),
        headers.get("X-RateLimit-Scope", "<missing>"),
        headers.get("Retry-After", "<missing>"),
        headers.get("X-RateLimit-Global", "<missing>"),
        headers.get("X-RateLimit-Bucket", "<missing>"),
    )


def build_discord_http_trace() -> aiohttp.TraceConfig:
    """Build the trace used by discord.py's private aiohttp session."""
    trace = aiohttp.TraceConfig()
    trace.on_request_end.append(_log_rate_limit_response)
    return trace
