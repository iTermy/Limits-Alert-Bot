"""Safe, opt-in diagnostics for Discord REST rate-limit responses.

Doubles as the bot's only window onto its real per-channel write budget:
Discord's documentation is explicit that per-route limits are dynamic and must
be read from the response rather than hard coded, and discord.py keeps its
bucket state private. Observing the headers as they arrive is what lets
`ChannelBudget` pace cosmetic edits against the channel's actual allowance.
"""

import logging
import re
from typing import Any, Callable, Optional

import aiohttp

from utils.logger import get_logger

logger = get_logger("discord_rate_limits")

# Message writes on a channel share one bucket regardless of verb, so the
# channel id is the only part of the path the budget needs.
_MESSAGE_ROUTE = re.compile(r"/channels/(\d+)/messages(?:/\d+)?/?$")

# (channel_id, limit, reset_after_seconds)
BudgetObserver = Callable[[int, int, float], None]


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


def _channel_message_budget(url: Any, headers) -> Optional[tuple[int, int, float]]:
    """Extract (channel_id, limit, reset_after) from a message-route response."""
    match = _MESSAGE_ROUTE.search(str(url.path))
    if not match:
        return None

    raw_limit = headers.get("X-RateLimit-Limit")
    raw_reset = headers.get("X-RateLimit-Reset-After")
    if raw_limit is None or raw_reset is None:
        return None

    try:
        return int(match.group(1)), int(raw_limit), float(raw_reset)
    except (TypeError, ValueError):
        # A malformed header is not worth failing a request over; the budget
        # keeps whatever it already learned.
        return None


def _log_429(params) -> None:
    """Log selected 429 metadata without headers containing credentials."""
    headers = params.response.headers
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


def build_discord_http_trace(observer: Optional[BudgetObserver] = None) -> aiohttp.TraceConfig:
    """Build the trace used by discord.py's private aiohttp session.

    `observer` is called with the bucket reported for every channel message
    write, including 429s — a 429's headers describe the same bucket and are the
    most reliable statement of it.
    """

    async def on_request_end(_session, _trace_config_ctx, params) -> None:
        response = params.response

        if observer is not None:
            budget = _channel_message_budget(params.url, response.headers)
            if budget is not None:
                observer(*budget)

        if response.status == 429 and logger.isEnabledFor(logging.DEBUG):
            _log_429(params)

    trace = aiohttp.TraceConfig()
    trace.on_request_end.append(on_request_end)
    return trace
