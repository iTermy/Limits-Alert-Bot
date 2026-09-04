"""Safe, opt-in diagnostics for Discord REST rate-limit responses.

Doubles as the bot's only window onto its real per-channel write budget:
Discord's documentation is explicit that per-route limits are dynamic and must
be read from the response rather than hard coded, and discord.py keeps its
bucket state private. Observing the headers as they arrive is what lets
`ChannelBudget` pace cosmetic edits against the channel's actual allowance.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional

import aiohttp

from utils.logger import get_logger

logger = get_logger("discord_rate_limits")

# Message writes on a channel share one bucket regardless of verb, so the
# channel id is the only part of the path the budget needs.
_MESSAGE_ROUTE = re.compile(r"/channels/(\d+)/messages(?:/\d+)?/?$")


@dataclass(frozen=True)
class ChannelWrite:
    """What one message-route response says about a channel's write budget."""

    channel_id: int
    limit: int
    remaining: int
    reset_after: float
    # Seconds Discord asked us to wait; 0.0 unless this response was a 429.
    retry_after: float = 0.0

    @property
    def bucket_was_fresh(self) -> bool:
        """True when this was the first request of a new window.

        Only then does `Reset-After` state the bucket's full period — every
        other response reports whatever is left of it. Reading a remainder as a
        period is how the budget once talked itself into a 2.7 s window on a
        5 s bucket and generated a 429 on every sweep.
        """
        return self.remaining == self.limit - 1

    @property
    def rate_limited(self) -> bool:
        return self.retry_after > 0.0


BudgetObserver = Callable[[ChannelWrite], None]


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


def _float_header(headers, name: str) -> float:
    try:
        return float(headers.get(name, 0.0))
    except (TypeError, ValueError):
        return 0.0


def _channel_message_write(url: Any, headers, status: int) -> Optional[ChannelWrite]:
    """Describe a message-route response, or None if it is not one."""
    match = _MESSAGE_ROUTE.search(str(url.path))
    if not match:
        return None

    raw_limit = headers.get("X-RateLimit-Limit")
    raw_remaining = headers.get("X-RateLimit-Remaining")
    raw_reset = headers.get("X-RateLimit-Reset-After")
    if raw_limit is None or raw_remaining is None or raw_reset is None:
        return None

    try:
        return ChannelWrite(
            channel_id=int(match.group(1)),
            limit=int(raw_limit),
            remaining=int(raw_remaining),
            reset_after=float(raw_reset),
            retry_after=_float_header(headers, "Retry-After") if status == 429 else 0.0,
        )
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

    `observer` is called for every channel message write, including 429s — a
    429 is the one response that states outright that the channel's real
    allowance is smaller than the bucket it advertises.
    """

    async def on_request_end(_session, _trace_config_ctx, params) -> None:
        response = params.response

        if observer is not None:
            write = _channel_message_write(params.url, response.headers, response.status)
            if write is not None:
                observer(write)

        if response.status == 429 and logger.isEnabledFor(logging.DEBUG):
            _log_429(params)

    trace = aiohttp.TraceConfig()
    trace.on_request_end.append(on_request_end)
    return trace
