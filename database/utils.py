"""
Utility functions for signal operations
"""

from datetime import datetime, timedelta
from typing import Optional

import pytz

async def calculate_sl_pnl(signal_id: int, signal: dict, signal_db, tp_config) -> Optional[float]:
    """Return combined P&L of all hit limits at the stop-loss price, or None if no hit limits."""
    stop_price = signal.get("stop_loss")
    if not stop_price:
        return None
    hit_limits = await signal_db.get_hit_limits_for_signal(signal_id)
    if not hit_limits:
        return None
    combined = 0.0
    for lim in hit_limits:
        entry = lim.get("hit_price") or lim.get("price_level")
        if entry is not None:
            combined += tp_config.calculate_pnl(
                signal["instrument"],
                signal["direction"],
                entry,
                stop_price,
                signal_type=signal.get("type", "standard"),
            )
    return combined


def _parse_dt(value) -> Optional[datetime]:
    """Convert an ISO string or datetime to a timezone-aware datetime, or return None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else pytz.UTC.localize(value)
    s = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt


def calculate_expiry(expiry_type: str) -> Optional[str]:
    """
    Calculate expiry timestamp based on type.

    Returns:
        ISO format timestamp or None
    """
    if expiry_type == "no_expiry":
        return None

    est = pytz.timezone("America/New_York")
    now = datetime.now(est)

    if expiry_type == "day_end":
        expiry = now.replace(hour=16, minute=45, second=0, microsecond=0)
        if now >= expiry:
            expiry += timedelta(days=1)

    elif expiry_type == "week_end":
        days_until_friday = (4 - now.weekday()) % 7
        if days_until_friday == 0 and now >= now.replace(
            hour=16, minute=45, second=0, microsecond=0
        ):
            days_until_friday = 7
        expiry = now + timedelta(days=days_until_friday)
        expiry = expiry.replace(hour=16, minute=45, second=0, microsecond=0)

    elif expiry_type == "month_end":
        # Use est.localize() (not tzinfo=est) to get the correct modern UTC offset.
        # Passing tzinfo=est directly to datetime() uses pytz's LMT offset, which
        # is wrong by several minutes.
        next_month = now.month + 1 if now.month < 12 else 1
        year = now.year if now.month < 12 else now.year + 1
        first_of_next = est.localize(datetime(year, next_month, 1, 16, 45, 0))
        last_day = first_of_next - timedelta(days=1)
        while last_day.weekday() > 4:
            last_day -= timedelta(days=1)
        expiry = last_day

    else:
        expiry = now.replace(hour=16, minute=45, second=0, microsecond=0)
        if now >= expiry:
            expiry += timedelta(days=1)

    return expiry.isoformat()
