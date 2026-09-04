"""
Utility functions for signal operations
"""

from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import Optional

import pytz

MARKET_CLOSE = dtime(16, 45)


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


def _market_close(est, day: date) -> datetime:
    """The 4:45 PM close on a given EST date.

    Built with est.localize() rather than timedelta arithmetic on an aware
    datetime so a span crossing a DST change still lands on 4:45 PM local.
    """
    return est.localize(datetime.combine(day, MARKET_CLOSE))


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

    if expiry_type == "week_end":
        days_until_friday = (4 - now.weekday()) % 7
        if days_until_friday == 0 and now >= _market_close(est, now.date()):
            days_until_friday = 7
        expiry = _market_close(est, now.date() + timedelta(days=days_until_friday))

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
        # day_end, and the fallback for anything unrecognised. A close that
        # would land in the weekend gap moves to Monday: the market is shut,
        # so Saturday 4:45 PM is not an end of day anyone traded through.
        day = now.date()
        if now >= _market_close(est, day):
            day += timedelta(days=1)
        while day.weekday() > 4:
            day += timedelta(days=1)
        expiry = _market_close(est, day)

    return expiry.isoformat()
