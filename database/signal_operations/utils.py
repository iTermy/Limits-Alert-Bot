"""
Utility functions for signal operations
"""
from typing import Optional
from datetime import datetime, timedelta
import pytz
from database.models import SignalStatus


def calculate_expiry(expiry_type: str) -> Optional[str]:
    """
    Calculate expiry timestamp based on type

    Args:
        expiry_type: Type of expiry

    Returns:
        ISO format timestamp or None
    """
    if expiry_type == 'no_expiry':
        return None

    # Get current time in EST (typical trading timezone)
    est = pytz.timezone('America/New_York')
    now = datetime.now(est)

    if expiry_type == 'day_end':
        # End of current trading day (4:45 PM EST — 15 min before spread hour)
        expiry = now.replace(hour=16, minute=45, second=0, microsecond=0)
        if now >= expiry:
            # If at or past 4:45 PM, set to next trading day
            expiry += timedelta(days=1)

    elif expiry_type == 'week_end':
        # End of trading week (Friday 4:45 PM EST)
        days_until_friday = (4 - now.weekday()) % 7
        if days_until_friday == 0 and now >= now.replace(hour=16, minute=45, second=0, microsecond=0):
            days_until_friday = 7
        expiry = now + timedelta(days=days_until_friday)
        expiry = expiry.replace(hour=16, minute=45, second=0, microsecond=0)

    elif expiry_type == 'month_end':
        # Last trading day of month at 4:45 PM EST
        # Use est.localize() (not tzinfo=est) to get the correct modern UTC offset.
        # Passing tzinfo=est directly to datetime() uses pytz's LMT offset, which
        # is wrong by several minutes.
        next_month = now.month + 1 if now.month < 12 else 1
        year = now.year if now.month < 12 else now.year + 1
        first_of_next = est.localize(datetime(year, next_month, 1, 16, 45, 0))
        # Go back to last weekday
        last_day = first_of_next - timedelta(days=1)
        while last_day.weekday() > 4:  # Saturday = 5, Sunday = 6
            last_day -= timedelta(days=1)
        expiry = last_day

    else:
        # Default to day end
        expiry = now.replace(hour=16, minute=45, second=0, microsecond=0)
        if now >= expiry:
            expiry += timedelta(days=1)

    return expiry.isoformat()


def get_status_emoji(status: str) -> str:
    """
    Get emoji for status display

    Args:
        status: Signal status

    Returns:
        Emoji string
    """
    emoji_map = {
        SignalStatus.ACTIVE: '🟢',
        SignalStatus.HIT: '🎯',
        SignalStatus.PROFIT: '✅',
        SignalStatus.BREAKEVEN: '➖',
        SignalStatus.STOP_LOSS: '🛑',
        SignalStatus.CANCELLED: '❌'
    }
    return emoji_map.get(status, '❓')


def format_time_remaining(expiry_time: str) -> str:
    """
    Format time remaining until expiry

    Args:
        expiry_time: ISO format expiry timestamp

    Returns:
        Formatted string like "2h 30m" or "Expired"
    """
    if not expiry_time:
        return "No expiry"

    try:
        if isinstance(expiry_time, datetime):
            expiry = expiry_time if expiry_time.tzinfo else pytz.UTC.localize(expiry_time)
        else:
            s = str(expiry_time)
            if '+' in s or s.endswith('Z'):
                expiry = datetime.fromisoformat(s.replace('Z', '+00:00'))
            else:
                expiry = pytz.UTC.localize(datetime.fromisoformat(s))
        now = datetime.now(pytz.UTC)

        if expiry.tzinfo is None:
            expiry = pytz.UTC.localize(expiry)

        remaining = expiry - now

        if remaining.total_seconds() <= 0:
            return "Expired"

        days = remaining.days
        hours = int(remaining.total_seconds() // 3600)
        minutes = int((remaining.total_seconds() % 3600) // 60)

        if days > 0:
            return f"{days}d {hours % 24}h"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    except Exception:
        return "Unknown"


def calculate_pip_difference(instrument: str, price1: float, price2: float) -> float:
    """
    Calculate pip difference between two prices

    Args:
        instrument: Trading instrument
        price1: First price
        price2: Second price

    Returns:
        Pip difference
    """
    # Determine pip size based on instrument
    if 'JPY' in instrument.upper():
        # JPY pairs have 0.01 as 1 pip
        pip_size = 0.01
    elif 'XAU' in instrument.upper() or 'GOLD' in instrument.upper():
        # Gold typically uses 0.1 as 1 pip
        pip_size = 0.1
    elif any(crypto in instrument.upper() for crypto in ['BTC', 'ETH', 'CRYPTO']):
        # Crypto uses whole numbers as pips
        pip_size = 1.0
    else:
        # Most forex pairs use 0.0001 as 1 pip
        pip_size = 0.0001

    return abs(price2 - price1) / pip_size


def get_signal_priority(signal: dict) -> int:
    """
    Calculate priority score for a signal (for tracking order)

    Args:
        signal: Signal dictionary

    Returns:
        Priority score (higher = more urgent)
    """
    priority = 0

    # Active signals with no hits yet get highest priority
    if signal['status'] == SignalStatus.ACTIVE:
        priority += 100
    elif signal['status'] == SignalStatus.HIT:
        priority += 50

    # Signals close to expiry get priority boost
    time_remaining = signal.get('time_remaining', '')
    if 'h' in time_remaining:
        hours = int(time_remaining.split('h')[0])
        if hours < 1:
            priority += 30
        elif hours < 4:
            priority += 20
        elif hours < 12:
            priority += 10

    # Signals with fewer pending limits get priority
    # (closer to completion)
    pending_count = len(signal.get('pending_limits', []))
    if pending_count == 1:
        priority += 25
    elif pending_count == 2:
        priority += 15
    elif pending_count == 3:
        priority += 5

    return priority