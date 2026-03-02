"""
News Mode Manager - Tracks active news windows during which signals are auto-cancelled.

Usage:
    !news USD 12:30pm 15                  → cancel all USD pairs hit within 15 min of 12:30pm (EST)
    !news gold 8:30am                     → cancel all GOLD signals hit within 15 min of 8:30am (default window)
    !news all 14:00 30                    → cancel ALL signals hit within 30 min of 14:00
    !news USD 14:30 tz:UTC                → specify timezone (EST is default)
    !news USD 9:30am date:2025-06-15      → schedule for a specific date
    !news USD 9:30am date:06/15           → date without year (uses current year)
    !news now [category]                  → activate an immediate open-ended window (until !news off)
    !news off                             → deactivate all "now" windows

Tags (can be added in any order after the time):
    tz:<timezone>    e.g. tz:UTC  tz:EST  tz:GMT  tz:CST  tz:PST  tz:London
    date:<date>      e.g. date:2025-06-15  date:06/15  date:tomorrow

Times are interpreted as Eastern Time (EST/EDT) by default.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import pytz

from utils.logger import get_logger

logger = get_logger('news_manager')

EST = pytz.timezone('America/New_York')

# ---------------------------------------------------------------------------
# Timezone alias map — maps short names to pytz zone names
# ---------------------------------------------------------------------------

TIMEZONE_ALIASES: Dict[str, str] = {
    # Eastern
    'EST': 'America/New_York',
    'EDT': 'America/New_York',
    'ET':  'America/New_York',
    # Central
    'CST': 'America/Chicago',
    'CDT': 'America/Chicago',
    'CT':  'America/Chicago',
    # Mountain
    'MST': 'America/Denver',
    'MDT': 'America/Denver',
    'MT':  'America/Denver',
    # Pacific
    'PST': 'America/Los_Angeles',
    'PDT': 'America/Los_Angeles',
    'PT':  'America/Los_Angeles',
    # UTC / GMT
    'UTC': 'UTC',
    'GMT': 'UTC',
    # European
    'LONDON': 'Europe/London',
    'BST':    'Europe/London',
    'CET':    'Europe/Paris',
    'CEST':   'Europe/Paris',
    'PARIS':  'Europe/Paris',
    'BERLIN': 'Europe/Berlin',
    # Asian
    'JST':   'Asia/Tokyo',
    'TOKYO': 'Asia/Tokyo',
    'HKT':   'Asia/Hong_Kong',
    'SGT':   'Asia/Singapore',
    'IST':   'Asia/Kolkata',
    # Australian
    'AEST':   'Australia/Sydney',
    'AEDT':   'Australia/Sydney',
    'SYDNEY': 'Australia/Sydney',
}


def resolve_timezone(tz_str: str) -> pytz.BaseTzInfo:
    """
    Resolve a timezone string to a pytz timezone.
    Accepts short aliases (EST, UTC, CET) and full pytz names.
    Raises ValueError if unrecognised.
    """
    upper = tz_str.strip().upper()
    if upper in TIMEZONE_ALIASES:
        return pytz.timezone(TIMEZONE_ALIASES[upper])
    # Try as a direct pytz name (e.g. "America/Toronto")
    try:
        return pytz.timezone(tz_str.strip())
    except pytz.UnknownTimeZoneError:
        raise ValueError(
            f"Unknown timezone '{tz_str}'. "
            f"Use a short code like EST, UTC, GMT, CET, JST, or a full pytz name."
        )

# ---------------------------------------------------------------------------
# Currency / category → instrument matching rules
# ---------------------------------------------------------------------------

# All forex currency codes we recognise
FOREX_CURRENCIES: Set[str] = {
    'EUR', 'USD', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF',
    'SEK', 'NOK', 'DKK', 'PLN', 'HUF', 'CZK', 'MXN', 'SGD',
    'HKD', 'ZAR', 'TRY',
}

# Special named categories that map to specific instruments or patterns
NAMED_CATEGORIES: Dict[str, Set[str]] = {
    'GOLD': {'XAUUSD', 'GOLD'},
    'XAU':  {'XAUUSD', 'GOLD'},
    'OIL':  {'USOILSPOT', 'USOIL', 'WTIUSD', 'OIL'},
    'BTC':  {'BTCUSDT', 'BTCUSD'},
    'ETH':  {'ETHUSDT', 'ETHUSD'},
    'CRYPTO': None,   # None = special logic: crypto asset class
}


@dataclass
class NewsEvent:
    """A single news event window."""
    category: str           # raw category string (e.g. "USD", "gold", "all")
    news_time: datetime     # centre of the window (timezone-aware, UTC internally)
    window_minutes: int     # half the window on each side, so total = 2× this
    created_by: str         # Discord username who set the event
    created_at: datetime = field(default_factory=lambda: datetime.now(pytz.utc))
    event_id: int = field(default=0)
    # When True the window has no fixed end_time — stays open until manually deactivated
    is_now_mode: bool = field(default=False)
    # Original timezone label for display (e.g. "EST", "UTC")
    display_tz: str = field(default='EST')

    @property
    def start_time(self) -> datetime:
        return self.news_time - timedelta(minutes=self.window_minutes)

    @property
    def end_time(self) -> datetime:
        if self.is_now_mode:
            # Far future sentinel — effectively never expires on its own
            return self.news_time + timedelta(days=365)
        return self.news_time + timedelta(minutes=self.window_minutes)

    def is_active(self, at: Optional[datetime] = None) -> bool:
        """Return True if the event window covers the given time (default: now)."""
        now = at or datetime.now(pytz.utc)
        return self.start_time <= now <= self.end_time

    def is_expired(self, at: Optional[datetime] = None) -> bool:
        if self.is_now_mode:
            return False  # Never auto-expires; must be manually removed
        now = at or datetime.now(pytz.utc)
        return now > self.end_time

    def instrument_affected(self, instrument: str) -> bool:
        """Return True if *instrument* should be cancelled during this event."""
        cat = self.category.upper()
        instr = instrument.upper()

        # "all" catches everything
        if cat == 'ALL':
            return True

        # Named categories (gold, oil, btc, …)
        if cat in NAMED_CATEGORIES:
            explicit = NAMED_CATEGORIES[cat]
            if explicit is None:
                # Crypto: any instrument that is NOT forex / metals / oil
                return _is_crypto(instr)
            return instr in explicit

        # Forex currency code (USD, EUR, …)
        if cat in FOREX_CURRENCIES:
            # Match any 6-char forex pair that contains this currency on either side
            # but exclude metal/commodity pairs (XAU, XAG, XPT, XPD prefix)
            METAL_PREFIXES = {'XAU', 'XAG', 'XPT', 'XPD', 'BCO', 'WTI'}
            if len(instr) == 6:
                prefix3 = instr[:3]
                suffix3 = instr[3:]
                if prefix3 in METAL_PREFIXES:
                    return False   # e.g. XAUUSD — not a forex pair
                return prefix3 == cat or suffix3 == cat
            return False

        # Fallback: substring match
        return cat in instr

    def __str__(self) -> str:
        if self.is_now_mode:
            return (
                f"[#{self.event_id}] {self.category.upper()} news @ "
                f"NOW (open-ended, manual off required)"
            )
        news_est = self.news_time.astimezone(EST)
        return (
            f"[#{self.event_id}] {self.category.upper()} news @ "
            f"{news_est.strftime('%I:%M %p')} EST "
            f"(±{self.window_minutes} min)"
        )


def _is_crypto(symbol: str) -> bool:
    """Rough check: symbol ends with USDT, USDC, or BTC."""
    return (
        symbol.endswith('USDT')
        or symbol.endswith('USDC')
        or symbol.endswith('BTC')
        or symbol.endswith('USD') and len(symbol) > 6
    )


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class NewsManager:
    """
    Singleton-style manager attached to the bot.

    Stores upcoming / active NewsEvent objects and provides fast lookup
    methods used by the streaming monitor.

    Events are persisted to config/news_events.json so they survive bot
    restarts.  The file is written synchronously on every mutation (events
    are infrequent so this is fine).
    """

    # Path relative to this file: project_root/config/news_events.json
    _CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config' / 'news_events.json'

    def __init__(self):
        self._events: List[NewsEvent] = []
        self._next_id: int = 1
        self._cleanup_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        """Serialise all non-expired events to disk."""
        now = datetime.now(pytz.utc)
        data = {
            "next_id": self._next_id,
            "events": [
                {
                    "event_id":       e.event_id,
                    "category":       e.category,
                    "news_time":      e.news_time.isoformat(),
                    "window_minutes": e.window_minutes,
                    "created_by":     e.created_by,
                    "created_at":     e.created_at.isoformat(),
                    "is_now_mode":    e.is_now_mode,
                    "display_tz":     e.display_tz,
                }
                for e in self._events
                if not e.is_expired(now)   # don't bother saving already-expired events
            ],
        }
        try:
            self._CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            self._CONFIG_PATH.write_text(
                json.dumps(data, indent=2), encoding='utf-8'
            )
            logger.debug(f"News events saved to {self._CONFIG_PATH} ({len(data['events'])} event(s))")
        except Exception as e:
            logger.error(f"Failed to save news events to disk: {e}")

    def load_from_file(self) -> int:
        """
        Load events from disk, discarding any that have already expired.
        Returns the number of events loaded.
        Called once at startup.
        """
        if not self._CONFIG_PATH.exists():
            logger.debug("No news_events.json found — starting fresh")
            return 0

        try:
            raw = json.loads(self._CONFIG_PATH.read_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"Failed to read news_events.json: {e}")
            return 0

        self._next_id = raw.get("next_id", 1)
        now = datetime.now(pytz.utc)
        loaded = 0

        for item in raw.get("events", []):
            try:
                news_time = datetime.fromisoformat(item["news_time"])
                created_at = datetime.fromisoformat(item["created_at"])

                # Ensure timezone-aware
                if news_time.tzinfo is None:
                    news_time = pytz.utc.localize(news_time)
                if created_at.tzinfo is None:
                    created_at = pytz.utc.localize(created_at)

                event = NewsEvent(
                    category=item["category"],
                    news_time=news_time,
                    window_minutes=item["window_minutes"],
                    created_by=item["created_by"],
                    created_at=created_at,
                    event_id=item["event_id"],
                    is_now_mode=item.get("is_now_mode", False),
                    display_tz=item.get("display_tz", "EST"),
                )

                if not event.is_expired(now):
                    self._events.append(event)
                    loaded += 1
                else:
                    logger.debug(f"Skipping expired event on load: {event}")

            except Exception as e:
                logger.warning(f"Skipping malformed news event entry: {item} — {e}")

        logger.info(f"Loaded {loaded} news event(s) from {self._CONFIG_PATH}")
        return loaded

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_event(
        self,
        category: str,
        news_time: datetime,
        window_minutes: int,
        created_by: str,
        is_now_mode: bool = False,
        display_tz: str = 'EST',
    ) -> NewsEvent:
        """Register a new news event, persist to disk, and return it."""
        event = NewsEvent(
            category=category,
            news_time=news_time,
            window_minutes=window_minutes,
            created_by=created_by,
            event_id=self._next_id,
            is_now_mode=is_now_mode,
            display_tz=display_tz,
        )
        self._next_id += 1
        self._events.append(event)
        self._save()
        logger.info(f"News event added: {event}")
        return event

    def remove_now_events(self) -> int:
        """Remove all 'now' mode events. Returns count removed."""
        before = len(self._events)
        self._events = [e for e in self._events if not e.is_now_mode]
        removed = before - len(self._events)
        if removed:
            self._save()
        return removed

    def is_news_active_for(self, instrument: str) -> Optional[NewsEvent]:
        """
        Return the first active NewsEvent that affects *instrument*, or None.
        Called on every limit-hit check — must be fast.
        """
        now = datetime.now(pytz.utc)
        for event in self._events:
            if event.is_active(now) and event.instrument_affected(instrument):
                return event
        return None

    def get_all_events(self) -> List[NewsEvent]:
        """Return all non-expired events (for display)."""
        now = datetime.now(pytz.utc)
        return [e for e in self._events if not e.is_expired(now)]

    def remove_event(self, event_id: int) -> bool:
        """Manually remove an event by ID, persist to disk. Returns True if found."""
        before = len(self._events)
        self._events = [e for e in self._events if e.event_id != event_id]
        found = len(self._events) < before
        if found:
            self._save()
        return found

    def purge_expired(self):
        """Remove events that have fully passed and persist the updated list."""
        now = datetime.now(pytz.utc)
        before = len(self._events)
        self._events = [e for e in self._events if not e.is_expired(now)]
        removed = before - len(self._events)
        if removed:
            logger.debug(f"Purged {removed} expired news event(s)")
            self._save()

    def start_cleanup_task(self, alert_system=None):
        """
        Start a background task that:
        - Every 30 s: fires a 'news activated' alert for any window that just opened
        - Every 5 min: purges expired events from memory and disk
        """
        async def _run():
            # Track which events have already had their activation alert sent
            alerted_ids: set = set()

            while True:
                await asyncio.sleep(30)
                now = datetime.now(pytz.utc)

                # Fire activation alerts for windows that are now open
                if alert_system is not None:
                    for event in self._events:
                        if (
                            event.event_id not in alerted_ids
                            and event.is_active(now)
                        ):
                            alerted_ids.add(event.event_id)
                            try:
                                await alert_system.send_news_activated_alert(event)
                            except Exception as e:
                                logger.error(f"Failed to send news activated alert: {e}")

                # Purge expired events (every ~5 min: 10 × 30 s)
                if not hasattr(_run, '_purge_counter'):
                    _run._purge_counter = 0
                _run._purge_counter += 1
                if _run._purge_counter >= 10:
                    _run._purge_counter = 0
                    self.purge_expired()
                    # Also clean up alerted IDs for events that no longer exist
                    active_ids = {e.event_id for e in self._events}
                    alerted_ids &= active_ids

        # Cancel any previously running cleanup task so we don't leak coroutines
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()

        self._cleanup_task = asyncio.ensure_future(_run())


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_news_command(args_str: str) -> Tuple[str, datetime, int, str]:
    """
    Parse the argument string from !news and return
    (category, news_time_utc, window_minutes, display_tz_label).

    Supported format:
        <category> <time> [window_minutes] [tz:<timezone>] [date:<date>]

    Tags can appear in any order after the time token.

    Examples:
        USD 12:30pm 15
        gold 8:30am
        all 14:00 30
        JPY 9:30AM tz:UTC
        USD 14:30 tz:London date:2025-06-20
        EUR 9:00am date:tomorrow tz:CET
        all 8:30am date:06/15
    """
    tokens = args_str.strip().split()
    if len(tokens) < 2:
        raise ValueError(
            "Usage: `!news <category> <time> [window] [tz:<timezone>] [date:<date>]`"
        )

    category = tokens[0].upper()

    # Validate / warn about category
    is_valid = (
        category == 'ALL'
        or category in FOREX_CURRENCIES
        or category in NAMED_CATEGORIES
        or category in {k.upper() for k in NAMED_CATEGORIES}
    )
    if not is_valid:
        logger.warning(f"Unknown news category: {category!r} — treating as exact match")

    # ---- Extract tags from remaining tokens ----
    remaining = tokens[1:]
    tz_label: str = 'EST'
    tz_zone: pytz.BaseTzInfo = EST
    date_override: Optional[datetime] = None
    window_minutes: int = 10
    time_str: Optional[str] = None

    non_tag_tokens = []
    for tok in remaining:
        lower = tok.lower()
        if lower.startswith('tz:'):
            tz_str = tok[3:]
            tz_zone = resolve_timezone(tz_str)           # raises ValueError if bad
            tz_label = tz_str.upper()
        elif lower.startswith('date:'):
            date_val = tok[5:]
            date_override = _parse_date(date_val, tz_zone)  # raises ValueError if bad
        else:
            non_tag_tokens.append(tok)

    # non_tag_tokens: first is time, optional second is window_minutes
    if not non_tag_tokens:
        raise ValueError("Missing time argument.")

    time_str = non_tag_tokens[0]
    if len(non_tag_tokens) >= 2:
        try:
            window_minutes = int(non_tag_tokens[1])
        except ValueError:
            raise ValueError(f"Window minutes must be an integer, got '{non_tag_tokens[1]}'")

    # Parse the time, using date_override if provided
    news_time_local = _parse_time(time_str, tz_zone, date_override)
    news_time_utc = news_time_local.astimezone(pytz.utc)

    # If no explicit date was given and the window has already fully passed,
    # auto-advance to the same time tomorrow.  This prevents silently scheduling
    # an event that is already expired (which would never appear in !newslist).
    if date_override is None:
        now_utc = datetime.now(pytz.utc)
        window_end_utc = news_time_utc + timedelta(minutes=window_minutes)
        if window_end_utc < now_utc:
            news_time_local = news_time_local + timedelta(days=1)
            news_time_utc = news_time_local.astimezone(pytz.utc)
            logger.info(
                f"News time {time_str} has already passed today — "
                f"auto-advanced to tomorrow: {news_time_utc.isoformat()}"
            )

    return category, news_time_utc, window_minutes, tz_label


def _parse_date(date_str: str, tz_zone: pytz.BaseTzInfo) -> datetime:
    """
    Parse a date string, returning a timezone-aware datetime at midnight in tz_zone.

    Accepts:
        YYYY-MM-DD      e.g. 2025-06-15
        MM/DD           e.g. 06/15  (current year assumed)
        MM-DD           e.g. 06-15  (current year assumed)
        tomorrow        next calendar day in the given tz
        today           today in the given tz
    """
    now_local = datetime.now(tz_zone)
    lower = date_str.lower().strip()

    if lower == 'today':
        return now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if lower == 'tomorrow':
        return (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # YYYY-MM-DD
    m = re.fullmatch(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        naive = datetime(year, month, day, 0, 0, 0)
        return tz_zone.localize(naive)

    # MM/DD or MM-DD
    m = re.fullmatch(r'(\d{1,2})[/-](\d{1,2})', date_str)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = now_local.year
        naive = datetime(year, month, day, 0, 0, 0)
        return tz_zone.localize(naive)

    raise ValueError(
        f"Cannot parse date '{date_str}'. "
        "Use YYYY-MM-DD, MM/DD, 'today', or 'tomorrow'."
    )


def _parse_time(
    time_str: str,
    tz_zone: pytz.BaseTzInfo,
    date_override: Optional[datetime] = None,
) -> datetime:
    """
    Parse a time string into a timezone-aware datetime in tz_zone.
    If date_override is given, use that date; otherwise use today in tz_zone.
    Accepts: 12:30pm, 12:30PM, 8:30am, 14:00, 9:30, 930am, 2pm, etc.
    """
    if date_override is not None:
        base = date_override.astimezone(tz_zone)
    else:
        base = datetime.now(tz_zone)

    time_str = time_str.strip()

    formats = [
        '%I:%M%p',   # 12:30pm / 12:30PM
        '%I:%M %p',  # 12:30 pm / 12:30 PM
        '%H:%M',     # 14:00
        '%H%M',      # 1430
        '%I%p',      # 2pm / 2PM
    ]

    parsed = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(time_str.upper(), fmt)
            break
        except ValueError:
            continue

    if parsed is None:
        raise ValueError(
            f"Cannot parse time '{time_str}'. "
            "Use formats like: 12:30pm, 14:00, 8:30am"
        )

    result = base.replace(
        hour=parsed.hour,
        minute=parsed.minute,
        second=0,
        microsecond=0,
    )
    return result


# Keep legacy name for any external callers
def _parse_time_est(time_str: str) -> datetime:
    """Legacy wrapper — parses time as EST for today."""
    return _parse_time(time_str, EST)