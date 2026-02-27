"""
News Mode Manager - Tracks active news windows during which signals are auto-cancelled.

Usage:
    !news USD 12:30pm 15   → cancel all USD pairs hit within 15 min of 12:30pm
    !news gold 8:30am      → cancel all GOLD signals hit within 15 min of 8:30am (default window)
    !news all 14:00 30     → cancel ALL signals hit within 30 min of 14:00

Times are interpreted as Eastern Time (EST/EDT).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import pytz

from utils.logger import get_logger

logger = get_logger('news_manager')

EST = pytz.timezone('America/New_York')

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

    @property
    def start_time(self) -> datetime:
        return self.news_time - timedelta(minutes=self.window_minutes)

    @property
    def end_time(self) -> datetime:
        return self.news_time + timedelta(minutes=self.window_minutes)

    def is_active(self, at: Optional[datetime] = None) -> bool:
        """Return True if the event window covers the given time (default: now)."""
        now = at or datetime.now(pytz.utc)
        return self.start_time <= now <= self.end_time

    def is_expired(self, at: Optional[datetime] = None) -> bool:
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
    ) -> NewsEvent:
        """Register a new news event, persist to disk, and return it."""
        event = NewsEvent(
            category=category,
            news_time=news_time,
            window_minutes=window_minutes,
            created_by=created_by,
            event_id=self._next_id,
        )
        self._next_id += 1
        self._events.append(event)
        self._save()
        logger.info(f"News event added: {event}")
        return event

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

        self._cleanup_task = asyncio.ensure_future(_run())


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_news_command(args_str: str) -> tuple[str, datetime, int]:
    """
    Parse the argument string from !news and return (category, news_time_utc, window_minutes).

    Expected format:  <category> <time> [window_minutes]
    Examples:
        USD 12:30pm 15
        gold 8:30am
        all 14:00 30
        JPY 9:30AM
    """
    tokens = args_str.strip().split()
    if len(tokens) < 2:
        raise ValueError("Usage: `!news <category> <time> [window_minutes]`")

    category = tokens[0].upper()

    # Validate category
    is_valid = (
        category == 'ALL'
        or category in FOREX_CURRENCIES
        or category in NAMED_CATEGORIES
        or category in {k.upper() for k in NAMED_CATEGORIES}
    )
    if not is_valid:
        # Still allow it — could be an exact instrument ticker
        logger.warning(f"Unknown news category: {category!r} — treating as exact match")

    time_str = tokens[1]
    window_minutes = int(tokens[2]) if len(tokens) >= 3 else 10

    # Parse time string (supports 12-hour and 24-hour)
    news_time_est = _parse_time_est(time_str)
    news_time_utc = news_time_est.astimezone(pytz.utc)

    return category, news_time_utc, window_minutes


def _parse_time_est(time_str: str) -> datetime:
    """
    Parse a time string into a timezone-aware datetime for today in EST.
    Accepts: 12:30pm, 12:30PM, 8:30am, 14:00, 9:30, 930am, etc.
    """
    now_est = datetime.now(EST)
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

    # Build timezone-aware datetime for today in EST
    result = now_est.replace(
        hour=parsed.hour,
        minute=parsed.minute,
        second=0,
        microsecond=0,
    )
    return result