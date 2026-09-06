"""
News Mode Manager - Tracks news windows for clients and alert-bot guards.

Usage:
    !news USD 12:30pm 15                  → cancel all USD pairs hit within 15 min of 12:30pm (EST)
    !news gold 8:30am                     → cancel all GOLD signals hit within 15 min of 8:30am (default window)
    !news all 14:00 30                    → cancel ALL signals hit within 30 min of 14:00
    !news USD 12:30pm 15 dryrun           → pause clients, but keep alert-bot signals running
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
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pytz

from utils.logger import get_logger

logger = get_logger("news_manager")

EST = pytz.timezone("America/New_York")

# ---------------------------------------------------------------------------
# Timezone alias map — maps short names to pytz zone names
# ---------------------------------------------------------------------------

TIMEZONE_ALIASES: dict[str, str] = {
    # Eastern
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "ET": "America/New_York",
    # Central
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "CT": "America/Chicago",
    # Mountain
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "MT": "America/Denver",
    # Pacific
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "PT": "America/Los_Angeles",
    # UTC / GMT
    "UTC": "UTC",
    "GMT": "UTC",
    # European
    "LONDON": "Europe/London",
    "BST": "Europe/London",
    "CET": "Europe/Paris",
    "CEST": "Europe/Paris",
    "PARIS": "Europe/Paris",
    "BERLIN": "Europe/Berlin",
    # Asian
    "JST": "Asia/Tokyo",
    "TOKYO": "Asia/Tokyo",
    "HKT": "Asia/Hong_Kong",
    "SGT": "Asia/Singapore",
    "IST": "Asia/Kolkata",
    # Australian
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "SYDNEY": "Australia/Sydney",
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
    except pytz.UnknownTimeZoneError as e:
        raise ValueError(
            f"Unknown timezone '{tz_str}'. "
            f"Use a short code like EST, UTC, GMT, CET, JST, or a full pytz name."
        ) from e


# ---------------------------------------------------------------------------
# Currency / category → instrument matching rules
# ---------------------------------------------------------------------------

# All forex currency codes we recognise
FOREX_CURRENCIES: set[str] = {
    "EUR",
    "USD",
    "GBP",
    "JPY",
    "AUD",
    "NZD",
    "CAD",
    "CHF",
    "SEK",
    "NOK",
    "DKK",
    "PLN",
    "HUF",
    "CZK",
    "MXN",
    "SGD",
    "HKD",
    "ZAR",
    "TRY",
}

# US index identifiers (internal/feed symbol fragments). USD news pauses these
# alongside US equities — high-impact dollar releases move US indices hardest.
US_INDEX_KEYWORDS: set[str] = {
    "NAS100",
    "US30",
    "US500",
    "SPX500",
    "SPX",
    "USTEC",
    "US2000",
    "NDX",
    "DJI",
    "DJ30",
    "NASDAQ",
}

# Oil trades under several symbols across the feeds (USOILSPOT on Exness, XTIUSD
# on ICMarkets). Matching the keyword rather than an explicit symbol list keeps a
# newly-added contract from silently escaping every oil news window.
_OIL_KEYWORDS: tuple[str, ...] = ("OIL", "WTI", "BRENT", "XTI", "BCO")

# Gold trades as spot (XAUUSD) and as CFDs on the futures contracts (GCZ26_CFD,
# MGC…), which share no substring with the spot symbol.
_GOLD_FUTURES_PREFIXES: tuple[str, ...] = ("GC", "MGC")
_OTHER_METAL_PREFIXES: tuple[str, ...] = ("XAG", "XPT", "XPD")

# Every index we quote, US and otherwise. Indices settle in a currency and say so
# in the symbol (NAS100USD, DE30EUR), so they have to be recognised before the
# crypto test, which would otherwise claim every long USD-suffixed symbol.
_INDEX_KEYWORDS: tuple[str, ...] = tuple(US_INDEX_KEYWORDS) + (
    "NAS",
    "DAX",
    "DE30",
    "DE40",
    "F40",
    "JP225",
    "UK100",
    "AUS200",
    "AUS2000",
    "HK50",
    "CHINA",
)


def _is_us_stock(symbol: str) -> bool:
    """US equities are stored with a .NAS / .NYSE suffix."""
    return symbol.endswith((".NAS", ".NYSE"))


def _is_us_index(symbol: str) -> bool:
    """Match US index symbols while excluding non-US indices (DAX, JP225, …).

    The keyword has to start the symbol rather than merely appear in it: AUS2000 is
    the Australian small-cap index and contains 'US2000', which would otherwise put
    an Australian index under every dollar release.
    """
    return symbol.startswith(tuple(US_INDEX_KEYWORDS))


def _is_gold(symbol: str) -> bool:
    return symbol in {"XAUUSD", "GOLD"} or symbol.startswith(_GOLD_FUTURES_PREFIXES)


def _is_metal(symbol: str) -> bool:
    return _is_gold(symbol) or symbol == "SILVER" or symbol.startswith(_OTHER_METAL_PREFIXES)


def _is_oil(symbol: str) -> bool:
    return any(keyword in symbol for keyword in _OIL_KEYWORDS)


def _is_crypto(symbol: str) -> bool:
    """Quotes against USDT/USDC, or a long-form USD pair (BTCUSD, SOLUSD).

    Stocks, metals, oil and indices are dollar-denominated too and several carry a
    USD suffix, so they are ruled out before the suffix test — otherwise a crypto
    news window cancels every index signal.
    """
    if _is_us_stock(symbol) or _is_metal(symbol) or _is_oil(symbol):
        return False
    if any(keyword in symbol for keyword in _INDEX_KEYWORDS):
        return False
    return symbol.endswith(("USDT", "USDC", "BTC")) or (
        symbol.endswith("USD") and len(symbol) > 6
    )


# Special named categories, each a predicate over the internal instrument symbol.
NAMED_CATEGORIES: dict[str, Callable[[str], bool]] = {
    "GOLD": _is_gold,
    "XAU": _is_gold,
    "OIL": _is_oil,
    "BTC": lambda s: s.startswith("BTC"),
    "ETH": lambda s: s.startswith("ETH"),
    "CRYPTO": _is_crypto,
}


@dataclass
class NewsEvent:
    """A single news event window."""

    category: str  # raw category string (e.g. "USD", "gold", "all")
    news_time: datetime  # centre of the window (timezone-aware, UTC internally)
    window_minutes: int  # half the window on each side, so total = 2× this
    created_by: str  # Discord username who set the event
    created_at: datetime = field(default_factory=lambda: datetime.now(pytz.utc))
    event_id: int = field(default=0)
    # When True the window has no fixed end_time — stays open until manually deactivated
    is_now_mode: bool = field(default=False)
    # Original timezone label for display (e.g. "EST", "UTC")
    display_tz: str = field(default="EST")
    # Optional explicit end time for timed "now" events (e.g. !news now gold 5 minutes)
    end_time_override: datetime | None = field(default=None)
    # "manual" (set via !news) or "forexfactory" (auto-fetched from the calendar feed)
    source: str = field(default="manual")
    # Stable dedup key for auto-fetched events; None for manual events
    external_id: str | None = field(default=None)
    # Human-readable event name for auto-fetched events (e.g. "Federal Funds Rate")
    title: str | None = field(default=None)
    # Client-only ("dry run") events are published to bot_mode_status but do not
    # suppress or cancel signals in this alert bot.
    client_only: bool = field(default=False)

    @property
    def display_label(self) -> str:
        """Human-readable label for alerts: 'USD — Federal Funds Rate' when a
        title is known (auto-fetched events), otherwise just the category."""
        cat = self.category.upper()
        if self.title:
            return f"{cat} — {self.title}"
        return cat

    @property
    def start_time(self) -> datetime:
        return self.news_time - timedelta(minutes=self.window_minutes)

    @property
    def end_time(self) -> datetime:
        if self.end_time_override is not None:
            return self.end_time_override
        if self.is_now_mode:
            # Far future sentinel — effectively never expires on its own
            return self.news_time + timedelta(days=365)
        return self.news_time + timedelta(minutes=self.window_minutes)

    def is_active(self, at: datetime | None = None) -> bool:
        """Return True if the event window covers the given time (default: now)."""
        now = at or datetime.now(pytz.utc)
        return self.start_time <= now <= self.end_time

    def is_expired(self, at: datetime | None = None) -> bool:
        # Now-mode with a timed end CAN expire when end_time_override passes
        if self.is_now_mode and self.end_time_override is None:
            return False  # Never auto-expires; must be manually removed
        now = at or datetime.now(pytz.utc)
        return now > self.end_time

    def instrument_affected(self, instrument: str) -> bool:
        """Return True if *instrument* should be cancelled during this event.

        Mirrors the execution bot's `instrument_under_news`: a currency code matches
        any instrument quoted in it, plus the dollar-denominated markets whose
        symbols carry no currency code at all. The two must agree — where they don't,
        one bot keeps tracking a signal whose position the other has already
        flattened at market.
        """
        cat = self.category.upper()
        instr = instrument.upper()

        if cat == "ALL":
            return True

        if cat in NAMED_CATEGORIES:
            return NAMED_CATEGORIES[cat](instr)

        if cat in FOREX_CURRENCIES:
            # A 24/7 book doesn't halt for a scheduled release, and every crypto
            # symbol carries a currency code it would otherwise match on (BTCUSDT
            # contains USD). Naming the asset directly (`!news btc`) still works —
            # that goes through NAMED_CATEGORIES above.
            if _is_crypto(instr):
                return False
            if cat in instr:
                return True
            # Metals, oil and US equities are dollar-denominated and move on US
            # releases, but only some of them say so in the symbol: XAUUSD does,
            # GCZ26_CFD, USOILSPOT and AMD.NAS do not.
            return cat == "USD" and (
                _is_metal(instr) or _is_oil(instr) or _is_us_stock(instr) or _is_us_index(instr)
            )

        return cat in instr

    def __str__(self) -> str:
        tags = []
        if self.source == "forexfactory":
            tags.append("auto")
        if self.client_only:
            tags.append("dry run")
        tag = f" [{' / '.join(tags)}]" if tags else ""
        name = f" {self.title}" if self.title else ""
        if self.is_now_mode:
            return (
                f"[#{self.event_id}]{tag} {self.category.upper()}{name} news @ "
                f"NOW (open-ended, manual off required)"
            )
        news_est = self.news_time.astimezone(EST)
        return (
            f"[#{self.event_id}]{tag} {self.category.upper()}{name} news @ "
            f"{news_est.strftime('%I:%M %p')} EST "
            f"(±{self.window_minutes} min)"
        )


def _load_optional_dt(value) -> datetime | None:
    """Parse an optional ISO datetime string from JSON, returning None if absent."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        return dt
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class NewsManager:
    """
    Singleton-style manager attached to the bot.

    Stores upcoming / active NewsEvent objects and provides fast lookup
    methods used by the streaming monitor.

    Events are persisted to data/news_events.json so they survive bot
    restarts.  The file is written synchronously on every mutation (events
    are infrequent so this is fine).
    """

    _CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "news_events.json"

    def __init__(self):
        self._events: list[NewsEvent] = []
        self._next_id: int = 1
        self._cleanup_task: asyncio.Task | None = None
        self._db = None  # Set by bot after DB initialisation via set_db()
        # Last news_mode value written to bot_mode_status. _news_mode_synced is
        # False until the first successful write, forcing one reconcile at startup
        # to correct any stale value a crash may have left behind.
        self._last_news_mode: str | None = None
        self._news_mode_synced: bool = False

    def set_db(self, db) -> None:
        """Attach the DatabaseManager so mode-status flags can be persisted."""
        self._db = db

    def _compute_news_mode_value(self) -> str | None:
        """Return the news_mode column value for the currently-active events.

        'ALL' if any active event covers all symbols; otherwise a comma-separated
        list of the active category labels (e.g. 'EUR, GOLD'); None when nothing
        is active.
        """
        labels: list[str] = []
        for event in self._events:
            if not event.is_active():
                continue
            label = event.category.upper()
            if label == "ALL":
                return "ALL"
            if label not in labels:
                labels.append(label)
        return ", ".join(labels) if labels else None

    async def reconcile_news_mode(self) -> None:
        """Sync bot_mode_status.news_mode to the active news categories.

        Writes only when the value changes (the first call after startup always
        writes, so a stale value left by a crash is corrected). Safe to call from
        any path (commands, the cleanup loop) — it is the single source of truth
        for the flag and does not rely on per-restart edge-detection state.

        A failed write leaves the tracking state untouched so the 30 s cleanup pass
        retries. Recording it as synced would skip every later write of the same
        value, and the EX bot would trade the whole window unguarded.
        """
        if self._db is None:
            return
        value = self._compute_news_mode_value()
        if self._news_mode_synced and value == self._last_news_mode:
            return
        try:
            await self._db.set_news_mode(value)
        except Exception as e:
            logger.error(f"Failed to reconcile news_mode in DB, will retry: {e}")
            return
        self._last_news_mode = value
        self._news_mode_synced = True

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        """Serialise non-expired manual events to disk.

        Auto-fetched (ForexFactory) events are deliberately not persisted — they
        are re-fetched from the feed on every startup, so saving them would only
        risk restoring stale windows.
        """
        now = datetime.now(pytz.utc)
        data = {
            "next_id": self._next_id,
            "events": [
                {
                    "event_id": e.event_id,
                    "category": e.category,
                    "news_time": e.news_time.isoformat(),
                    "window_minutes": e.window_minutes,
                    "created_by": e.created_by,
                    "created_at": e.created_at.isoformat(),
                    "is_now_mode": e.is_now_mode,
                    "display_tz": e.display_tz,
                    "end_time_override": e.end_time_override.isoformat()
                    if e.end_time_override
                    else None,
                    "client_only": e.client_only,
                }
                for e in self._events
                # Skip already-expired events and auto-fetched ones (re-fetched on boot)
                if not e.is_expired(now) and e.source == "manual"
            ],
        }
        try:
            self._CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            self._CONFIG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.debug(
                f"News events saved to {self._CONFIG_PATH} ({len(data['events'])} event(s))"
            )
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
            raw = json.loads(self._CONFIG_PATH.read_text(encoding="utf-8"))
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
                    end_time_override=_load_optional_dt(item.get("end_time_override")),
                    source=item.get("source", "manual"),
                    external_id=item.get("external_id"),
                    title=item.get("title"),
                    client_only=item.get("client_only", False),
                )

                if not event.is_expired(now):
                    self._events.append(event)
                    loaded += 1
                else:
                    logger.debug(f"Skipping expired event on load: {event}")

            except Exception as e:
                logger.warning(f"Skipping malformed news event entry: {item} — {e}")

        logger.debug(f"Loaded {loaded} news event(s) from {self._CONFIG_PATH}")
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
        display_tz: str = "EST",
        end_time_override: datetime | None = None,
        source: str = "manual",
        external_id: str | None = None,
        title: str | None = None,
        client_only: bool = False,
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
            end_time_override=end_time_override,
            source=source,
            external_id=external_id,
            title=title,
            client_only=client_only,
        )
        self._next_id += 1
        self._events.append(event)
        self._save()
        logger.debug(f"News event added: {event}")
        return event

    def remove_now_events(self) -> list[NewsEvent]:
        """Remove all 'now' mode events. Returns list of removed events."""
        removed = [e for e in self._events if e.is_now_mode]
        if removed:
            self._events = [e for e in self._events if not e.is_now_mode]
            self._save()
        return removed

    def is_news_active_for(self, instrument: str) -> NewsEvent | None:
        """
        Return the first active NewsEvent that affects *instrument*, or None.

        Includes client-only dry-run events because clients and context reporting
        should see every active news window.
        """
        now = datetime.now(pytz.utc)
        for event in self._events:
            if event.is_active(now) and event.instrument_affected(instrument):
                return event
        return None

    def is_alert_bot_news_active_for(self, instrument: str) -> NewsEvent | None:
        """Return an active event that should guard this alert bot's signals.

        Client-only (dry-run) events still reach bot_mode_status through
        reconcile_news_mode(), but are intentionally invisible to this lookup.
        """
        now = datetime.now(pytz.utc)
        for event in self._events:
            if (
                not event.client_only
                and event.is_active(now)
                and event.instrument_affected(instrument)
            ):
                return event
        return None

    def get_all_events(self) -> list[NewsEvent]:
        """Return all non-expired events (for display)."""
        now = datetime.now(pytz.utc)
        return [e for e in self._events if not e.is_expired(now)]

    def remove_event(self, event_id: int) -> NewsEvent | None:
        """Manually remove an event by ID, persist to disk. Returns the removed event if found."""
        target = next((e for e in self._events if e.event_id == event_id), None)
        if target:
            self._events = [e for e in self._events if e.event_id != event_id]
            self._save()
        return target

    def purge_expired(self):
        """Remove events that have fully passed and persist the updated list."""
        now = datetime.now(pytz.utc)
        before = len(self._events)
        self._events = [e for e in self._events if not e.is_expired(now)]
        removed = before - len(self._events)
        if removed:
            logger.debug(f"Purged {removed} expired news event(s)")
            self._save()

    def sync_forexfactory_events(self, events: list[NewsEvent]) -> tuple[int, int]:
        """Reconcile the set of auto-fetched events against a fresh feed pull.

        Adds any incoming event whose external_id is not already tracked, and
        removes auto events that have dropped out of the feed unless their window
        is currently active (so a live window is never yanked mid-event). Manual
        events are left untouched. Returns (added, removed) counts.
        """
        now = datetime.now(pytz.utc)
        existing_auto = {
            e.external_id: e for e in self._events if e.source == "forexfactory" and e.external_id
        }
        incoming_ids = {e.external_id for e in events if e.external_id}

        added = 0
        for event in events:
            if event.external_id in existing_auto:
                continue
            event.event_id = self._next_id
            self._next_id += 1
            self._events.append(event)
            added += 1

        before = len(self._events)
        self._events = [
            e
            for e in self._events
            if e.source != "forexfactory"
            or e.external_id in incoming_ids
            or e.is_active(now)
        ]
        removed = before - len(self._events)

        if added or removed:
            self._save()
            logger.info(f"ForexFactory sync: +{added} / -{removed} auto news event(s)")
        return added, removed

    def start_cleanup_task(self, alert_system=None):
        """
        Start a background task that:
        - Every 30 s: fires a 'news activated' alert for any window that just opened
        - Every 30 s: fires a 'news ended' alert + schedules deletion for expired windows
        - Every 5 min: purges expired events from memory and disk
        """

        async def _run():
            # Track which events have already had their activation / ended alert sent
            alerted_ids: set = set()
            ended_ids: set = set()

            while True:
                await asyncio.sleep(30)
                now = datetime.now(pytz.utc)

                if alert_system is not None:
                    for event in list(self._events):
                        # Fire activation alerts for windows that are now open
                        if event.event_id not in alerted_ids and event.is_active(now):
                            alerted_ids.add(event.event_id)
                            try:
                                await alert_system.send_news_activated_alert(event)
                            except Exception as e:
                                logger.error(f"Failed to send news activated alert: {e}")

                        # Fire ended alert for windows that just expired
                        if (
                            event.event_id not in ended_ids
                            and event.event_id in alerted_ids  # only if activation was sent
                            and event.is_expired(now)
                        ):
                            ended_ids.add(event.event_id)
                            try:
                                await alert_system.send_news_ended_alert(event)
                            except Exception as e:
                                logger.error(f"Failed to send news ended alert: {e}")

                # Reconcile bot_mode_status.news_mode against the actual set of
                # active windows. This self-heals every path (commands, expiry,
                # restarts) since it does not depend on the local edge sets above,
                # which reset to empty on each restart.
                await self.reconcile_news_mode()

                # Purge expired events (every ~5 min: 10 × 30 s)
                if not hasattr(_run, "_purge_counter"):
                    _run._purge_counter = 0
                _run._purge_counter += 1
                if _run._purge_counter >= 10:
                    _run._purge_counter = 0
                    self.purge_expired()
                    # Also clean up tracking sets for events that no longer exist
                    active_ids = {e.event_id for e in self._events}
                    alerted_ids &= active_ids
                    ended_ids &= active_ids

        # Cancel any previously running cleanup task so we don't leak coroutines
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()

        self._cleanup_task = asyncio.ensure_future(_run())

    def stop_cleanup_task(self):
        """Cancel the background cleanup task (called on bot shutdown)."""
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            self._cleanup_task = None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_news_command(args_str: str) -> tuple[str, datetime, int, str, bool]:
    """
    Parse the argument string from !news and return
    (category, news_time_utc, window_minutes, display_tz_label, auto_advanced).

    auto_advanced is True only when no explicit date was given and the time had
    already passed today, so it was rolled to tomorrow.

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
        raise ValueError("Usage: `!news <category> <time> [window] [tz:<timezone>] [date:<date>]`")

    category = tokens[0].upper()

    # Validate / warn about category
    is_valid = (
        category == "ALL"
        or category in FOREX_CURRENCIES
        or category in NAMED_CATEGORIES
        or category in {k.upper() for k in NAMED_CATEGORIES}
    )
    if not is_valid:
        logger.warning(f"Unknown news category: {category!r} — treating as exact match")

    # ---- Extract tags from remaining tokens ----
    remaining = tokens[1:]
    tz_label: str = "EST"
    tz_zone: pytz.BaseTzInfo = EST
    date_override: datetime | None = None
    window_minutes: int = 10
    time_str: str | None = None

    non_tag_tokens = []
    for tok in remaining:
        lower = tok.lower()
        if lower.startswith("tz:"):
            tz_str = tok[3:]
            tz_zone = resolve_timezone(tz_str)  # raises ValueError if bad
            tz_label = tz_str.upper()
        elif lower.startswith("date:"):
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
        except ValueError as e:
            raise ValueError(
                f"Window minutes must be an integer, got '{non_tag_tokens[1]}'"
            ) from e

    # Parse the time, using date_override if provided
    news_time_local = _parse_time(time_str, tz_zone, date_override)
    news_time_utc = news_time_local.astimezone(pytz.utc)

    # If no explicit date was given and the window has already fully passed,
    # auto-advance to the same time tomorrow.  This prevents silently scheduling
    # an event that is already expired (which would never appear in !newslist).
    auto_advanced = False
    if date_override is None:
        now_utc = datetime.now(pytz.utc)
        window_end_utc = news_time_utc + timedelta(minutes=window_minutes)
        if window_end_utc < now_utc:
            news_time_local = news_time_local + timedelta(days=1)
            news_time_utc = news_time_local.astimezone(pytz.utc)
            auto_advanced = True
            logger.info(
                f"News time {time_str} has already passed today — "
                f"auto-advanced to tomorrow: {news_time_utc.isoformat()}"
            )

    return category, news_time_utc, window_minutes, tz_label, auto_advanced


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

    if lower == "today":
        return now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if lower == "tomorrow":
        return (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        naive = datetime(year, month, day, 0, 0, 0)
        return tz_zone.localize(naive)

    # MM/DD or MM-DD
    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", date_str)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = now_local.year
        naive = datetime(year, month, day, 0, 0, 0)
        return tz_zone.localize(naive)

    raise ValueError(
        f"Cannot parse date '{date_str}'. Use YYYY-MM-DD, MM/DD, 'today', or 'tomorrow'."
    )


def _parse_time(
    time_str: str,
    tz_zone: pytz.BaseTzInfo,
    date_override: datetime | None = None,
) -> datetime:
    """
    Parse a time string into a timezone-aware datetime in tz_zone.
    If date_override is given, use that date; otherwise use today in tz_zone.
    Accepts: 12:30pm, 12:30PM, 8:30am, 14:00, 9:30, 930am, 2pm, etc.
    """
    base = date_override.astimezone(tz_zone) if date_override is not None else datetime.now(tz_zone)

    time_str = time_str.strip()

    formats = [
        "%I:%M%p",  # 12:30pm / 12:30PM
        "%I:%M %p",  # 12:30 pm / 12:30 PM
        "%H:%M",  # 14:00
        "%H%M",  # 1430
        "%I%p",  # 2pm / 2PM
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
            f"Cannot parse time '{time_str}'. Use formats like: 12:30pm, 14:00, 8:30am"
        )

    result = base.replace(
        hour=parsed.hour,
        minute=parsed.minute,
        second=0,
        microsecond=0,
    )
    return result
