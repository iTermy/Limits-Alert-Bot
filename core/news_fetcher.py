"""
ForexFactory news fetcher — auto-populates news windows from the weekly calendar feed.

Pulls the ForexFactory weekly economic calendar JSON, keeps only the configured
impact levels and currencies, and registers each event as an auto-managed
NewsEvent on the NewsManager. High-impact USD events optionally spawn a paired
GOLD window (ForexFactory files gold-moving releases — NFP, CPI, FOMC — under USD).

The feed is rate-limited (~2 downloads / 5 min), so we fetch on startup and then
every ``refresh_hours`` hours, which stays comfortably under the limit.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import aiohttp
import pytz

from core.news_manager import NewsEvent, NewsManager
from models.config import NewsAutofetchConfig
from utils.logger import get_logger

logger = get_logger("news_fetcher")

FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_REQUEST_TIMEOUT = 15  # seconds
_USER_AGENT = "TM-Bot/1.0 (news calendar fetcher)"


class NewsFetcher:
    """Background task that syncs ForexFactory calendar events into NewsManager."""

    def __init__(self, news_manager: NewsManager, config: NewsAutofetchConfig):
        self._news_manager = news_manager
        self._config = config
        self._task: asyncio.Task | None = None

    async def fetch_once(self) -> tuple[int, int]:
        """Pull the feed and sync auto events. Returns (added, removed) counts."""
        raw = await self._download()
        if raw is None:
            return 0, 0
        events = self._parse(raw)
        return self._news_manager.sync_forexfactory_events(events)

    async def _download(self) -> list | None:
        """Fetch and JSON-decode the feed. Returns None on any failure."""
        try:
            timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)
            headers = {"User-Agent": _USER_AGENT}
            async with (
                aiohttp.ClientSession(timeout=timeout, headers=headers) as session,
                session.get(FEED_URL) as resp,
            ):
                    if resp.status != 200:
                        logger.warning(f"ForexFactory feed returned HTTP {resp.status}")
                        return None
                    # Feed is served as text/plain; parse JSON manually
                    return await resp.json(content_type=None)
        except Exception as e:
            logger.warning(f"Failed to fetch ForexFactory feed: {e}")
            return None

    def _parse(self, raw: list) -> list[NewsEvent]:
        """Filter feed entries and merge releases that share a currency and time.

        ForexFactory often lists several releases at the same moment (e.g. the
        three FOMC items at 2:00 PM, or BOJ rate + statement). They cover the same
        window for pausing purposes, so they collapse into one event whose title
        joins the individual names. USD events also flag gold (FOMC/NFP/CPI move it).
        """
        impacts = {lvl.lower() for lvl in self._config.impact_levels}
        currencies = {c.upper() for c in self._config.currencies}
        window = self._config.window_minutes

        # Group by (currency, exact time) → list of titles, preserving feed order
        groups: dict[tuple[str, datetime], list[str]] = {}
        for item in raw:
            country = str(item.get("country", "")).upper()
            impact = str(item.get("impact", "")).lower()
            date_str = item.get("date")
            title = item.get("title") or ""

            if impact not in impacts or country not in currencies or not date_str:
                continue

            try:
                news_time = datetime.fromisoformat(date_str).astimezone(pytz.utc)
            except (ValueError, TypeError):
                logger.debug(f"Skipping event with unparseable date: {date_str!r}")
                continue

            titles = groups.setdefault((country, news_time), [])
            if title and title not in titles:
                titles.append(title)

        events: list[NewsEvent] = []
        for (country, news_time), titles in groups.items():
            events.append(
                NewsEvent(
                    category=country,
                    news_time=news_time,
                    window_minutes=window,
                    created_by="forexfactory",
                    display_tz="EST",
                    source="forexfactory",
                    external_id=f"{country}|{news_time.isoformat()}",
                    title=" / ".join(titles),
                )
            )

        return events

    def start(self) -> None:
        """Start the periodic fetch loop (fetches immediately, then every refresh_hours)."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        interval = max(1, self._config.refresh_hours) * 3600
        while True:
            try:
                await self.fetch_once()
            except Exception as e:
                logger.error(f"News fetch loop error: {e}", exc_info=True)
            await asyncio.sleep(interval)

    async def refresh_now(self) -> tuple[int, int]:
        """One-shot fetch for the !news refresh command."""
        return await self.fetch_once()

    def stop(self) -> None:
        """Cancel the background fetch loop (called on bot shutdown)."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            self._task = None
