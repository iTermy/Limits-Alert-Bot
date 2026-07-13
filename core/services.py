"""Typed service container for bot subsystems.

Replaces reach-through coupling (``bot.monitor.X.Y``) with flat access
(``services.X``).  Populated during bot startup; injected into cogs and
handlers via constructor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.news_fetcher import NewsFetcher
    from database import DatabaseManager
    from database.signal_ops import SignalDatabase
    from price_feeds.alert_config import AlertDistanceConfig
    from price_feeds.alert_system import AlertSystem
    from price_feeds.excursion_monitor import ExcursionMonitor
    from price_feeds.nm_config import NMConfig
    from price_feeds.nm_monitor import NearMissMonitor
    from price_feeds.price_stream_manager import PriceStreamManager
    from price_feeds.risky_window import RiskyWindowAnnouncer
    from price_feeds.streaming_monitor import StreamingPriceMonitor
    from price_feeds.tp_config import TPConfig
    from price_feeds.tp_monitor import AutoTPMonitor
    from price_feeds.trailing_monitor import TrailingStopMonitor
    from price_feeds.vol_guard import VolatilityGuard


class ServiceRegistry:
    """Flat, typed container holding references to every bot subsystem."""

    def __init__(self) -> None:
        self.monitor: StreamingPriceMonitor | None = None
        self.alert_system: AlertSystem | None = None
        self.stream_manager: PriceStreamManager | None = None
        self.tp_config: TPConfig | None = None
        self.tp_monitor: AutoTPMonitor | None = None
        self.nm_config: NMConfig | None = None
        self.nm_monitor: NearMissMonitor | None = None
        self.trailing_monitor: TrailingStopMonitor | None = None
        self.excursion_monitor: ExcursionMonitor | None = None
        self.alert_config: AlertDistanceConfig | None = None
        self.signal_db: SignalDatabase | None = None
        self.db: DatabaseManager | None = None
        self.news_fetcher: NewsFetcher | None = None
        self.vol_guard: VolatilityGuard | None = None
        self.risky_window_announcer: RiskyWindowAnnouncer | None = None
