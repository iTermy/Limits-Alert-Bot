"""Typed service container for bot subsystems.

Replaces reach-through coupling (``bot.monitor.X.Y``) with flat access
(``services.X``).  Populated during bot startup; injected into cogs and
handlers via constructor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from core.news_fetcher import NewsFetcher
    from database import DatabaseManager
    from database.signal_ops import SignalDatabase
    from price_feeds.alert_config import AlertDistanceConfig
    from price_feeds.alert_system import AlertSystem
    from price_feeds.nm_config import NMConfig
    from price_feeds.nm_monitor import NearMissMonitor
    from price_feeds.price_stream_manager import PriceStreamManager
    from price_feeds.streaming_monitor import StreamingPriceMonitor
    from price_feeds.tp_config import TPConfig
    from price_feeds.tp_monitor import AutoTPMonitor
    from price_feeds.trailing_monitor import TrailingStopMonitor
    from price_feeds.excursion_monitor import ExcursionMonitor
    from price_feeds.vol_guard import VolatilityGuard
    from price_feeds.risky_window import RiskyWindowAnnouncer


class ServiceRegistry:
    """Flat, typed container holding references to every bot subsystem."""

    def __init__(self) -> None:
        self.monitor: Optional[StreamingPriceMonitor] = None
        self.alert_system: Optional[AlertSystem] = None
        self.stream_manager: Optional[PriceStreamManager] = None
        self.tp_config: Optional[TPConfig] = None
        self.tp_monitor: Optional[AutoTPMonitor] = None
        self.nm_config: Optional[NMConfig] = None
        self.nm_monitor: Optional[NearMissMonitor] = None
        self.trailing_monitor: Optional[TrailingStopMonitor] = None
        self.excursion_monitor: Optional[ExcursionMonitor] = None
        self.alert_config: Optional[AlertDistanceConfig] = None
        self.signal_db: Optional[SignalDatabase] = None
        self.db: Optional[DatabaseManager] = None
        self.news_fetcher: Optional[NewsFetcher] = None
        self.vol_guard: Optional[VolatilityGuard] = None
        self.risky_window_announcer: Optional[RiskyWindowAnnouncer] = None
