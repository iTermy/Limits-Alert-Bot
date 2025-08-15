"""
Base class for all price feed implementations
Defines the interface that all feeds must implement
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseFeed(ABC):
    """
    Abstract base class for price feeds

    All price feeds (ICMarkets, OANDA, Binance) must implement this interface
    """

    def __init__(self, feed_name: str):
        """
        Initialize base feed

        Args:
            feed_name: Name of the feed for logging/identification
        """
        self.feed_name = feed_name
        self.connected = False
        self.last_successful_fetch = None
        self.consecutive_failures = 0
        self.max_failures_before_reconnect = 3

        # Health metrics
        self.connection_attempts = 0
        self.successful_fetches = 0
        self.failed_fetches = 0

        # Batch operation metrics
        self.total_batch_requests = 0
        self.total_symbols_requested = 0
        self.total_symbols_fetched = 0

        logger.info(f"{feed_name} feed initialized")

    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the price feed

        Returns:
            True if connection successful
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """Close connection to the price feed"""
        pass

    @abstractmethod
    async def ensure_connected(self) -> bool:
        """
        Ensure feed is connected, reconnect if necessary

        Returns:
            True if connected or reconnected successfully
        """
        pass

    @abstractmethod
    async def get_price(self, symbol: str) -> Optional[Dict]:
        """
        Get current bid/ask price for a single symbol

        Args:
            symbol: Trading symbol

        Returns:
            Dict with bid, ask, timestamp, or None if failed
        """
        pass

    @abstractmethod
    async def get_batch_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Get prices for multiple symbols efficiently

        Args:
            symbols: List of trading symbols

        Returns:
            Dict mapping symbol to price data
        """
        pass

    def get_health_status(self) -> Dict:
        """
        Get feed health metrics

        Returns:
            Dict with health information
        """
        # Calculate success rates
        if self.successful_fetches + self.failed_fetches > 0:
            fetch_success_rate = (self.successful_fetches /
                                  (self.successful_fetches + self.failed_fetches)) * 100
        else:
            fetch_success_rate = 0

        if self.total_symbols_requested > 0:
            batch_success_rate = (self.total_symbols_fetched /
                                  self.total_symbols_requested) * 100
        else:
            batch_success_rate = 100

        # Calculate average symbols per batch
        avg_symbols_per_batch = (self.total_symbols_requested /
                                 self.total_batch_requests
                                 if self.total_batch_requests > 0 else 0)

        return {
            'feed_name': self.feed_name,
            'connected': self.connected,
            'last_successful_fetch': self.last_successful_fetch,
            'consecutive_failures': self.consecutive_failures,
            'connection_attempts': self.connection_attempts,
            'successful_fetches': self.successful_fetches,
            'failed_fetches': self.failed_fetches,
            'fetch_success_rate': f"{fetch_success_rate:.1f}%",
            'total_batch_requests': self.total_batch_requests,
            'batch_success_rate': f"{batch_success_rate:.1f}%",
            'avg_symbols_per_batch': f"{avg_symbols_per_batch:.1f}"
        }

    async def test_connection(self) -> Tuple[bool, str]:
        """
        Test feed connection and basic functionality

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Test connection
            if not await self.ensure_connected():
                return False, f"Failed to connect to {self.feed_name}"

            # Test fetching a common symbol
            test_symbol = self._get_test_symbol()
            price = await self.get_price(test_symbol)

            if price:
                return True, (f"{self.feed_name} connection successful! "
                              f"{test_symbol}: Bid={price['bid']}, Ask={price['ask']}")
            else:
                return False, f"Connected but couldn't fetch {test_symbol} price"

        except Exception as e:
            return False, f"{self.feed_name} connection test failed: {str(e)}"

    def _get_test_symbol(self) -> str:
        """Get appropriate test symbol for this feed"""
        # Override in subclasses if needed
        return 'EURUSD'

    def _update_metrics(self, success: bool, batch_size: int = 1):
        """
        Update internal metrics

        Args:
            success: Whether the operation was successful
            batch_size: Number of symbols in batch (1 for single)
        """
        if success:
            self.successful_fetches += 1
            self.consecutive_failures = 0
            self.last_successful_fetch = datetime.now()
        else:
            self.failed_fetches += 1
            self.consecutive_failures += 1

    def should_reconnect(self) -> bool:
        """Check if reconnection is needed based on failure count"""
        return self.consecutive_failures >= self.max_failures_before_reconnect