"""
Signal-specific database operations main module
"""
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
from core.parser import ParsedSignal
from utils.logger import get_logger


logger = get_logger("signal_db")


class SignalStatus(Enum):
    """Signal status enumeration"""
    ACTIVE = 'active'
    HIT = 'hit'
    PROFIT = 'profit'
    BREAKEVEN = 'breakeven'
    STOP_LOSS = 'stop_loss'
    CANCELLED = 'cancelled'

    @classmethod
    def is_final(cls, status: str) -> bool:
        """Check if status is final"""
        return status in [cls.PROFIT.value, cls.BREAKEVEN.value,
                         cls.STOP_LOSS.value, cls.CANCELLED.value]

    @classmethod
    def is_trackable(cls, status: str) -> bool:
        """Check if status requires price tracking"""
        return status in [cls.ACTIVE.value, cls.HIT.value]


class SignalDatabase:
    """Handles signal-specific database operations with enhanced lifecycle management"""

    def __init__(self, db_manager):
        """
        Initialize signal database handler

        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager

        # Initialize sub-modules
        from .lifecycle import LifecycleManager
        from .crud import CrudOperations
        from .analytics import AnalyticsManager

        self._lifecycle = LifecycleManager(db_manager)
        self._crud = CrudOperations(db_manager)
        self._analytics = AnalyticsManager(db_manager)

    # ==================== CRUD Operations ====================

    async def save_signal(self, parsed_signal: ParsedSignal, message_id: str,
                         channel_id: str) -> Tuple[bool, Optional[int]]:
        """
        Save a parsed signal to the database

        Args:
            parsed_signal: Parsed signal object
            message_id: Discord message ID
            channel_id: Discord channel ID

        Returns:
            Tuple of (success, signal_id)
        """
        return await self._crud.save_signal(parsed_signal, message_id, channel_id)

    async def get_signal_by_message_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get signal by Discord message ID

        Args:
            message_id: Discord message ID

        Returns:
            Signal data or None
        """
        return await self._crud.get_signal_by_message_id(message_id)

    async def get_signal_with_limits(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """
        Get signal with all its limits (including hit ones)

        Args:
            signal_id: Signal ID

        Returns:
            Signal data with limits
        """
        return await self._crud.get_signal_with_limits(signal_id)

    async def update_signal_from_edit(self, message_id: str, parsed_signal: ParsedSignal) -> bool:
        """
        Update an existing signal from an edited message

        Args:
            message_id: Discord message ID
            parsed_signal: Newly parsed signal

        Returns:
            Success status
        """
        return await self._crud.update_signal_from_edit(message_id, parsed_signal)

    async def get_active_signals_detailed(self, instrument: str = None) -> List[Dict[str, Any]]:
        """
        Get detailed active signals (ACTIVE or HIT status) with limits

        Args:
            instrument: Optional filter by instrument

        Returns:
            List of signals with detailed information
        """
        return await self._crud.get_active_signals_detailed(instrument)

    async def get_signals_for_tracking(self) -> List[Dict[str, Any]]:
        """
        Get all signals that need price tracking (wrapper for DB method)

        Returns:
            List of signals with their pending limits for price tracking
        """
        return await self.db.get_active_signals_for_tracking()

    # ==================== Lifecycle Operations ====================

    async def cancel_signal_by_message(self, message_id: str) -> bool:
        """
        Cancel a signal when its message is deleted or cancelled by user

        Args:
            message_id: Discord message ID

        Returns:
            Success status
        """
        return await self._lifecycle.cancel_signal_by_message(message_id, self)

    async def reactivate_cancelled_signal(self, signal_id: int, parsed_signal: ParsedSignal) -> bool:
        """
        Reactivate a cancelled signal (e.g., when message is undeleted or edited)

        Args:
            signal_id: Signal ID to reactivate
            parsed_signal: New parsed signal data

        Returns:
            Success status
        """
        return await self._lifecycle.reactivate_cancelled_signal(signal_id, parsed_signal, self.db)

    async def manually_set_signal_status(self, signal_id: int, new_status: str,
                                        reason: str = None) -> bool:
        """
        Manually set a signal's status (for admin override)
        Bypasses validation for manual overrides

        Args:
            signal_id: Signal ID
            new_status: New status to set
            reason: Optional reason for manual change

        Returns:
            Success status
        """
        return await self._lifecycle.manually_set_signal_status(signal_id, new_status, reason, self.db)

    async def process_limit_hit(self, limit_id: int, actual_price: float = None) -> Dict[str, Any]:
        """
        Process a limit hit event

        Args:
            limit_id: Limit ID that was hit
            actual_price: Actual price at hit (for spread tracking)

        Returns:
            Dict with signal info and status changes
        """
        return await self._lifecycle.process_limit_hit(limit_id, actual_price, self)

    async def check_and_update_stop_loss(self, signal_id: int, current_price: float) -> bool:
        """
        Check if stop loss is hit and update status

        Args:
            signal_id: Signal ID
            current_price: Current market price

        Returns:
            True if stop loss was hit
        """
        return await self._lifecycle.check_and_update_stop_loss(signal_id, current_price, self)

    async def manually_set_signal_expiry(self, signal_id: int, expiry_type: str,
                                        custom_datetime: str = None) -> bool:
        """
        Manually set a signal's expiry type and recalculate expiry time

        Args:
            signal_id: Signal ID
            expiry_type: New expiry type (day_end, week_end, month_end, no_expiry, custom)
            custom_datetime: Custom datetime string in ISO format (for custom type)

        Returns:
            Success status
        """
        return await self._lifecycle.manually_set_signal_expiry(
            signal_id, expiry_type, custom_datetime, self.db
        )

    async def expire_old_signals(self) -> int:
        """
        Check and expire signals past their expiry time

        Returns:
            Number of signals expired
        """
        return await self._lifecycle.expire_old_signals(self.db)

    # ==================== Analytics Operations ====================

    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive database statistics

        Returns:
            Statistics dictionary
        """
        return await self._analytics.get_statistics(self.db)

    # ==================== Helper Methods ====================

    def _calculate_expiry(self, expiry_type: str) -> Optional[str]:
        """
        Calculate expiry timestamp based on type

        Args:
            expiry_type: Type of expiry

        Returns:
            ISO format timestamp or None
        """
        from .utils import calculate_expiry
        return calculate_expiry(expiry_type)

    def _get_status_emoji(self, status: str) -> str:
        """Get emoji for status display"""
        from .utils import get_status_emoji
        return get_status_emoji(status)

    async def get_trading_period_range(self, period: str = 'week') -> Dict[str, Any]:
        """
        Get the date range for the current trading period

        Args:
            period: 'week' or 'month'

        Returns:
            Dictionary with start/end dates and display strings
        """
        return await self._analytics.get_trading_period_range(period)

    async def get_period_signals_with_results(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get all signals with final results within a date range

        Args:
            start_date: ISO format start date
            end_date: ISO format end date

        Returns:
            List of signals with their results
        """
        return await self._analytics.get_period_signals_with_results(start_date, end_date)

    async def get_week_performance_summary(self) -> Dict[str, Any]:
        """
        Get current week's performance summary

        Returns:
            Dictionary with week's performance metrics
        """
        return await self._analytics.get_week_performance_summary()

    async def get_month_performance_summary(self) -> Dict[str, Any]:
        """
        Get current month's performance summary

        Returns:
            Dictionary with month's performance metrics
        """
        return await self._analytics.get_month_performance_summary()