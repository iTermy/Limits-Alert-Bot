"""
Enhanced DatabaseManager that integrates all modules and maintains backward compatibility
"""
from typing import Optional, List, Dict, Any
from .connection import DatabaseManager as BaseConnectionManager
from .base_operations import BaseOperations
from .schema import initialize_database
from .models import SignalStatus, LimitStatus, StatusTransitions
from utils.logger import get_logger


def _parse_dt(value):
    """Convert ISO string or datetime to timezone-aware datetime, or None."""
    if value is None:
        return None
    if hasattr(value, 'tzinfo'):
        return value if value.tzinfo else __import__('pytz').UTC.localize(value)
    from datetime import datetime
    s = str(value).replace('Z', '+00:00')
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return __import__('pytz').UTC.localize(dt)
    return dt

logger = get_logger("database")


class DatabaseManager(BaseConnectionManager):
    """
    Enhanced database manager with all operations integrated
    Maintains backward compatibility with the original interface
    """

    def __init__(self, db_url: str = None):
        """
        Initialize enhanced database manager

        Args:
            db_url: PostgreSQL connection string. Falls back to SUPABASE_DB_URL env var.
        """
        # Initialize base connection manager
        super().__init__(db_url)

        # Initialize base operations handler
        self._ops = BaseOperations(self)

        # Export status constants for backward compatibility
        self.STATUS_ACTIVE = SignalStatus.ACTIVE
        self.STATUS_HIT = SignalStatus.HIT
        self.STATUS_PROFIT = SignalStatus.PROFIT
        self.STATUS_BREAKEVEN = SignalStatus.BREAKEVEN
        self.STATUS_STOP_LOSS = SignalStatus.STOP_LOSS
        self.STATUS_CANCELLED = SignalStatus.CANCELLED
        self.FINAL_STATUSES = SignalStatus.FINAL_STATUSES

        self.LIMIT_STATUS_PENDING = LimitStatus.PENDING
        self.LIMIT_STATUS_HIT = LimitStatus.HIT
        self.LIMIT_STATUS_CANCELLED = LimitStatus.CANCELLED

    async def initialize(self):
        """Initialize database and create enhanced tables"""
        await initialize_database(self)
        logger.info("Database manager initialized successfully")

    # Delegate all operations to the operations handler
    async def insert_signal(self, message_id: str, channel_id: str, instrument: str,
                           direction: str, stop_loss: float, expiry_type: str = None,
                           expiry_time: str = None, total_limits: int = 0,
                           scalp: bool = False) -> int:
        """
        Insert a new signal with enhanced tracking

        Args:
            message_id: Discord message ID
            channel_id: Discord channel ID
            instrument: Trading instrument (e.g., GBPUSD)
            direction: Trade direction (long/short)
            stop_loss: Stop loss price
            expiry_type: Expiry type (day_end, week_end, etc.)
            expiry_time: Calculated expiry timestamp
            total_limits: Total number of limit orders
            scalp: Whether this is a scalp signal

        Returns:
            Signal ID
        """
        return await self._ops.insert_signal(
            message_id, channel_id, instrument, direction,
            stop_loss, expiry_type, expiry_time, total_limits, scalp
        )

    async def insert_limits(self, signal_id: int, price_levels: List[float]):
        """
        Insert limits for a signal with sequence numbers

        Args:
            signal_id: Parent signal ID
            price_levels: List of limit prices (ordered)
        """
        return await self._ops.insert_limits(signal_id, price_levels)

    async def update_signal_status(self, signal_id: int, new_status: str,
                                  change_type: str = 'automatic', reason: str = None) -> bool:
        """
        Update signal status with proper lifecycle management

        Args:
            signal_id: Signal ID
            new_status: New status
            change_type: 'automatic' or 'manual'
            reason: Optional reason for change

        Returns:
            Success status
        """
        return await self._ops.update_signal_status(
            signal_id, new_status, change_type, reason
        )

    async def mark_limit_hit(self, limit_id: int, hit_price: float = None) -> Dict[str, Any]:
        """
        Mark a limit as hit and update signal status if needed

        Args:
            limit_id: Limit ID
            hit_price: Actual price when hit (for spread tracking)

        Returns:
            Dict with signal_id and whether signal status changed
        """
        return await self._ops.mark_limit_hit(limit_id, hit_price)

    async def check_stop_loss_hit(self, signal_id: int, current_price: float) -> bool:
        """
        Check if stop loss has been hit and update status if needed

        Args:
            signal_id: Signal ID
            current_price: Current market price

        Returns:
            True if stop loss was hit
        """
        return await self._ops.check_stop_loss_hit(signal_id, current_price)

    async def get_active_signals_for_tracking(self) -> List[Dict[str, Any]]:
        """
        Get all signals that need price tracking (ACTIVE or HIT status)

        Returns:
            List of signals with their pending limits
        """
        return await self._ops.get_active_signals_for_tracking()

    async def mark_approaching_alert_sent(self, limit_id: int) -> bool:
        """
        Mark that an approaching alert has been sent for a limit

        Args:
            limit_id: Limit ID

        Returns:
            Success status
        """
        return await self._ops.mark_approaching_alert_sent(limit_id)

    async def mark_hit_alert_sent(self, limit_id: int) -> bool:
        """
        Mark that a hit alert has been sent for a limit

        Args:
            limit_id: Limit ID

        Returns:
            Success status
        """
        return await self._ops.mark_hit_alert_sent(limit_id)

    async def get_hit_limits_for_signal(self, signal_id: int) -> List[Dict[str, Any]]:
        """Return all hit limits for a signal with hit_price for P&L calculations."""
        return await self._ops.get_hit_limits_for_signal(signal_id)

    async def get_performance_stats(self, start_date: str = None, end_date: str = None,
                                   instrument: str = None) -> Dict[str, Any]:
        """
        Get performance statistics for closed signals

        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            instrument: Optional instrument filter

        Returns:
            Performance statistics
        """
        return await self._ops.get_performance_stats(start_date, end_date, instrument)

    async def update_signal_expiry(self, signal_id: int, expiry_type: str) -> bool:
        """
        Update a signal's expiry type and recalculate expiry time

        Args:
            signal_id: Signal ID
            expiry_type: New expiry type (day_end, week_end, month_end, no_expiry)

        Returns:
            Success status
        """
        # Validate expiry type
        valid_types = ['day_end', 'week_end', 'month_end', 'no_expiry']
        if expiry_type not in valid_types:
            logger.error(f"Invalid expiry type: {expiry_type}")
            return False

        # Get current signal
        signal = await self.fetch_one(
            "SELECT * FROM signals WHERE id = $1",
            (signal_id,)
        )

        if not signal:
            logger.error(f"Signal {signal_id} not found")
            return False

        # Check if signal is in final status
        if SignalStatus.is_final(signal['status']):
            logger.warning(f"Cannot modify expiry for signal {signal_id} in final status {signal['status']}")
            return False

        # Calculate new expiry time
        from database.signal_operations.utils import calculate_expiry
        new_expiry_time = calculate_expiry(expiry_type)

        # Update signal
        try:
            from datetime import datetime
            import pytz

            now = datetime.now(pytz.UTC)

            query = """
                UPDATE signals 
                SET expiry_type = $1, expiry_time = $2, updated_at = $3
                WHERE id = $4
            """

            rows = await self.execute(query, (expiry_type, _parse_dt(new_expiry_time), now, signal_id))

            if rows > 0:
                logger.info(f"Updated expiry for signal {signal_id} to {expiry_type}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error updating signal expiry: {e}", exc_info=True)
            return False

    def _is_valid_transition(self, old_status: str, new_status: str) -> bool:
        """
        Check if a status transition is valid

        Args:
            old_status: Current status
            new_status: Desired new status

        Returns:
            Whether transition is valid
        """
        return StatusTransitions.is_valid_transition(old_status, new_status)