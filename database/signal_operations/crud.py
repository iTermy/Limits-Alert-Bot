"""
CRUD operations for signals
"""
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import pytz
from database.models import SignalStatus
from core.parser import ParsedSignal
from utils.logger import get_logger


def _to_dt(value) -> datetime:
    """Return a timezone-aware datetime from either a datetime object or an ISO string."""
    if isinstance(value, datetime):
        return value if value.tzinfo else pytz.UTC.localize(value)
    s = str(value)
    if '+' in s or s.endswith('Z'):
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    return pytz.UTC.localize(datetime.fromisoformat(s))


logger = get_logger("signal_db.crud")


class CrudOperations:
    """Handles CRUD operations for signals"""

    def __init__(self, db_manager):
        """
        Initialize CRUD operations handler

        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager

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
        try:
            # Check if signal already exists
            existing = await self.get_signal_by_message_id(message_id)
            if existing:
                # Check if it was cancelled and can be reactivated
                if existing['status'] == SignalStatus.CANCELLED:
                    logger.info(f"Reactivating cancelled signal for message {message_id}")
                    # Import here to avoid circular dependency
                    from .lifecycle import LifecycleManager
                    lifecycle = LifecycleManager(self.db)
                    await lifecycle.reactivate_cancelled_signal(existing['id'], parsed_signal, self.db)
                    return True, existing['id']
                else:
                    logger.warning(f"Signal already exists for message {message_id}")
                    return False, existing['id']

            # Calculate expiry time
            from .utils import calculate_expiry
            expiry_time = calculate_expiry(parsed_signal.expiry_type)

            # Insert signal with total limits count
            signal_id = await self.db.insert_signal(
                message_id=message_id,
                channel_id=channel_id,
                instrument=parsed_signal.instrument,
                direction=parsed_signal.direction,
                stop_loss=parsed_signal.stop_loss,
                expiry_type=parsed_signal.expiry_type,
                expiry_time=expiry_time,
                total_limits=len(parsed_signal.limits) if parsed_signal.limits else 0
            )

            # Insert limits with sequence numbers
            if signal_id and parsed_signal.limits:
                await self.db.insert_limits(signal_id, parsed_signal.limits)

            logger.info(f"Saved signal {signal_id}: {parsed_signal.instrument} "
                        f"{parsed_signal.direction} with {len(parsed_signal.limits)} limits")
            return True, signal_id

        except Exception as e:
            logger.error(f"Error saving signal: {e}", exc_info=True)
            return False, None

    async def get_signal_by_message_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get signal by Discord message ID

        Args:
            message_id: Discord message ID

        Returns:
            Signal data or None
        """
        query = "SELECT * FROM signals WHERE message_id = $1"
        return await self.db.fetch_one(query, (message_id,))

    async def get_signal_with_limits(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """
        Get signal with all its limits (including hit ones)

        Args:
            signal_id: Signal ID

        Returns:
            Signal data with limits
        """
        # Get signal
        signal_query = "SELECT * FROM signals WHERE id = $1"
        signal = await self.db.fetch_one(signal_query, (signal_id,))

        if not signal:
            return None

        # Get all limits (pending and hit)
        limits_query = """
            SELECT * FROM limits 
            WHERE signal_id = $1 
            ORDER BY sequence_number
        """
        limits = await self.db.fetch_all(limits_query, (signal_id,))

        signal['limits'] = limits
        signal['pending_limits'] = [l for l in limits if l['status'] == 'pending']
        signal['hit_limits'] = [l for l in limits if l['status'] == 'hit']

        return signal

    async def update_signal_from_edit(self, message_id: str, parsed_signal: ParsedSignal) -> bool:
        """
        Update an existing signal from an edited message

        Args:
            message_id: Discord message ID
            parsed_signal: Newly parsed signal

        Returns:
            Success status
        """
        try:
            # Get existing signal
            existing = await self.get_signal_by_message_id(message_id)
            if not existing:
                logger.warning(f"No signal found for message {message_id}")
                return False

            signal_id = existing['id']

            # Only allow updates if signal is not in final status
            if SignalStatus.is_final(existing['status']):
                logger.warning(f"Cannot update signal {signal_id} in final status {existing['status']}")
                return False

            # Update signal basic info
            update_query = """
                UPDATE signals 
                SET instrument = $1, direction = $2, stop_loss = $3, 
                    expiry_type = $4, total_limits = $5, updated_at = CURRENT_TIMESTAMP
                WHERE id = $6
            """
            await self.db.execute(
                update_query,
                (parsed_signal.instrument, parsed_signal.direction,
                 parsed_signal.stop_loss, parsed_signal.expiry_type,
                 len(parsed_signal.limits), signal_id)
            )

            # Get existing limits that were hit
            hit_limits_query = """
                SELECT price_level FROM limits 
                WHERE signal_id = $1 AND status = 'hit'
                ORDER BY sequence_number
            """
            hit_limits = await self.db.fetch_all(hit_limits_query, (signal_id,))
            hit_prices = [l['price_level'] for l in hit_limits]

            # Delete old limits
            delete_limits = "DELETE FROM limits WHERE signal_id = $1"
            await self.db.execute(delete_limits, (signal_id,))

            # Insert new limits, preserving hit status for matching prices
            for idx, level in enumerate(parsed_signal.limits):
                if level in hit_prices:
                    # Re-insert as hit
                    await self.db.execute("""
                        INSERT INTO limits (signal_id, price_level, sequence_number, status, hit_time)
                        VALUES ($1, $2, $3, 'hit', CURRENT_TIMESTAMP)
                    """, signal_id, level, idx + 1)
                else:
                    # Insert as pending
                    await self.db.execute("""
                        INSERT INTO limits (signal_id, price_level, sequence_number, status)
                        VALUES ($1, $2, $3, 'pending')
                    """, signal_id, level, idx + 1)
            logger.info(f"Updated signal {signal_id} from edited message")
            return True

        except Exception as e:
            logger.error(f"Error updating signal from edit: {e}", exc_info=True)
            return False

    async def get_active_signals_detailed(self, instrument: str = None) -> List[Dict[str, Any]]:
        """
        Get detailed active signals (ACTIVE or HIT status) with limits

        Args:
            instrument: Optional filter by instrument

        Returns:
            List of signals with detailed information
        """
        # Build query
        base_query = """
            SELECT 
                s.*,
                COUNT(DISTINCT l.id) as total_limit_count,
                COUNT(DISTINCT CASE WHEN l.status = 'hit' THEN l.id END) as hit_limit_count,
                STRING_AGG(
                    (CASE WHEN l.status = 'pending' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as pending_limits_str,
                STRING_AGG(
                    (CASE WHEN l.status = 'hit' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as hit_limits_str
            FROM signals s
            LEFT JOIN limits l ON s.id = l.signal_id
            WHERE s.status IN ($1, $2)
        """

        params = [SignalStatus.ACTIVE, SignalStatus.HIT]

        if instrument:
            base_query += " AND s.instrument = $3"
            params.append(instrument)

        base_query += " GROUP BY s.id ORDER BY s.created_at DESC"

        signals = await self.db.fetch_all(base_query, tuple(params))

        # Enhance with additional data
        for signal in signals:
            # Parse limit strings into lists
            signal['pending_limits'] = []
            signal['hit_limits'] = []

            if signal.get('pending_limits_str'):
                signal['pending_limits'] = [float(p) for p in signal['pending_limits_str'].split(',')]

            if signal.get('hit_limits_str'):
                signal['hit_limits'] = [float(p) for p in signal['hit_limits_str'].split(',')]

            # Remove temporary string fields
            signal.pop('pending_limits_str', None)
            signal.pop('hit_limits_str', None)

            # Add time remaining for expiry
            if signal.get('expiry_time'):
                expiry = _to_dt(signal['expiry_time'])
                now = datetime.now(pytz.UTC)
                if expiry.tzinfo is None:
                    expiry = pytz.UTC.localize(expiry)

                remaining = expiry - now
                if remaining.total_seconds() > 0:
                    hours = int(remaining.total_seconds() // 3600)
                    minutes = int((remaining.total_seconds() % 3600) // 60)
                    signal['time_remaining'] = f"{hours}h {minutes}m"
                else:
                    signal['time_remaining'] = "Expired"
            else:
                signal['time_remaining'] = "No expiry"

            # Add status display info
            from .utils import get_status_emoji
            signal['status_emoji'] = get_status_emoji(signal['status'])
            signal['progress'] = f"{signal['hit_limit_count']}/{signal['total_limit_count']} limits hit"

        return signals


"""
Additional method for crud.py to support sorting at database level
Add this method to your existing crud.py file
"""


async def get_active_signals_detailed_sorted(self, instrument: str = None,
                                             sort_by: str = 'recent',
                                             limit: int = None) -> List[Dict[str, Any]]:
    """
    Get detailed active signals with sorting options

    Args:
        instrument: Optional filter by instrument
        sort_by: Sort method ('recent', 'oldest', 'progress')
        limit: Optional limit on number of results

    Returns:
        List of signals with detailed information
    """
    # Build query
    base_query = """
        SELECT 
            s.*,
            COUNT(DISTINCT l.id) as total_limit_count,
            COUNT(DISTINCT CASE WHEN l.status = 'hit' THEN l.id END) as hit_limit_count,
            STRING_AGG(
                (CASE WHEN l.status = 'pending' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as pending_limits_str,
            STRING_AGG(
                (CASE WHEN l.status = 'hit' THEN l.price_level END)::TEXT, ',' ORDER BY l.sequence_number) as hit_limits_str,
            MIN(CASE WHEN l.status = 'pending' THEN l.price_level END) as first_pending_limit
        FROM signals s
        LEFT JOIN limits l ON s.id = l.signal_id
        WHERE s.status IN ($1, $2)
    """

    params = [SignalStatus.ACTIVE, SignalStatus.HIT]

    if instrument:
        base_query += " AND s.instrument = $3"
        params.append(instrument)

    base_query += " GROUP BY s.id"

    # Add sorting
    if sort_by == 'recent':
        base_query += " ORDER BY s.created_at DESC"
    elif sort_by == 'oldest':
        base_query += " ORDER BY s.created_at ASC"
    elif sort_by == 'progress':
        # Sort by number of hit limits (descending), then by created_at
        base_query += " ORDER BY hit_limit_count DESC, s.created_at DESC"
    else:
        # Default to recent
        base_query += " ORDER BY s.created_at DESC"

    # Add limit if specified
    if limit:
        base_query += f" LIMIT {limit}"

    signals = await self.db.fetch_all(base_query, tuple(params))

    # Enhance with additional data
    for signal in signals:
        # Parse limit strings into lists
        signal['pending_limits'] = []
        signal['hit_limits'] = []

        if signal.get('pending_limits_str'):
            signal['pending_limits'] = [float(p) for p in signal['pending_limits_str'].split(',')]

        if signal.get('hit_limits_str'):
            signal['hit_limits'] = [float(p) for p in signal['hit_limits_str'].split(',')]

        # Remove temporary string fields
        signal.pop('pending_limits_str', None)
        signal.pop('hit_limits_str', None)
        signal.pop('first_pending_limit', None)  # Remove this temp field

        # Add time remaining for expiry
        if signal.get('expiry_time'):
            expiry = _to_dt(signal['expiry_time'])
            now = datetime.now(pytz.UTC)
            if expiry.tzinfo is None:
                expiry = pytz.UTC.localize(expiry)

            remaining = expiry - now
            if remaining.total_seconds() > 0:
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                signal['time_remaining'] = f"{hours}h {minutes}m"
            else:
                signal['time_remaining'] = "Expired"
        else:
            signal['time_remaining'] = "No expiry"

        # Add status display info
        from .utils import get_status_emoji
        signal['status_emoji'] = get_status_emoji(signal['status'])
        signal['progress'] = f"{signal['hit_limit_count']}/{signal['total_limit_count']} limits hit"

    return signals