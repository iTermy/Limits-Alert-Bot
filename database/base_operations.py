"""
Base database operations for signals and limits
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
import pytz
from .models import SignalStatus, LimitStatus, StatusTransitions, ChangeType
from utils.logger import get_logger

logger = get_logger("database.operations")


class BaseOperations:
    """Base database operations for signals and limits"""

    def __init__(self, db_manager):
        """
        Initialize base operations

        Args:
            db_manager: DatabaseManager instance
        """
        self.db = db_manager

        # Import status constants for backward compatibility
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

    async def insert_signal(self, message_id: str, channel_id: str, instrument: str,
                            direction: str, stop_loss: float, expiry_type: str = None,
                            expiry_time: str = None, total_limits: int = 0) -> int:
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

        Returns:
            Signal ID
        """
        query = """
            INSERT INTO signals (
                message_id, channel_id, instrument, direction, 
                stop_loss, expiry_type, expiry_time, total_limits, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        signal_id = await self.db.execute(
            query,
            (message_id, channel_id, instrument, direction, stop_loss,
             expiry_type, expiry_time, total_limits, SignalStatus.ACTIVE)
        )

        logger.info(f"Inserted signal {signal_id} for {instrument} {direction} with {total_limits} limits")
        return signal_id

    async def insert_limits(self, signal_id: int, price_levels: List[float]):
        """
        Insert limits for a signal with sequence numbers

        Args:
            signal_id: Parent signal ID
            price_levels: List of limit prices (ordered)
        """
        query = """
            INSERT INTO limits (signal_id, price_level, sequence_number, status) 
            VALUES (?, ?, ?, ?)
        """

        params_list = [
            (signal_id, level, idx + 1, LimitStatus.PENDING)
            for idx, level in enumerate(price_levels)
        ]

        await self.db.execute_many(query, params_list)
        logger.info(f"Inserted {len(price_levels)} limits for signal {signal_id}")

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
        async with self.db.get_connection() as conn:
            # Get current status
            current = await self.db.fetch_one(
                "SELECT status FROM signals WHERE id = ?",
                (signal_id,)
            )

            if not current:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = current['status']

            # Validate status transition
            if not StatusTransitions.is_valid_transition(old_status, new_status):
                logger.warning(f"Invalid status transition: {old_status} -> {new_status}")
                return False

            # Update signal
            now = datetime.now(pytz.UTC).isoformat()

            if SignalStatus.is_final(new_status):
                # Set closed_at for final statuses
                query = """
                    UPDATE signals 
                    SET status = ?, updated_at = ?, closed_at = ?, closed_reason = ?
                    WHERE id = ?
                """
                await conn.execute(query, (new_status, now, now, change_type, signal_id))
            else:
                # Regular status update
                query = """
                    UPDATE signals 
                    SET status = ?, updated_at = ?
                    WHERE id = ?
                """
                await conn.execute(query, (new_status, now, signal_id))

            # Record status change
            await conn.execute("""
                INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                VALUES (?, ?, ?, ?, ?)
            """, (signal_id, old_status, new_status, change_type, reason))

            await conn.commit()

            logger.info(f"Updated signal {signal_id}: {old_status} -> {new_status} ({change_type})")
            return True

    async def mark_limit_hit(self, limit_id: int, hit_price: float = None) -> Dict[str, Any]:
        """
        Mark a limit as hit and update signal status if needed

        Args:
            limit_id: Limit ID
            hit_price: Actual price when hit (for spread tracking)

        Returns:
            Dict with signal_id and whether signal status changed
        """
        async with self.db.get_connection() as conn:
            # Get limit and signal info
            query = """
                SELECT l.*, s.status as signal_status, s.id as signal_id
                FROM limits l
                JOIN signals s ON l.signal_id = s.id
                WHERE l.id = ?
            """
            result = await conn.execute(query, (limit_id,))
            limit_data = await result.fetchone()

            if not limit_data:
                logger.error(f"Limit {limit_id} not found")
                return {'signal_id': None, 'status_changed': False}

            limit_data = dict(limit_data)
            signal_id = limit_data['signal_id']

            # Update limit - CRITICAL FIX: Set hit_alert_sent = 1
            now = datetime.now(pytz.UTC).isoformat()
            await conn.execute("""
                UPDATE limits 
                SET status = ?, 
                    hit_time = ?, 
                    hit_price = ?,
                    hit_alert_sent = 1
                WHERE id = ?
            """, (LimitStatus.HIT, now, hit_price or limit_data['price_level'], limit_id))

            # Update signal limits_hit count
            await conn.execute("""
                UPDATE signals 
                SET limits_hit = limits_hit + 1,
                    updated_at = ?
                WHERE id = ?
            """, (now, signal_id))

            # Check if signal status should change from active to hit
            status_changed = False
            if limit_data['signal_status'] == SignalStatus.ACTIVE:
                await conn.execute("""
                    UPDATE signals 
                    SET status = ?, first_limit_hit_time = ?, updated_at = ?
                    WHERE id = ?
                """, (SignalStatus.HIT, now, now, signal_id))

                # Record status change
                await conn.execute("""
                    INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (signal_id, SignalStatus.ACTIVE, SignalStatus.HIT,
                      ChangeType.AUTOMATIC, f'Limit {limit_id} hit'))

                status_changed = True
                logger.info(f"Signal {signal_id} status changed to HIT (first limit hit)")

            await conn.commit()

            return {
                'signal_id': signal_id,
                'status_changed': status_changed,
                'signal_status': SignalStatus.HIT if status_changed else limit_data['signal_status']
            }

    async def check_stop_loss_hit(self, signal_id: int, current_price: float) -> bool:
        """
        Check if stop loss has been hit and update status if needed

        Args:
            signal_id: Signal ID
            current_price: Current market price

        Returns:
            True if stop loss was hit
        """
        signal = await self.db.fetch_one("""
            SELECT direction, stop_loss, status 
            FROM signals 
            WHERE id = ?
        """, (signal_id,))

        if not signal or signal['status'] not in [SignalStatus.HIT]:
            return False

        stop_hit = False

        if signal['direction'] == 'long' and current_price <= signal['stop_loss']:
            stop_hit = True
        elif signal['direction'] == 'short' and current_price >= signal['stop_loss']:
            stop_hit = True

        if stop_hit:
            await self.update_signal_status(
                signal_id,
                SignalStatus.STOP_LOSS,
                ChangeType.AUTOMATIC,
                f'Stop loss hit at {current_price}'
            )
            logger.info(f"Signal {signal_id} hit stop loss at {current_price}")

        return stop_hit

    async def get_active_signals_for_tracking(self) -> List[Dict[str, Any]]:
        """
        Get all signals that need price tracking (ACTIVE or HIT status)

        Returns:
            List of signals with their pending limits
        """
        query = """
            SELECT 
                s.id as signal_id,
                s.message_id,
                s.channel_id,
                s.instrument,
                s.direction,
                s.stop_loss,
                s.status,
                s.limits_hit,
                s.total_limits,
                l.id as limit_id,
                l.price_level,
                l.sequence_number,
                l.approaching_alert_sent,
                l.hit_alert_sent
            FROM signals s
            LEFT JOIN limits l ON s.id = l.signal_id AND l.status = ?
            WHERE s.status IN (?, ?)
            ORDER BY s.id, l.sequence_number
        """

        rows = await self.db.fetch_all(
            query,
            (LimitStatus.PENDING, SignalStatus.ACTIVE, SignalStatus.HIT)
        )

        # Group by signal
        signals = {}
        for row in rows:
            signal_id = row['signal_id']
            if signal_id not in signals:
                signals[signal_id] = {
                    'signal_id': signal_id,
                    'message_id': row['message_id'],
                    'channel_id': row['channel_id'],
                    'instrument': row['instrument'],
                    'direction': row['direction'],
                    'stop_loss': row['stop_loss'],
                    'status': row['status'],
                    'limits_hit': row['limits_hit'],
                    'total_limits': row['total_limits'],
                    'pending_limits': []
                }

            # Add pending limit if exists
            if row['limit_id']:
                signals[signal_id]['pending_limits'].append({
                    'limit_id': row['limit_id'],
                    'price_level': row['price_level'],
                    'sequence_number': row['sequence_number'],
                    'approaching_alert_sent': row['approaching_alert_sent'],
                    'hit_alert_sent': row['hit_alert_sent']
                })

        return list(signals.values())

    async def mark_approaching_alert_sent(self, limit_id: int) -> bool:
        """
        Mark that an approaching alert has been sent for a limit

        Args:
            limit_id: Limit ID

        Returns:
            Success status
        """
        query = "UPDATE limits SET approaching_alert_sent = TRUE WHERE id = ?"
        rows = await self.db.execute(query, (limit_id,))
        return rows > 0

    async def mark_hit_alert_sent(self, limit_id: int) -> bool:
        """
        Mark that a hit alert has been sent for a limit

        Args:
            limit_id: Limit ID

        Returns:
            Success status
        """
        query = "UPDATE limits SET hit_alert_sent = TRUE WHERE id = ?"
        rows = await self.db.execute(query, (limit_id,))
        return rows > 0

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
        # Build query
        conditions = ["status IN ('profit', 'breakeven', 'stop_loss')"]
        params = []

        if start_date:
            conditions.append("closed_at >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("closed_at <= ?")
            params.append(end_date)

        if instrument:
            conditions.append("instrument = ?")
            params.append(instrument)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT 
                COUNT(*) as total_trades,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as profitable,
                COUNT(CASE WHEN status = 'breakeven' THEN 1 END) as breakeven,
                COUNT(CASE WHEN status = 'stop_loss' THEN 1 END) as stop_loss,
                ROUND(
                    CAST(COUNT(CASE WHEN status = 'profit' THEN 1 END) AS FLOAT) / 
                    NULLIF(COUNT(*), 0) * 100, 2
                ) as win_rate
            FROM signals
            WHERE {where_clause}
        """

        stats = await self.db.fetch_one(query, tuple(params))

        # Get breakdown by instrument
        instrument_query = f"""
            SELECT 
                instrument,
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as wins
            FROM signals
            WHERE {where_clause}
            GROUP BY instrument
        """

        instrument_stats = await self.db.fetch_all(instrument_query, tuple(params))

        return {
            'overall': dict(stats) if stats else {},
            'by_instrument': instrument_stats
        }

    def _is_valid_transition(self, old_status: str, new_status: str) -> bool:
        """
        Check if a status transition is valid
        (Wrapper for backward compatibility)

        Args:
            old_status: Current status
            new_status: Desired new status

        Returns:
            Whether transition is valid
        """
        return StatusTransitions.is_valid_transition(old_status, new_status)