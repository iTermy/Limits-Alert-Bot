"""
Base database operations for signals and limits
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
import pytz
from .models import SignalStatus, LimitStatus, StatusTransitions, ChangeType
from utils.logger import get_logger

logger = get_logger("database.operations")


def _parse_dt(value) -> Optional[datetime]:
    """Convert an ISO string or datetime to a timezone-aware datetime, or return None.

    Handles all ISO formats including negative UTC offsets like -05:00 (EST/EDT).
    Only assumes UTC for truly naive strings (no timezone info at all).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else pytz.UTC.localize(value)
    s = str(value).replace('Z', '+00:00')
    dt = datetime.fromisoformat(s)
    # Only localize to UTC if the string had no timezone component at all
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt


class BaseOperations:
    """Base database operations for signals and limits"""

    def __init__(self, db_manager):
        self.db = db_manager

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
                            expiry_time: str = None, total_limits: int = 0,
                            scalp: bool = False) -> int:
        """
        Insert a new signal with enhanced tracking

        Returns:
            Signal ID
        """
        query = """
            INSERT INTO signals (
                message_id, channel_id, instrument, direction,
                stop_loss, expiry_type, expiry_time, total_limits, status, scalp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        """

        signal_id = await self.db.execute(
            query,
            (message_id, channel_id, instrument, direction, stop_loss,
             expiry_type, _parse_dt(expiry_time), total_limits, SignalStatus.ACTIVE, scalp)
        )

        logger.info(f"Inserted signal {signal_id} for {instrument} {direction} with {total_limits} limits (scalp={scalp})")
        return signal_id

    async def insert_limits(self, signal_id: int, price_levels: List[float]):
        """Insert limits for a signal with sequence numbers"""
        query = """
            INSERT INTO limits (signal_id, price_level, sequence_number, status)
            VALUES ($1, $2, $3, $4)
        """
        params_list = [
            (signal_id, level, idx + 1, LimitStatus.PENDING)
            for idx, level in enumerate(price_levels)
        ]
        await self.db.execute_many(query, params_list)
        logger.info(f"Inserted {len(price_levels)} limits for signal {signal_id}")

    async def update_signal_status(self, signal_id: int, new_status: str,
                                   change_type: str = 'automatic', reason: str = None) -> bool:
        """Update signal status with proper lifecycle management"""
        async with self.db.get_connection() as conn:
            current = await self.db.fetch_one(
                "SELECT status FROM signals WHERE id = $1",
                (signal_id,)
            )
            if not current:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = current['status']

            if not StatusTransitions.is_valid_transition(old_status, new_status):
                logger.warning(f"Invalid status transition: {old_status} -> {new_status}")
                return False

            now = datetime.now(pytz.UTC)

            if SignalStatus.is_final(new_status):
                await conn.execute("""
                    UPDATE signals
                    SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                    WHERE id = $5
                """, new_status, now, now, change_type, signal_id)
            else:
                await conn.execute("""
                    UPDATE signals
                    SET status = $1, updated_at = $2
                    WHERE id = $3
                """, new_status, now, signal_id)

            await conn.execute("""
                INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                VALUES ($1, $2, $3, $4, $5)
            """, signal_id, old_status, new_status, change_type, reason)

            logger.info(f"Updated signal {signal_id}: {old_status} -> {new_status} ({change_type})")
            return True

    async def mark_limit_hit(self, limit_id: int, hit_price: float = None) -> Dict[str, Any]:
        """Mark a limit as hit and update signal status if needed"""
        async with self.db.get_connection() as conn:
            limit_data = await conn.fetchrow("""
                SELECT l.*, s.status as signal_status, s.id as signal_id
                FROM limits l
                JOIN signals s ON l.signal_id = s.id
                WHERE l.id = $1
            """, limit_id)

            if not limit_data:
                logger.error(f"Limit {limit_id} not found")
                return {'signal_id': None, 'status_changed': False}

            limit_data = dict(limit_data)
            signal_id = limit_data['signal_id']
            now = datetime.now(pytz.UTC)

            await conn.execute("""
                UPDATE limits
                SET status = $1, hit_time = $2, hit_price = $3, hit_alert_sent = TRUE
                WHERE id = $4
            """, LimitStatus.HIT, now, hit_price or limit_data['price_level'], limit_id)

            await conn.execute("""
                UPDATE signals
                SET limits_hit = limits_hit + 1, updated_at = $1
                WHERE id = $2
            """, now, signal_id)

            status_changed = False
            if limit_data['signal_status'] == SignalStatus.ACTIVE:
                await conn.execute("""
                    UPDATE signals
                    SET status = $1, first_limit_hit_time = $2, updated_at = $3
                    WHERE id = $4
                """, SignalStatus.HIT, now, now, signal_id)

                await conn.execute("""
                    INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                    VALUES ($1, $2, $3, $4, $5)
                """, signal_id, SignalStatus.ACTIVE, SignalStatus.HIT,
                    ChangeType.AUTOMATIC, f'Limit {limit_id} hit')

                status_changed = True
                logger.info(f"Signal {signal_id} status changed to HIT (first limit hit)")

            return {
                'signal_id': signal_id,
                'status_changed': status_changed,
                'signal_status': SignalStatus.HIT if status_changed else limit_data['signal_status']
            }

    async def check_stop_loss_hit(self, signal_id: int, current_price: float) -> bool:
        """Check if stop loss has been hit and update status if needed"""
        signal = await self.db.fetch_one("""
            SELECT direction, stop_loss, status
            FROM signals
            WHERE id = $1
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
                signal_id, SignalStatus.STOP_LOSS,
                ChangeType.AUTOMATIC, f'Stop loss hit at {current_price}'
            )
            logger.info(f"Signal {signal_id} hit stop loss at {current_price}")

        return stop_hit

    async def get_active_signals_for_tracking(self) -> List[Dict[str, Any]]:
        """Get all signals that need price tracking (ACTIVE or HIT status)"""
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
                s.scalp,
                l.id as limit_id,
                l.price_level,
                l.sequence_number,
                l.approaching_alert_sent,
                l.hit_alert_sent
            FROM signals s
            LEFT JOIN limits l ON s.id = l.signal_id AND l.status = $1
            WHERE s.status IN ($2, $3)
            ORDER BY s.id, l.sequence_number
        """
        rows = await self.db.fetch_all(
            query,
            (LimitStatus.PENDING, SignalStatus.ACTIVE, SignalStatus.HIT)
        )

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
                    'scalp': row['scalp'] or False,
                    'pending_limits': []
                }
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
        """Mark that an approaching alert has been sent for a limit"""
        rows = await self.db.execute(
            "UPDATE limits SET approaching_alert_sent = TRUE WHERE id = $1",
            (limit_id,)
        )
        return rows > 0

    async def mark_hit_alert_sent(self, limit_id: int) -> bool:
        """Mark that a hit alert has been sent for a limit"""
        rows = await self.db.execute(
            "UPDATE limits SET hit_alert_sent = TRUE WHERE id = $1",
            (limit_id,)
        )
        return rows > 0

    async def get_hit_limits_for_signal(self, signal_id: int) -> List[Dict[str, Any]]:
        """
        Return all hit limits for a signal ordered by sequence_number.
        Includes hit_price (actual fill price) for P&L calculations.
        """
        query = """
            SELECT id AS limit_id,
                   sequence_number,
                   price_level,
                   hit_price,
                   hit_time
            FROM limits
            WHERE signal_id = $1 AND status = 'hit'
            ORDER BY sequence_number
        """
        rows = await self.db.fetch_all(query, (signal_id,))
        return [dict(r) for r in rows]

    async def get_performance_stats(self, start_date: str = None, end_date: str = None,
                                    instrument: str = None) -> Dict[str, Any]:
        """Get performance statistics for closed signals"""
        conditions = ["status IN ('profit', 'breakeven', 'stop_loss')"]
        params = []
        param_idx = 1

        if start_date:
            conditions.append(f"closed_at >= ${param_idx}")
            params.append(_parse_dt(start_date))
            param_idx += 1
        if end_date:
            conditions.append(f"closed_at <= ${param_idx}")
            params.append(_parse_dt(end_date))
            param_idx += 1
        if instrument:
            conditions.append(f"instrument = ${param_idx}")
            params.append(instrument)
            param_idx += 1

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                COUNT(*) as total_trades,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as profitable,
                COUNT(CASE WHEN status = 'breakeven' THEN 1 END) as breakeven,
                COUNT(CASE WHEN status = 'stop_loss' THEN 1 END) as stop_loss,
                ROUND(
                    CAST(COUNT(CASE WHEN status = 'profit' THEN 1 END) AS NUMERIC) /
                    NULLIF(COUNT(*), 0) * 100, 2
                ) as win_rate
            FROM signals
            WHERE {where_clause}
        """
        stats = await self.db.fetch_one(query, tuple(params))

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
        """Check if a status transition is valid (wrapper for backward compatibility)"""
        return StatusTransitions.is_valid_transition(old_status, new_status)