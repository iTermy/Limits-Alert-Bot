"""
DatabaseManager — integrates connection and core signal/limit operations
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from models.signal import SignalData
from utils.logger import get_logger

from models.enums import ChangeType, LimitStatus, SignalStatus, StatusTransitions

from .connection import DatabaseManager as BaseConnectionManager
from .schema import initialize_database
from .utils import _parse_dt, calculate_expiry

logger = get_logger("database")


class DatabaseManager(BaseConnectionManager):
    """Database manager with all core operations integrated."""

    async def initialize(self):
        """Initialize database and create tables."""
        await initialize_database(self)
        logger.info("Database manager initialized successfully")

    # === Signal Status Operations ===

    async def update_signal_status(
        self, signal_id: int, new_status: str, change_type: str = "automatic", reason: str = None
    ) -> bool:
        """Update signal status with proper lifecycle management."""
        async with self.get_connection() as conn:
            current = await self.fetch_one("SELECT status FROM signals WHERE id = $1", (signal_id,))
            if not current:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = current["status"]

            if not StatusTransitions.is_valid_transition(old_status, new_status):
                logger.warning(f"Invalid status transition: {old_status} -> {new_status}")
                return False

            now = datetime.now(pytz.UTC)

            if SignalStatus.is_final(new_status):
                await conn.execute(
                    """
                    UPDATE signals
                    SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                    WHERE id = $5
                """,
                    new_status,
                    now,
                    now,
                    change_type,
                    signal_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE signals
                    SET status = $1, updated_at = $2
                    WHERE id = $3
                """,
                    new_status,
                    now,
                    signal_id,
                )

            await conn.execute(
                """
                INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                VALUES ($1, $2, $3, $4, $5)
            """,
                signal_id,
                old_status,
                new_status,
                change_type,
                reason,
            )

            logger.info(f"Updated signal {signal_id}: {old_status} -> {new_status} ({change_type})")
            return True

    async def mark_limit_hit(self, limit_id: int, hit_price: float = None) -> Dict[str, Any]:
        """Mark a limit as hit and update signal status if needed."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                limit_data = await conn.fetchrow(
                    """
                    SELECT l.*, s.status as signal_status, s.id as signal_id
                    FROM limits l
                    JOIN signals s ON l.signal_id = s.id
                    WHERE l.id = $1
                """,
                    limit_id,
                )

                if not limit_data:
                    logger.error(f"Limit {limit_id} not found")
                    return {"signal_id": None, "status_changed": False}

                limit_data = dict(limit_data)
                signal_id = limit_data["signal_id"]
                now = datetime.now(pytz.UTC)

                await conn.execute(
                    """
                    UPDATE limits
                    SET status = $1, hit_time = $2, hit_price = $3, hit_alert_sent = TRUE
                    WHERE id = $4
                """,
                    LimitStatus.HIT,
                    now,
                    hit_price or limit_data["price_level"],
                    limit_id,
                )

                await conn.execute(
                    """
                    UPDATE signals
                    SET limits_hit = limits_hit + 1, updated_at = $1
                    WHERE id = $2
                """,
                    now,
                    signal_id,
                )

                status_changed = False
                if limit_data["signal_status"] == SignalStatus.ACTIVE:
                    await conn.execute(
                        """
                        UPDATE signals
                        SET status = $1, first_limit_hit_time = $2, updated_at = $3
                        WHERE id = $4
                    """,
                        SignalStatus.HIT,
                        now,
                        now,
                        signal_id,
                    )

                    await conn.execute(
                        """
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """,
                        signal_id,
                        SignalStatus.ACTIVE,
                        SignalStatus.HIT,
                        ChangeType.AUTOMATIC,
                        f"Limit {limit_id} hit",
                    )

                    status_changed = True
                    logger.info(f"Signal {signal_id} status changed to HIT (first limit hit)")

                return {
                    "signal_id": signal_id,
                    "status_changed": status_changed,
                    "signal_status": SignalStatus.HIT
                    if status_changed
                    else limit_data["signal_status"],
                }

    async def get_active_signals_for_tracking(self) -> List[SignalData]:
        """Get all signals that need price tracking (ACTIVE or HIT status)."""
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
                s.type,
                s.alert_message_id,
                s.alert_channel_id,
                s.ping_message_id,
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
        rows = await self.fetch_all(
            query, (LimitStatus.PENDING, SignalStatus.ACTIVE, SignalStatus.HIT)
        )

        signals: Dict[int, dict] = {}
        for row in rows:
            signal_id = row["signal_id"]
            if signal_id not in signals:
                signals[signal_id] = {
                    "signal_id": signal_id,
                    "message_id": row["message_id"],
                    "channel_id": row["channel_id"],
                    "instrument": row["instrument"],
                    "direction": row["direction"],
                    "stop_loss": row["stop_loss"],
                    "status": row["status"],
                    "limits_hit": row["limits_hit"],
                    "total_limits": row["total_limits"],
                    "type": row["type"] or "standard",
                    "alert_message_id": row["alert_message_id"],
                    "alert_channel_id": row["alert_channel_id"],
                    "ping_message_id": row["ping_message_id"],
                    "limits": [],
                }
            if row["limit_id"]:
                signals[signal_id]["limits"].append(
                    {
                        "limit_id": row["limit_id"],
                        "signal_id": signal_id,
                        "price_level": row["price_level"],
                        "sequence_number": row["sequence_number"],
                        "approaching_alert_sent": row["approaching_alert_sent"],
                        "hit_alert_sent": row["hit_alert_sent"],
                    }
                )

        return [SignalData.model_validate(s) for s in signals.values()]

    async def get_hit_limits_for_signal(self, signal_id: int) -> List[Dict[str, Any]]:
        """Return all hit limits for a signal ordered by sequence_number."""
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
        rows = await self.fetch_all(query, (signal_id,))
        return [dict(r) for r in rows]

    async def get_performance_stats(
        self, start_date: str = None, end_date: str = None, instrument: str = None
    ) -> Dict[str, Any]:
        """Get performance statistics for closed signals."""
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
        stats = await self.fetch_one(query, tuple(params))

        instrument_query = f"""
            SELECT
                instrument,
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'profit' THEN 1 END) as wins
            FROM signals
            WHERE {where_clause}
            GROUP BY instrument
        """
        instrument_stats = await self.fetch_all(instrument_query, tuple(params))

        return {"overall": dict(stats) if stats else {}, "by_instrument": instrument_stats}

    # === Expiry Operations ===

    async def update_signal_expiry(self, signal_id: int, expiry_type: str) -> bool:
        """Update a signal's expiry type and recalculate expiry time."""
        valid_types = ["day_end", "week_end", "month_end", "no_expiry"]
        if expiry_type not in valid_types:
            logger.error(f"Invalid expiry type: {expiry_type}")
            return False

        signal = await self.fetch_one("SELECT * FROM signals WHERE id = $1", (signal_id,))

        if not signal:
            logger.error(f"Signal {signal_id} not found")
            return False

        if SignalStatus.is_final(signal["status"]):
            logger.warning(
                f"Cannot modify expiry for signal {signal_id} in final status {signal['status']}"
            )
            return False

        new_expiry_time = calculate_expiry(expiry_type)

        try:
            now = datetime.now(pytz.UTC)

            query = """
                UPDATE signals
                SET expiry_type = $1, expiry_time = $2, updated_at = $3
                WHERE id = $4
            """

            rows = await self.execute(
                query, (expiry_type, _parse_dt(new_expiry_time), now, signal_id)
            )

            if rows > 0:
                logger.info(f"Updated expiry for signal {signal_id} to {expiry_type}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error updating signal expiry: {e}", exc_info=True)
            return False

    # === Bot Mode Status ===

    async def set_news_mode(self, value: Optional[str]) -> None:
        """Set bot_mode_status.news_mode to a category list (e.g. 'EUR, GOLD') or NULL.

        A non-empty string marks news as active; NULL marks it inactive. Consumers
        (including the EX bot) read this column for truthiness, so callers must pass
        None — never the literal string 'FALSE' — when no news is active.
        """
        try:
            await self.execute(
                """
                UPDATE bot_mode_status
                SET news_mode = $1, updated_at = NOW()
                WHERE id = 1
                """,
                (value,),
            )
            logger.debug(f"bot_mode_status.news_mode set to {value!r}")
        except Exception as e:
            logger.error(f"Failed to update news_mode status: {e}", exc_info=True)

    async def set_vol_guard_mode(self, value: Optional[str]) -> None:
        """Set bot_mode_status.vol_guard to a currency list (e.g. 'EUR, USD'), 'ALL',
        or NULL.

        A non-empty string marks the market as volatile; NULL marks it calm. Like
        news_mode, consumers read this column for truthiness, so callers must pass
        None — never the literal string 'FALSE' — when no volatility is active.
        """
        try:
            await self.execute(
                """
                UPDATE bot_mode_status
                SET vol_guard = $1, updated_at = NOW()
                WHERE id = 1
                """,
                (value,),
            )
            logger.debug(f"bot_mode_status.vol_guard set to {value!r}")
        except Exception as e:
            logger.error(f"Failed to update vol_guard status: {e}", exc_info=True)

    async def set_spread_hour(self, active: bool) -> None:
        """Update the spread_hour flag in bot_mode_status."""
        try:
            await self.execute(
                """
                UPDATE bot_mode_status
                SET spread_hour = $1, updated_at = NOW()
                WHERE id = 1
                """,
                (active,),
            )
            logger.debug(f"bot_mode_status.spread_hour set to {active}")
        except Exception as e:
            logger.error(f"Failed to update spread_hour status: {e}", exc_info=True)

    async def get_bot_mode_status(self) -> dict:
        """Retrieve the current bot mode flags."""
        try:
            row = await self.fetch_one(
                "SELECT news_mode, vol_guard, spread_hour, updated_at "
                "FROM bot_mode_status WHERE id = 1",
                (),
            )
            if row:
                return dict(row)
            return {"news_mode": None, "vol_guard": None, "spread_hour": False, "updated_at": None}
        except Exception as e:
            logger.error(f"Failed to fetch bot_mode_status: {e}", exc_info=True)
            return {"news_mode": None, "vol_guard": None, "spread_hour": False, "updated_at": None}
