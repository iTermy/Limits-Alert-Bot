"""
Signal-specific database operations — CRUD, lifecycle, and analytics
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from core.parser import ParsedSignal
from models.signal import SignalData
from utils.logger import get_logger

from models.enums import LimitStatus, SignalStatus
from utils.formatting import get_status_emoji

from .utils import _parse_dt, calculate_expiry

logger = get_logger("signal_db")


class SignalDatabase:
    """Handles all signal-specific database operations."""

    def __init__(self, db_manager):
        self.db = db_manager

    # === CRUD ===

    async def save_signal(
        self, parsed_signal: ParsedSignal, message_id: str, channel_id: str
    ) -> Tuple[bool, Optional[int]]:
        """Save a parsed signal to the database."""
        try:
            expiry_time = calculate_expiry(parsed_signal.expiry_type)

            existing_id = None
            existing_status = None
            signal_id = None

            async with self.db.get_connection() as conn:
                signal_id = await conn.fetchval(
                    """
                    INSERT INTO signals (
                        message_id, channel_id, instrument, direction,
                        stop_loss, expiry_type, expiry_time, total_limits, status, type
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (message_id) DO NOTHING
                    RETURNING id
                    """,
                    message_id,
                    channel_id,
                    parsed_signal.instrument,
                    parsed_signal.direction,
                    parsed_signal.stop_loss,
                    parsed_signal.expiry_type,
                    _parse_dt(expiry_time),
                    len(parsed_signal.limits) if parsed_signal.limits else 0,
                    SignalStatus.ACTIVE,
                    getattr(parsed_signal, "type", "standard"),
                )

                if signal_id is None:
                    # Conflict: row already existed. Determine what to do.
                    row = await conn.fetchrow(
                        "SELECT id, status FROM signals WHERE message_id = $1", message_id
                    )
                    if row is not None:
                        existing_id = row["id"]
                        existing_status = row["status"]
                else:
                    if parsed_signal.limits:
                        await conn.executemany(
                            """
                            INSERT INTO limits (signal_id, price_level, sequence_number, status)
                            VALUES ($1, $2, $3, $4)
                            """,
                            [
                                (signal_id, level, idx + 1, LimitStatus.PENDING)
                                for idx, level in enumerate(parsed_signal.limits)
                            ],
                        )

            # Handle conflict result after releasing connection
            if signal_id is None:
                if existing_id is None:
                    logger.error(f"Signal not found after conflict on message {message_id}")
                    return False, None
                if existing_status == SignalStatus.CANCELLED:
                    logger.info(f"Reactivating cancelled signal for message {message_id}")
                    await self.reactivate_cancelled_signal(existing_id, parsed_signal)
                    return True, existing_id
                logger.warning(f"Signal already exists for message {message_id} (status={existing_status})")
                return False, existing_id

            logger.info(
                f"Saved signal {signal_id}: {parsed_signal.instrument} "
                f"{parsed_signal.direction} with {len(parsed_signal.limits)} limits"
            )
            return True, signal_id

        except Exception as e:
            logger.error(f"Error saving signal: {e}", exc_info=True)
            return False, None

    async def get_signal_by_message_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """Get signal by Discord message ID."""
        query = "SELECT * FROM signals WHERE message_id = $1"
        signal = await self.db.fetch_one(query, (message_id,))
        if signal is not None:
            signal["signal_id"] = signal["id"]
        return signal

    async def get_signal_with_limits(self, signal_id: int) -> Optional[SignalData]:
        """Get signal with all its limits (including hit ones)."""
        signal_query = "SELECT * FROM signals WHERE id = $1"
        signal = await self.db.fetch_one(signal_query, (signal_id,))

        if not signal:
            return None

        limits_query = """
            SELECT * FROM limits
            WHERE signal_id = $1
            ORDER BY sequence_number
        """
        limits = await self.db.fetch_all(limits_query, (signal_id,))

        return SignalData.from_db_row(signal, limits)

    async def update_signal_from_edit(self, message_id: str, parsed_signal: ParsedSignal) -> bool:
        """Update an existing signal from an edited message."""
        try:
            existing = await self.get_signal_by_message_id(message_id)
            if not existing:
                logger.warning(f"No signal found for message {message_id}")
                return False

            signal_id = existing["id"]

            truly_final = [s for s in SignalStatus.final_statuses() if s != SignalStatus.CANCELLED]
            if existing["status"] in truly_final:
                logger.warning(
                    f"Cannot update signal {signal_id} in final status {existing['status']}"
                )
                return False

            async with self.db.get_connection() as conn:
                await conn.execute(
                    """
                    UPDATE signals
                    SET instrument = $1, direction = $2, stop_loss = $3,
                        expiry_type = $4, total_limits = $5, type = $6,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $7
                    """,
                    parsed_signal.instrument,
                    parsed_signal.direction,
                    parsed_signal.stop_loss,
                    parsed_signal.expiry_type,
                    len(parsed_signal.limits),
                    getattr(parsed_signal, "type", "standard"),
                    signal_id,
                )

                hit_rows = await conn.fetch(
                    "SELECT price_level FROM limits WHERE signal_id = $1 AND status = 'hit' ORDER BY sequence_number",
                    signal_id,
                )
                hit_prices = [r["price_level"] for r in hit_rows]

                await conn.execute("DELETE FROM limits WHERE signal_id = $1", signal_id)

                for idx, level in enumerate(parsed_signal.limits):
                    if level in hit_prices:
                        await conn.execute(
                            """
                            INSERT INTO limits (signal_id, price_level, sequence_number, status, hit_time)
                            VALUES ($1, $2, $3, 'hit', CURRENT_TIMESTAMP)
                            """,
                            signal_id,
                            level,
                            idx + 1,
                        )
                    else:
                        await conn.execute(
                            """
                            INSERT INTO limits (signal_id, price_level, sequence_number, status)
                            VALUES ($1, $2, $3, 'pending')
                            """,
                            signal_id,
                            level,
                            idx + 1,
                        )

            logger.info(f"Updated signal {signal_id} from edited message")
            return True

        except Exception as e:
            logger.error(f"Error updating signal from edit: {e}", exc_info=True)
            return False

    async def get_active_signals_detailed_sorted(
        self, instrument: str = None, sort_by: str = "recent", limit: int = None
    ) -> List[Dict[str, Any]]:
        """Get detailed active signals with sorting options."""
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

        base_query += " GROUP BY s.id"

        if sort_by == "recent":
            base_query += " ORDER BY s.created_at DESC"
        elif sort_by == "oldest":
            base_query += " ORDER BY s.created_at ASC"
        elif sort_by == "progress":
            base_query += " ORDER BY hit_limit_count DESC, s.created_at DESC"
        else:
            base_query += " ORDER BY s.created_at DESC"

        if limit:
            base_query += f" LIMIT {limit}"

        signals = await self.db.fetch_all(base_query, tuple(params))

        for signal in signals:
            signal["pending_limits"] = []
            signal["hit_limits"] = []

            if signal.get("pending_limits_str"):
                signal["pending_limits"] = [
                    float(p) for p in signal["pending_limits_str"].split(",")
                ]

            if signal.get("hit_limits_str"):
                signal["hit_limits"] = [float(p) for p in signal["hit_limits_str"].split(",")]

            signal.pop("pending_limits_str", None)
            signal.pop("hit_limits_str", None)

            if signal.get("expiry_time"):
                expiry = _parse_dt(signal["expiry_time"])
                now = datetime.now(pytz.UTC)
                remaining = expiry - now
                if remaining.total_seconds() > 0:
                    hours = int(remaining.total_seconds() // 3600)
                    minutes = int((remaining.total_seconds() % 3600) // 60)
                    signal["time_remaining"] = f"{hours}h {minutes}m"
                else:
                    signal["time_remaining"] = "Expired"
            else:
                signal["time_remaining"] = "No expiry"

            signal["status_emoji"] = get_status_emoji(signal["status"])
            signal["progress"] = (
                f"{signal['hit_limit_count']}/{signal['total_limit_count']} limits hit"
            )

        return signals

    async def get_signals_for_tracking(self) -> List[Dict[str, Any]]:
        """Get all signals that need price tracking (wrapper for DB method)."""
        return await self.db.get_active_signals_for_tracking()

    async def get_hit_limits_for_signal(self, signal_id: int) -> List[Dict[str, Any]]:
        """Return all hit limits for a signal with hit_price for P&L calculations."""
        return await self.db.get_hit_limits_for_signal(signal_id)

    # === Lifecycle ===

    async def cancel_signal_by_message(self, message_id: str) -> bool:
        """Cancel a signal when its message is deleted or cancelled by user."""
        try:
            logger.debug(f"Starting cancel_signal_by_message for message {message_id}")

            signal = await self.get_signal_by_message_id(message_id)
            if not signal:
                logger.warning(f"No signal found for message {message_id}")
                return False

            logger.debug(f"Found signal {signal['id']} with status {signal['status']}")

            if signal["status"] == SignalStatus.CANCELLED:
                logger.info(f"Signal {signal['id']} is already cancelled")
                return True

            if (
                SignalStatus.is_final(signal["status"])
                and signal["status"] != SignalStatus.CANCELLED
            ):
                logger.warning(
                    f"Cannot cancel signal {signal['id']} in final status {signal['status']}"
                )
                return False

            try:
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # C3 invariant: cancel limits before updating signal status so
                    # EX's Supabase query (WHERE l.status='pending') stops seeing
                    # these limits before the signal transitions to 'cancelled'.
                    await conn.execute(
                        """
                        UPDATE limits
                        SET status = 'cancelled'
                        WHERE signal_id = $1 AND status = 'pending'
                    """,
                        signal["id"],
                    )
                    await conn.execute(
                        """
                        UPDATE signals
                        SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                        WHERE id = $5
                    """,
                        SignalStatus.CANCELLED,
                        now,
                        now,
                        "manual",
                        signal["id"],
                    )
                    await conn.execute(
                        """
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """,
                        signal["id"],
                        signal["status"],
                        SignalStatus.CANCELLED,
                        "manual",
                        "User cancelled",
                    )
                logger.info(f"Successfully cancelled signal {signal['id']}")
                return True

            except Exception as e:
                logger.error(f"Database error while cancelling signal: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error in cancel_signal_by_message: {e}", exc_info=True)
            return False

    async def reactivate_cancelled_signal(
        self, signal_id: int, parsed_signal: ParsedSignal
    ) -> bool:
        """Reactivate a cancelled signal (e.g., when message is undeleted or edited)."""
        try:
            logger.debug(f"Attempting to reactivate signal {signal_id}")

            signal_query = "SELECT * FROM signals WHERE id = $1"
            signal = await self.db.fetch_one(signal_query, (signal_id,))

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            if signal["status"] != SignalStatus.CANCELLED:
                logger.warning(f"Signal {signal_id} is not cancelled, status: {signal['status']}")
                return False

            new_status = (
                SignalStatus.HIT if signal.get("limits_hit", 0) > 0 else SignalStatus.ACTIVE
            )

            try:
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    await conn.execute(
                        """
                        UPDATE signals
                        SET status = $1, closed_at = NULL, closed_reason = NULL, updated_at = $2
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
                        SignalStatus.CANCELLED,
                        new_status,
                        "manual",
                        "Signal reactivated",
                    )
                    # approaching_alert_sent is intentionally left as-is (TRUE) so the
                    # existing approaching embed is reused rather than sending a duplicate.
                    await conn.execute(
                        """
                        UPDATE limits
                        SET status = 'pending'
                        WHERE signal_id = $1 AND status = 'cancelled'
                    """,
                        signal_id,
                    )
                logger.info(f"Successfully reactivated signal {signal_id} to status {new_status}")
                return True

            except Exception as e:
                logger.error(f"Database error reactivating signal: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error reactivating signal: {e}", exc_info=True)
            return False

    async def get_overlapping_signals(
        self, instrument: str, min_limit: float, max_limit: float, exclude_signal_id: int
    ) -> List[Dict[str, Any]]:
        """
        Return active/hit signals on the same instrument whose pending-limit price range
        intersects [min_limit, max_limit].  Used for overlap detection on new signal saves.
        """
        query = """
            SELECT
                s.id,
                s.instrument,
                s.direction,
                s.message_id,
                s.channel_id,
                s.status
            FROM signals s
            JOIN (
                SELECT signal_id,
                       MIN(price_level) AS min_price,
                       MAX(price_level) AS max_price
                FROM limits
                WHERE status = 'pending'
                GROUP BY signal_id
            ) lr ON lr.signal_id = s.id
            WHERE s.status IN ('active', 'hit')
              AND UPPER(s.instrument) = $1
              AND s.id != $2
              AND lr.min_price <= $4
              AND lr.max_price >= $3
        """
        rows = await self.db.fetch_all(
            query, (instrument.upper(), exclude_signal_id, float(min_limit), float(max_limit))
        )
        return [dict(r) for r in rows]

    async def _get_live_price(self, instrument: str) -> Optional[Dict[str, Any]]:
        """Fetch current bid/ask from live_prices table."""
        row = await self.db.fetch_one(
            "SELECT bid, ask FROM live_prices WHERE symbol = $1",
            (instrument.upper(),),
        )
        return dict(row) if row else None

    async def check_reactivation_guard(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """
        Before reactivating a cancelled signal, check whether the current price has
        already moved to or past any of its would-be-pending limits.

        Returns None if the guard cannot run (missing price data) — caller should
        allow reactivation in that case.  Otherwise returns a dict:
          - blocked (bool): True if at least one limit would fire immediately
          - blocked_limits (list): dicts with id, price_level, sequence_number
          - current_price (float): mid-price used for comparison
          - instrument (str)
          - direction (str)
        """
        signal = await self.db.fetch_one(
            "SELECT id, instrument, direction, status FROM signals WHERE id = $1",
            (signal_id,),
        )
        if not signal or signal["status"] != SignalStatus.CANCELLED:
            return None

        # These are limits that were pending at cancellation time and will become
        # pending again on reactivation (currently stored as 'cancelled').
        pending_limits = await self.db.fetch_all(
            """SELECT id, price_level, sequence_number
               FROM limits
               WHERE signal_id = $1 AND status = 'cancelled'
               ORDER BY sequence_number""",
            (signal_id,),
        )
        if not pending_limits:
            return None

        price_data = await self._get_live_price(signal["instrument"])
        if not price_data or price_data.get("bid") is None or price_data.get("ask") is None:
            logger.warning(
                f"No live price for {signal['instrument']} — reactivation guard skipped"
            )
            return None

        mid = (float(price_data["bid"]) + float(price_data["ask"])) / 2
        direction = signal["direction"]

        # A limit is "past" if the hit condition would fire immediately on reactivation:
        # long hits when price ≤ limit; short hits when price ≥ limit.
        blocked = [
            dict(lim)
            for lim in pending_limits
            if (direction == "long" and mid <= float(lim["price_level"]))
            or (direction == "short" and mid >= float(lim["price_level"]))
        ]

        return {
            "blocked": len(blocked) > 0,
            "blocked_limits": blocked,
            "current_price": mid,
            "instrument": signal["instrument"],
            "direction": direction,
        }

    async def manually_set_signal_status(
        self,
        signal_id: int,
        new_status: str,
        reason: str = None,
        result_pips: float = None,
        closed_reason: str = None,
    ) -> bool:
        """
        Manually set a signal's status (for admin override).
        Bypasses validation for manual overrides.
        """
        try:
            logger.debug(f"Manually setting signal {signal_id} to {new_status}")

            if not SignalStatus.is_valid(new_status):
                logger.error(f"Invalid status: {new_status}")
                return False

            signal = await self.db.fetch_one("SELECT * FROM signals WHERE id = $1", (signal_id,))

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = signal["status"]

            if old_status == new_status:
                logger.info(f"Signal {signal_id} already has status {new_status}")
                return True

            effective_closed_reason = closed_reason if closed_reason is not None else "manual"

            try:
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # C3 invariant: for cancel paths, update limits before signal status
                    # so EX's Supabase query stops seeing pending limits before the signal
                    # transitions to a final status.
                    if SignalStatus.is_final(new_status):
                        await conn.execute(
                            """
                            UPDATE limits
                            SET status = 'cancelled'
                            WHERE signal_id = $1 AND status = 'pending'
                        """,
                            signal_id,
                        )
                    elif new_status == SignalStatus.ACTIVE:
                        await conn.execute(
                            """
                            UPDATE limits
                            SET status = 'pending'
                            WHERE signal_id = $1 AND status = 'cancelled'
                        """,
                            signal_id,
                        )

                    if SignalStatus.is_final(new_status):
                        if result_pips is not None:
                            await conn.execute(
                                """
                                UPDATE signals
                                SET status = $1, updated_at = $2, closed_at = $3,
                                    closed_reason = $4, result_pips = $5
                                WHERE id = $6
                            """,
                                new_status,
                                now,
                                now,
                                effective_closed_reason,
                                result_pips,
                                signal_id,
                            )
                        else:
                            await conn.execute(
                                """
                                UPDATE signals
                                SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                                WHERE id = $5
                            """,
                                new_status,
                                now,
                                now,
                                effective_closed_reason,
                                signal_id,
                            )
                    else:
                        await conn.execute(
                            """
                            UPDATE signals
                            SET status = $1, updated_at = $2, closed_at = NULL,
                                closed_reason = NULL, result_pips = NULL
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
                        effective_closed_reason,
                        reason or "Manual override",
                    )
                logger.info(
                    f"Successfully set signal {signal_id} status: {old_status} -> {new_status}"
                    + (f" (result_pips={result_pips:.4f})" if result_pips is not None else "")
                )
                return True

            except Exception as e:
                logger.error(f"Database error setting status: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error manually setting signal status: {e}", exc_info=True)
            return False

    async def manually_set_signal_to_hit(self, signal_id: int, reason: str) -> bool:
        """
        Manually mark a signal as HIT by marking its first pending limit as hit.
        This mimics the behavior of automatic hit detection.
        """
        try:
            signal_query = "SELECT * FROM signals WHERE id = $1"
            signal_row = await self.db.fetch_one(signal_query, (signal_id,))

            if not signal_row:
                logger.error(f"Signal {signal_id} not found")
                return False

            limits = await self.db.fetch_all(
                "SELECT * FROM limits WHERE signal_id = $1 ORDER BY sequence_number", (signal_id,)
            )

            if signal_row["status"] == SignalStatus.HIT:
                logger.info(f"Signal {signal_id} is already HIT — no-op for manual hit")
                return False

            if signal_row["status"] == SignalStatus.CANCELLED:
                logger.info(f"Signal {signal_id} is CANCELLED — reactivating before manual HIT")
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)
                    await conn.execute(
                        """
                        UPDATE signals
                        SET status = $1, updated_at = $2,
                            closed_at = NULL, closed_reason = NULL, result_pips = NULL
                        WHERE id = $3
                    """,
                        SignalStatus.ACTIVE,
                        now,
                        signal_id,
                    )
                    await conn.execute(
                        """
                        UPDATE limits
                        SET status = 'pending'
                        WHERE signal_id = $1 AND status = 'cancelled'
                    """,
                        signal_id,
                    )
                    await conn.execute(
                        """
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, 'manual', $4)
                    """,
                        signal_id,
                        SignalStatus.CANCELLED,
                        SignalStatus.ACTIVE,
                        f"Reactivated as part of manual hit — {reason}",
                    )
                limits = await self.db.fetch_all(
                    "SELECT * FROM limits WHERE signal_id = $1 ORDER BY sequence_number",
                    (signal_id,),
                )

            elif signal_row["status"] != SignalStatus.ACTIVE:
                logger.warning(
                    f"Signal {signal_id} is not ACTIVE (status: {signal_row['status']}), cannot manually mark as HIT"
                )
                return False

            pending_limits = [l for l in limits if l.get("status") == "pending"]

            if not pending_limits:
                logger.warning(
                    f"Signal {signal_id} has no pending limits, cannot manually mark as HIT"
                )
                return False

            first_limit = min(pending_limits, key=lambda l: l.get("sequence_number", 999))

            logger.info(
                f"Manually marking signal {signal_id} as HIT by hitting limit {first_limit['id']} "
                f"(price: {first_limit['price_level']})"
            )

            result = await self.db.mark_limit_hit(
                first_limit["id"],
                first_limit["price_level"],
            )

            if result and result.get("signal_id"):
                logger.info(f"Signal {signal_id} manually marked as HIT via limit hit")

                async with self.db.get_connection() as conn:
                    await conn.execute(
                        """
                        UPDATE status_changes
                        SET reason = $1, change_type = 'manual'
                        WHERE signal_id = $2
                        AND new_status = 'hit'
                        AND id = (
                            SELECT MAX(id) FROM status_changes
                            WHERE signal_id = $3 AND new_status = 'hit'
                        )
                    """,
                        reason,
                        signal_id,
                        signal_id,
                    )
                return True
            logger.error(f"Failed to process limit hit for signal {signal_id}")
            return False

        except Exception as e:
            logger.error(f"Error manually setting signal {signal_id} to hit: {e}", exc_info=True)
            return False

    async def process_limit_hit(self, limit_id: int, actual_price: float = None) -> Dict[str, Any]:
        """Process a limit hit event."""
        result = await self.db.mark_limit_hit(limit_id, actual_price)

        if result["signal_id"]:
            signal = await self.get_signal_with_limits(result["signal_id"])
            result["signal"] = signal

            if signal and len(signal["hit_limits"]) == signal["total_limits"]:
                result["all_limits_hit"] = True
                logger.info(f"All limits hit for signal {signal['id']}")
            else:
                result["all_limits_hit"] = False

        return result

    async def manually_set_signal_expiry(
        self, signal_id: int, expiry_type: str, custom_datetime: str = None
    ) -> bool:
        """Manually set a signal's expiry type and recalculate expiry time."""
        try:
            logger.debug(f"Manually setting signal {signal_id} expiry to {expiry_type}")

            valid_types = ["day_end", "week_end", "month_end", "no_expiry", "custom"]
            if expiry_type not in valid_types:
                logger.error(f"Invalid expiry type: {expiry_type}")
                return False

            if expiry_type == "custom" and not custom_datetime:
                logger.error("Custom expiry type requires datetime")
                return False

            signal = await self.db.fetch_one("SELECT * FROM signals WHERE id = $1", (signal_id,))

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            if SignalStatus.is_final(signal["status"]):
                logger.warning(
                    f"Cannot modify expiry for signal {signal_id} in final status {signal['status']}"
                )
                return False

            if expiry_type == "custom":
                new_expiry_time = custom_datetime
            else:
                new_expiry_time = calculate_expiry(expiry_type)

            try:
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    await conn.execute(
                        """
                        UPDATE signals
                        SET expiry_type = $1, expiry_time = $2, updated_at = $3
                        WHERE id = $4
                    """,
                        expiry_type,
                        _parse_dt(new_expiry_time),
                        now,
                        signal_id,
                    )
                    await conn.execute(
                        """
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """,
                        signal_id,
                        signal["status"],
                        signal["status"],
                        "manual",
                        f"Expiry changed from {signal['expiry_type']} to {expiry_type}",
                    )

                old_expiry = signal["expiry_type"] or "none"
                if expiry_type == "no_expiry":
                    logger.info(f"Removed expiry for signal {signal_id} (was {old_expiry})")
                elif expiry_type == "custom":
                    logger.info(f"Set custom expiry for signal {signal_id} to {new_expiry_time}")
                else:
                    logger.info(
                        f"Changed signal {signal_id} expiry from {old_expiry} to {expiry_type}"
                    )

                return True

            except Exception as e:
                logger.error(f"Database error setting expiry: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error manually setting signal expiry: {e}", exc_info=True)
            return False

    async def expire_old_signals(self) -> int:
        """Check and expire signals past their expiry time."""
        query = """
            SELECT id, status, expiry_type FROM signals
            WHERE status IN ($1, $2)
            AND expiry_time IS NOT NULL
            AND expiry_time < CURRENT_TIMESTAMP
        """

        expired = await self.db.fetch_all(query, (SignalStatus.ACTIVE, SignalStatus.HIT))

        if not expired:
            return 0

        count = 0
        rollover_count = 0

        # Each signal gets its own transaction so a single failure does not
        # roll back signals that were already successfully processed.
        for signal in expired:
            signal_id = signal["id"]
            old_status = signal["status"]

            if old_status == SignalStatus.HIT:
                # HIT signals roll over to the next expiry window instead of cancelling.
                # Re-using calculate_expiry ensures the next occurrence is always in the future.
                next_expiry = calculate_expiry(signal["expiry_type"])
                if next_expiry is None:
                    continue
                try:
                    async with self.db.get_connection() as conn:
                        await conn.execute(
                            """
                            UPDATE signals
                            SET expiry_time = $1, updated_at = CURRENT_TIMESTAMP
                            WHERE id = $2
                            """,
                            _parse_dt(next_expiry),
                            signal_id,
                        )
                        await conn.execute(
                            """
                            INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                            VALUES ($1, $2, $3, $4, $5)
                            """,
                            signal_id,
                            old_status,
                            old_status,
                            "automatic",
                            "rollover",
                        )
                    rollover_count += 1
                except Exception as e:
                    logger.error(f"Error rolling over signal {signal_id}: {e}", exc_info=True)
            else:
                try:
                    async with self.db.get_connection() as conn:
                        # C3 invariant: cancel limits before updating signal status
                        await conn.execute(
                            """
                            UPDATE limits
                            SET status = 'cancelled'
                            WHERE signal_id = $1 AND status = 'pending'
                        """,
                            signal_id,
                        )
                        await conn.execute(
                            """
                            UPDATE signals
                            SET status = $1, updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP, closed_reason = $2
                            WHERE id = $3
                        """,
                            SignalStatus.CANCELLED,
                            "expiry",
                            signal_id,
                        )
                        await conn.execute(
                            """
                            INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                            VALUES ($1, $2, $3, $4, $5)
                        """,
                            signal_id,
                            old_status,
                            SignalStatus.CANCELLED,
                            "automatic",
                            "Expired",
                        )
                    count += 1
                except Exception as e:
                    logger.error(f"Error expiring signal {signal_id}: {e}", exc_info=True)

        if count > 0 or rollover_count > 0:
            logger.info(f"Expired {count} signals, rolled over {rollover_count} HIT signals")

        return count

    async def bulk_update_toll_sl(self, offset: float, channel_ids: list) -> tuple:
        """
        Recompute and persist stop-loss for all active/hit toll signals given a new offset.
        Returns (updated_list, skipped_count, error_count).
        """
        try:
            async with self.db.get_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        s.id, s.direction, s.stop_loss,
                        ARRAY_AGG(l.price_level ORDER BY l.sequence_number)
                            FILTER (WHERE l.id IS NOT NULL) AS all_price_levels
                    FROM signals s
                    LEFT JOIN limits l ON l.signal_id = s.id
                    WHERE s.status IN ('active', 'hit')
                      AND CAST(s.channel_id AS TEXT) = ANY($1)
                    GROUP BY s.id, s.direction, s.stop_loss
                    """,
                    channel_ids,
                )
        except Exception as e:
            logger.error(f"bulk_update_toll_sl fetch failed: {e}", exc_info=True)
            return [], 0, 0

        updated = []
        skipped = 0
        errors = 0

        for row in rows:
            sig_id = row["id"]
            direction = row["direction"]
            old_sl = row["stop_loss"]
            price_levels = row["all_price_levels"] or []

            if not price_levels:
                skipped += 1
                continue

            new_sl = (
                min(price_levels) - offset if direction == "long" else max(price_levels) + offset
            )

            if old_sl is not None and abs(float(old_sl) - new_sl) < 0.001:
                skipped += 1
                continue

            try:
                async with self.db.get_connection() as conn:
                    await conn.execute(
                        "UPDATE signals SET stop_loss = $1 WHERE id = $2", new_sl, sig_id
                    )
                updated.append({"id": sig_id, "new_sl": new_sl})
                logger.info(
                    f"Updated SL for toll signal {sig_id}: {old_sl} → {new_sl} "
                    f"(offset={offset}, dir={direction})"
                )
            except Exception as e:
                errors += 1
                logger.error(f"Failed to update SL for toll signal {sig_id}: {e}", exc_info=True)

        return updated, skipped, errors

    # === Analytics ===

    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        stats = {}

        total_query = "SELECT COUNT(*) as count FROM signals"
        result = await self.db.fetch_one(total_query)
        stats["total_signals"] = result["count"]

        status_query = """
            SELECT status, COUNT(*) as count
            FROM signals
            GROUP BY status
        """
        status_results = await self.db.fetch_all(status_query)
        stats["by_status"] = {row["status"]: row["count"] for row in status_results}

        tracking_query = """
            SELECT COUNT(*) as count
            FROM signals
            WHERE status IN ($1, $2)
        """
        result = await self.db.fetch_one(tracking_query, (SignalStatus.ACTIVE, SignalStatus.HIT))
        stats["tracking_count"] = result["count"]

        today_start = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        today_stats = await self.db.get_performance_stats(start_date=today_start)
        stats["today"] = today_stats["overall"]

        overall_stats = await self.db.get_performance_stats()
        stats["overall"] = overall_stats["overall"]
        stats["by_instrument"] = overall_stats["by_instrument"]

        return stats

    async def get_trading_period_range(self, period: str = "week") -> Dict[str, Any]:
        """
        Get the date range for the current trading period.
        Trading week starts Sunday 6:00 PM UTC and ends Sunday 5:59 PM UTC.
        """
        now = datetime.now(pytz.UTC)

        if period == "week":
            days_since_sunday = (now.weekday() + 1) % 7
            last_sunday = now - timedelta(days=days_since_sunday)

            week_start = last_sunday.replace(hour=18, minute=0, second=0, microsecond=0)

            if now < week_start:
                week_start = week_start - timedelta(days=7)

            week_end = week_start + timedelta(days=7) - timedelta(seconds=1)

            return {
                "start": week_start,
                "end": week_end,
                "display_start": week_start.strftime("%B %d, %Y"),
                "display_end": week_end.strftime("%B %d, %Y"),
            }

        if period == "month":
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            if now.month == 12:
                next_month = month_start.replace(year=now.year + 1, month=1)
            else:
                next_month = month_start.replace(month=now.month + 1)

            month_end = next_month - timedelta(seconds=1)

            return {
                "start": month_start,
                "end": month_end,
                "display_start": month_start.strftime("%B %d, %Y"),
                "display_end": month_end.strftime("%B %d, %Y"),
            }

        raise ValueError(f"Invalid period: {period}")

    async def get_period_signals_with_results(self, start_date, end_date) -> List[Dict[str, Any]]:
        """Get all signals with final results within a date range."""
        query = """
            SELECT
                s.id,
                s.message_id,
                s.channel_id,
                s.instrument,
                s.direction,
                s.status,
                s.limits_hit,
                s.total_limits,
                s.created_at,
                s.closed_at,
                CASE
                    WHEN s.closed_at IS NOT NULL THEN s.closed_at
                    ELSE s.updated_at
                END as completion_time
            FROM signals s
            WHERE s.status IN ($1, $2, $3)
            AND (
                (s.closed_at IS NOT NULL AND s.closed_at >= $4 AND s.closed_at <= $5)
                OR
                (s.closed_at IS NULL AND s.updated_at >= $6 AND s.updated_at <= $7)
            )
            ORDER BY completion_time DESC
        """

        params = (
            SignalStatus.PROFIT,
            SignalStatus.BREAKEVEN,
            SignalStatus.STOP_LOSS,
            start_date,
            end_date,
            start_date,
            end_date,
        )

        signals = await self.db.fetch_all(query, params)

        result = []
        for signal in signals:
            signal_dict = dict(signal)

            signal_dict["status_emoji"] = get_status_emoji(signal_dict["status"])

            if signal_dict["total_limits"] > 0:
                signal_dict["completion_pct"] = (
                    signal_dict["limits_hit"] / signal_dict["total_limits"]
                ) * 100
            else:
                signal_dict["completion_pct"] = 0

            result.append(signal_dict)

        return result
