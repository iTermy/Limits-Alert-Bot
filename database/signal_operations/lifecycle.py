"""
Signal lifecycle management operations
"""
from typing import Dict, Any
from datetime import datetime
import pytz
from database.models import SignalStatus
from core.parser import ParsedSignal


def _parse_dt(value):
    """Convert ISO string or datetime to timezone-aware datetime, or None."""
    if value is None:
        return None
    if hasattr(value, 'tzinfo'):
        import pytz
        return value if value.tzinfo else pytz.UTC.localize(value)
    from datetime import datetime
    s = str(value)
    if '+' in s or s.endswith('Z'):
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    import pytz
    return pytz.UTC.localize(datetime.fromisoformat(s))
from utils.logger import get_logger

logger = get_logger("signal_db.lifecycle")


class LifecycleManager:
    """Manages signal lifecycle transitions and status changes"""

    def __init__(self, db_manager):
        """
        Initialize lifecycle manager

        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager

    async def cancel_signal_by_message(self, message_id: str, signal_db) -> bool:
        """
        Cancel a signal when its message is deleted or cancelled by user

        Args:
            message_id: Discord message ID
            signal_db: SignalDatabase instance for accessing CRUD operations

        Returns:
            Success status
        """
        try:
            logger.debug(f"Starting cancel_signal_by_message for message {message_id}")

            # Get signal using CRUD operations
            signal = await signal_db.get_signal_by_message_id(message_id)
            if not signal:
                logger.warning(f"No signal found for message {message_id}")
                return False

            logger.debug(f"Found signal {signal['id']} with status {signal['status']}")

            # Check if already cancelled
            if signal['status'] == SignalStatus.CANCELLED:
                logger.info(f"Signal {signal['id']} is already cancelled")
                return True

            # Check if in final status that can't be cancelled
            if SignalStatus.is_final(signal['status']) and signal['status'] != SignalStatus.CANCELLED:
                logger.warning(f"Cannot cancel signal {signal['id']} in final status {signal['status']}")
                return False

            # Directly update the signal status without validation
            # (since we're cancelling, we bypass normal transition rules)
            try:
                async with self.db.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # Update signal status
                    await conn.execute("""
                        UPDATE signals 
                        SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                        WHERE id = $5
                    """, SignalStatus.CANCELLED, now, now, 'manual', signal['id'])
                    # Record status change
                    await conn.execute("""
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """, signal['id'], signal['status'], SignalStatus.CANCELLED, 'manual', 'User cancelled')
                    # Cancel all pending limits
                    await conn.execute("""
                        UPDATE limits 
                        SET status = 'cancelled' 
                        WHERE signal_id = $1 AND status = 'pending'
                    """, signal['id'],)
                logger.info(f"Successfully cancelled signal {signal['id']}")
                return True

            except Exception as e:
                logger.error(f"Database error while cancelling signal: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error in cancel_signal_by_message: {e}", exc_info=True)
            return False

    async def reactivate_cancelled_signal(self, signal_id: int, parsed_signal: ParsedSignal, db_manager) -> bool:
        """
        Reactivate a cancelled signal (e.g., when message is undeleted or edited)

        Args:
            signal_id: Signal ID to reactivate
            parsed_signal: New parsed signal data
            db_manager: Database manager for fetching signal

        Returns:
            Success status
        """
        try:
            logger.debug(f"Attempting to reactivate signal {signal_id}")

            # Get current signal state with limits
            signal_query = "SELECT * FROM signals WHERE id = $1"
            signal = await db_manager.fetch_one(signal_query, (signal_id,))

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            if signal['status'] != SignalStatus.CANCELLED:
                logger.warning(f"Signal {signal_id} is not cancelled, status: {signal['status']}")
                return False

            # Determine new status based on whether any limits were hit before cancellation
            new_status = SignalStatus.HIT if signal.get('limits_hit', 0) > 0 else SignalStatus.ACTIVE

            # Directly update without going through validation
            try:
                async with db_manager.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # Clear closed_at and update status
                    await conn.execute("""
                        UPDATE signals 
                        SET status = $1, closed_at = NULL, closed_reason = NULL, updated_at = $2
                        WHERE id = $3
                    """, new_status, now, signal_id)
                    # Record status change
                    await conn.execute("""
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """, signal_id, SignalStatus.CANCELLED, new_status, 'manual', 'Signal reactivated')
                    # Reactivate cancelled limits as pending
                    await conn.execute("""
                        UPDATE limits 
                        SET status = 'pending' 
                        WHERE signal_id = $1 AND status = 'cancelled'
                    """, signal_id,)
                logger.info(f"Successfully reactivated signal {signal_id} to status {new_status}")
                return True

            except Exception as e:
                logger.error(f"Database error reactivating signal: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error reactivating signal: {e}", exc_info=True)
            return False

    async def manually_set_signal_status(self, signal_id: int, new_status: str,
                                        reason: str, db_manager) -> bool:
        """
        Manually set a signal's status (for admin override)
        Bypasses validation for manual overrides

        Args:
            signal_id: Signal ID
            new_status: New status to set
            reason: Optional reason for manual change
            db_manager: Database manager instance

        Returns:
            Success status
        """
        try:
            logger.debug(f"Manually setting signal {signal_id} to {new_status}")

            # Validate status
            if not SignalStatus.is_valid(new_status):
                logger.error(f"Invalid status: {new_status}")
                return False

            # Get current signal
            signal = await db_manager.fetch_one(
                "SELECT * FROM signals WHERE id = $1",
                (signal_id,)
            )

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = signal['status']

            # If same status, return success
            if old_status == new_status:
                logger.info(f"Signal {signal_id} already has status {new_status}")
                return True

            # For manual overrides, bypass validation and directly update
            try:
                async with db_manager.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # Update based on whether it's a final status
                    if SignalStatus.is_final(new_status):
                        await conn.execute("""
                            UPDATE signals 
                            SET status = $1, updated_at = $2, closed_at = $3, closed_reason = $4
                            WHERE id = $5
                        """, new_status, now, now, 'manual', signal_id)
                    else:
                        # If reverting from final to non-final, clear closed_at
                        await conn.execute("""
                            UPDATE signals 
                            SET status = $1, updated_at = $2, closed_at = NULL, closed_reason = NULL
                            WHERE id = $3
                        """, new_status, now, signal_id)
                    # Record status change
                    await conn.execute("""
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """, signal_id, old_status, new_status, 'manual', reason or 'Manual override')
                    # Handle limits based on new status
                    if SignalStatus.is_final(new_status):
                        # Cancel any pending limits
                        await conn.execute("""
                            UPDATE limits 
                            SET status = 'cancelled' 
                            WHERE signal_id = $1 AND status = 'pending'
                        """, signal_id,)
                    elif new_status == SignalStatus.ACTIVE:
                        # If reverting to active, reactivate cancelled limits
                        await conn.execute("""
                            UPDATE limits 
                            SET status = 'pending' 
                            WHERE signal_id = $1 AND status = 'cancelled'
                        """, signal_id,)
                logger.info(f"Successfully set signal {signal_id} status: {old_status} -> {new_status}")
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

        Args:
            signal_id: Signal ID to mark as hit
            reason: Reason for manual hit (for audit trail)

        Returns:
            bool: True if successful
        """
        try:
            from database.models import SignalStatus

            # Get the signal with all its limits
            signal = await self.get_signal_with_limits(signal_id)

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            # Check if signal is in ACTIVE status
            if signal['status'] != SignalStatus.ACTIVE:
                logger.warning(f"Signal {signal_id} is not ACTIVE (status: {signal['status']})")
                # For non-active signals, just change the status directly
                return await self.manually_set_signal_status(signal_id, SignalStatus.HIT, reason)

            # Find the first pending limit (lowest sequence number)
            pending_limits = [
                l for l in signal.get('limits', [])
                if l.get('status') == 'pending'
            ]

            if not pending_limits:
                logger.warning(f"Signal {signal_id} has no pending limits")
                # No pending limits, just change status
                return await self.manually_set_signal_status(signal_id, SignalStatus.HIT, reason)

            # Sort by sequence number and get first
            first_limit = min(pending_limits, key=lambda l: l.get('sequence_number', 999))

            logger.info(
                f"Manually marking signal {signal_id} as HIT by hitting limit {first_limit['id']} "
                f"(price: {first_limit['price_level']})"
            )

            # Use the existing process_limit_hit method which:
            # - Marks limit as hit
            # - Sets hit_alert_sent = 1
            # - Updates signal status to HIT
            # - Sets first_limit_hit_time
            # - Records status change in audit
            result = await self.process_limit_hit(
                limit_id=first_limit['id'],
                hit_price=first_limit['price_level']  # Use limit price as hit price
            )

            if result and result.get('signal_id'):
                logger.info(f"Signal {signal_id} manually marked as HIT via limit hit")

                # Update the status change reason to reflect it was manual
                async with self.db.get_connection() as conn:
                    # Update the most recent status change reason
                    await conn.execute("""
                        UPDATE status_changes 
                        SET reason = $1, change_type = 'manual'
                        WHERE signal_id = $2 
                        AND new_status = 'hit'
                        AND id = (
                            SELECT MAX(id) FROM status_changes 
                            WHERE signal_id = $3 AND new_status = 'hit'
                        )
                    """, reason, signal_id, signal_id)
                return True
            else:
                logger.error(f"Failed to process limit hit for signal {signal_id}")
                return False

        except Exception as e:
            logger.error(f"Error manually setting signal {signal_id} to hit: {e}", exc_info=True)
            return False

    async def process_limit_hit(self, limit_id: int, actual_price: float, signal_db) -> Dict[str, Any]:
        """
        Process a limit hit event

        Args:
            limit_id: Limit ID that was hit
            actual_price: Actual price at hit (for spread tracking)
            signal_db: SignalDatabase instance for accessing methods

        Returns:
            Dict with signal info and status changes
        """
        # Mark limit as hit and get signal info
        result = await signal_db.db.mark_limit_hit(limit_id, actual_price)

        if result['signal_id']:
            # Get updated signal info
            signal = await signal_db.get_signal_with_limits(result['signal_id'])
            result['signal'] = signal

            # Check if all limits are hit
            if signal and len(signal['hit_limits']) == signal['total_limits']:
                result['all_limits_hit'] = True
                logger.info(f"All limits hit for signal {signal['id']}")
            else:
                result['all_limits_hit'] = False

        return result

    async def check_and_update_stop_loss(self, signal_id: int, current_price: float, signal_db) -> bool:
        """
        Check if stop loss is hit and update status

        Args:
            signal_id: Signal ID
            current_price: Current market price
            signal_db: SignalDatabase instance

        Returns:
            True if stop loss was hit
        """
        try:
            signal = await signal_db.db.fetch_one("""
                SELECT direction, stop_loss, status 
                FROM signals 
                WHERE id = $1
            """, signal_id,)
            if not signal or signal['status'] not in [SignalStatus.HIT]:
                return False

            stop_hit = False

            if signal['direction'] == 'long' and current_price <= signal['stop_loss']:
                stop_hit = True
            elif signal['direction'] == 'short' and current_price >= signal['stop_loss']:
                stop_hit = True

            if stop_hit:
                # Directly update to stop_loss status
                success = await self.manually_set_signal_status(
                    signal_id,
                    SignalStatus.STOP_LOSS,
                    f'Stop loss hit at {current_price}',
                    signal_db.db
                )

                if success:
                    logger.info(f"Signal {signal_id} hit stop loss at {current_price}")

                return success

            return False

        except Exception as e:
            logger.error(f"Error checking stop loss: {e}", exc_info=True)
            return False

    async def manually_set_signal_expiry(self, signal_id: int, expiry_type: str,
                                        custom_datetime: str = None, db_manager = None) -> bool:
        """
        Manually set a signal's expiry type and recalculate expiry time

        Args:
            signal_id: Signal ID
            expiry_type: New expiry type (day_end, week_end, month_end, no_expiry, custom)
            custom_datetime: Custom datetime string in ISO format (for custom type)
            db_manager: Database manager instance

        Returns:
            Success status
        """
        try:
            logger.debug(f"Manually setting signal {signal_id} expiry to {expiry_type}")

            # Validate expiry type
            valid_types = ['day_end', 'week_end', 'month_end', 'no_expiry', 'custom']
            if expiry_type not in valid_types:
                logger.error(f"Invalid expiry type: {expiry_type}")
                return False

            # If custom, validate datetime is provided
            if expiry_type == 'custom' and not custom_datetime:
                logger.error("Custom expiry type requires datetime")
                return False

            # Get current signal
            signal = await db_manager.fetch_one(
                "SELECT * FROM signals WHERE id = $1",
                (signal_id,)
            )

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            # Check if signal is in a final status
            if SignalStatus.is_final(signal['status']):
                logger.warning(f"Cannot modify expiry for signal {signal_id} in final status {signal['status']}")
                return False

            # Calculate new expiry time
            if expiry_type == 'custom':
                # Custom datetime is already in ISO format from the command
                new_expiry_time = custom_datetime
            else:
                from .utils import calculate_expiry
                new_expiry_time = calculate_expiry(expiry_type)

            # Update signal expiry
            try:
                async with db_manager.get_connection() as conn:
                    now = datetime.now(pytz.UTC)

                    # Update expiry type and time
                    await conn.execute("""
                        UPDATE signals 
                        SET expiry_type = $1, expiry_time = $2, updated_at = $3
                        WHERE id = $4
                    """, expiry_type, _parse_dt(new_expiry_time), now, signal_id)
                    # Record the change in status_changes table for audit
                    await conn.execute("""
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """, (signal_id, signal['status'], signal['status'], 'manual',
                         f'Expiry changed from {signal["expiry_type"]} to {expiry_type}'))

                # Log the change
                old_expiry = signal['expiry_type'] or 'none'
                if expiry_type == 'no_expiry':
                    logger.info(f"Removed expiry for signal {signal_id} (was {old_expiry})")
                elif expiry_type == 'custom':
                    logger.info(f"Set custom expiry for signal {signal_id} to {new_expiry_time}")
                else:
                    logger.info(f"Changed signal {signal_id} expiry from {old_expiry} to {expiry_type}")

                return True

            except Exception as e:
                logger.error(f"Database error setting expiry: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error manually setting signal expiry: {e}", exc_info=True)
            return False

    async def expire_old_signals(self, db_manager) -> int:
        """
        Check and expire signals past their expiry time

        Args:
            db_manager: Database manager instance

        Returns:
            Number of signals expired
        """
        # now = datetime.now(pytz.UTC)

        # Find expired signals
        query = """
            SELECT id, status FROM signals
            WHERE status IN ($1, $2)
            AND expiry_time IS NOT NULL
            AND expiry_time < CURRENT_TIMESTAMP
        """

        expired = await db_manager.fetch_all(
            query,
            (SignalStatus.ACTIVE, SignalStatus.HIT)
        )

        if not expired:
            return 0

        count = 0

        # Use direct database updates to avoid validation issues
        try:
            async with db_manager.get_connection() as conn:
                for signal in expired:
                    signal_id = signal['id']
                    old_status = signal['status']

                    await conn.execute("""
                        UPDATE signals
                        SET status = $1, updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP, closed_reason = $2
                        WHERE id = $3
                    """, SignalStatus.CANCELLED, 'automatic', signal_id)
                    # Record status change
                    await conn.execute("""
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """, signal_id, old_status, SignalStatus.CANCELLED, 'automatic', 'Expired')
                    # Cancel pending limits
                    await conn.execute("""
                        UPDATE limits
                        SET status = 'cancelled'
                        WHERE signal_id = $1 AND status = 'pending'
                    """, signal_id,)
                    count += 1

        except Exception as e:
            logger.error(f"Error expiring signals: {e}", exc_info=True)
            return 0

        if count > 0:
            logger.info(f"Expired {count} signals")

        return count