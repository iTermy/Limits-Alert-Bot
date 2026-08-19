"""
Signal-specific database operations — CRUD, lifecycle, and analytics
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import pytz

from core.parser import ParsedSignal
from models.enums import LimitStatus, SignalStatus
from models.signal import LimitData, SignalData
from utils.logger import get_logger

from .utils import _parse_dt, calculate_expiry, is_weekend_window

logger = get_logger("signal_db")

# Explicit column lists for single-signal fetches. Matches the SignalData /
# LimitData model fields; analytics-only columns (close_*, tp_threshold_*,
# minutes_to_news, data_version) are written but never read at runtime.
SIGNAL_COLUMNS = (
    "id, message_id, channel_id, instrument, direction, stop_loss, expiry_type, "
    "expiry_time, status, type, first_limit_hit_time, closed_at, closed_reason, "
    "take_profit, be_stop_armed_at, tp_price, manual_tp_price, total_limits, limits_hit, alert_message_id, "
    "alert_channel_id, ping_message_id, finished_message_id, finished_channel_id, "
    "created_at, updated_at"
)
LIMIT_COLUMNS = (
    "id, signal_id, price_level, sequence_number, status, hit_time, hit_price, "
    "approaching_alert_sent, hit_alert_sent, created_at"
)

# Entries an instant-entry signal may hold: the one it opens with, plus at most
# one averaged in later via the `add` reply.
MAX_INSTANT_ENTRIES = 2


def _is_crypto_symbol(symbol: str) -> bool:
    """Lightweight crypto detector for DB-layer use (avoids importing the live mapper)."""
    if not symbol:
        return False
    s = symbol.upper()
    if any(c in s for c in ("BTC", "ETH", "BNB", "XRP", "ADA", "DOGE", "SOL", "DOT")):
        return True
    return s.endswith(("USDT", "USDC"))


def _plan_limit_diff(existing_limits: list[dict[str, Any]], new_levels: list[float]) -> dict[str, Any]:
    """Plan how an edited limit list maps onto the existing rows.

    Matches edited levels to preservable rows (pending/hit) by exact price so
    unchanged levels keep their limit_id — the execution bot keys pending
    orders and filled positions off limit_id, so re-minting ids on every edit
    would force it to cancel and re-place untouched limits.

    Returns a dict with:
      plan:              [(sequence_number, price_level, matched_row_or_None)]
      matched_ids:       ids of rows kept in place
      removed_rows:      rows deleted by the edit
      hit_count:         how many kept rows are already hit
      alert_invalidated: True when a removed row had already fired an alert
                         (stale embed must be retracted by the caller)
    """
    preservable_by_price = defaultdict(list)
    for r in existing_limits:
        if r["status"] in ("pending", "hit"):
            preservable_by_price[r["price_level"]].append(r)

    plan = []
    matched_ids = set()
    hit_count = 0
    for idx, level in enumerate(new_levels):
        queue = preservable_by_price.get(level)
        matched = queue.pop(0) if queue else None
        if matched is not None:
            matched_ids.add(matched["id"])
            if matched["status"] == "hit":
                hit_count += 1
        plan.append((idx + 1, level, matched))

    removed_rows = [r for r in existing_limits if r["id"] not in matched_ids]
    alert_invalidated = any(
        r["status"] == "hit" or r["hit_alert_sent"] or r["approaching_alert_sent"]
        for r in removed_rows
    )

    return {
        "plan": plan,
        "matched_ids": matched_ids,
        "removed_rows": removed_rows,
        "hit_count": hit_count,
        "alert_invalidated": alert_invalidated,
    }


class SignalDatabase:
    """Handles all signal-specific database operations."""

    def __init__(self, db_manager):
        self.db = db_manager

    # === CRUD ===

    async def save_signal(
        self,
        parsed_signal: ParsedSignal,
        message_id: str,
        channel_id: str,
        context: Optional[dict[str, Any]] = None,
    ) -> tuple[bool, Optional[int]]:
        """
        Save a parsed signal to the database.

        `context` optionally carries save-time analysis stamps
        (tp_threshold_used, tp_threshold_unit, minutes_to_news).
        """
        try:
            expiry_time = calculate_expiry(parsed_signal.expiry_type)
            context = context or {}

            existing_id = None
            existing_status = None
            signal_id = None

            # An instant signal's limit is a fill that already happened, so the row is
            # born filled rather than being inserted pending and marked hit a moment
            # later. That gap was real — a pooler status write takes seconds — and for
            # its duration the signal was indistinguishable from an ordinary one
            # resting a limit at the market price, which is what execution clients
            # keying on l.status='pending' acted on.
            instant = parsed_signal.instant_entry
            now = datetime.now(pytz.UTC)

            async with self.db.get_connection() as conn:
                signal_id = await conn.fetchval(
                    """
                    INSERT INTO signals (
                        message_id, channel_id, instrument, direction,
                        stop_loss, expiry_type, expiry_time, total_limits, status, type,
                        take_profit, tp_threshold_used, tp_threshold_unit, minutes_to_news,
                        limits_hit, first_limit_hit_time
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
                    SignalStatus.HIT if instant else SignalStatus.ACTIVE,
                    getattr(parsed_signal, "type", "standard"),
                    parsed_signal.take_profit,
                    context.get("tp_threshold_used"),
                    context.get("tp_threshold_unit"),
                    context.get("minutes_to_news"),
                    1 if instant else 0,
                    now if instant else None,
                )

                if signal_id is None:
                    # Conflict: row already existed. Determine what to do.
                    row = await conn.fetchrow(
                        "SELECT id, status FROM signals WHERE message_id = $1", message_id
                    )
                    if row is not None:
                        existing_id = row["id"]
                        existing_status = row["status"]
                elif instant:
                    await conn.execute(
                        """
                        INSERT INTO limits (
                            signal_id, price_level, sequence_number, status,
                            hit_time, hit_price, hit_alert_sent
                        )
                        VALUES ($1, $2, 1, $3, $4, $2, TRUE)
                        """,
                        signal_id,
                        parsed_signal.limits[0],
                        LimitStatus.HIT,
                        now,
                    )
                elif parsed_signal.limits:
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

    async def get_signal_by_message_id(self, message_id: str) -> Optional[dict[str, Any]]:
        """Get signal by Discord message ID."""
        query = f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE message_id = $1"
        row = await self.db.fetch_one(query, (message_id,))
        if row is not None:
            row["signal_id"] = row["id"]
        return row

    async def get_signal_with_limits(self, signal_id: int) -> Optional[SignalData]:
        """Get signal with all its limits (including hit ones)."""
        signal_query = f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE id = $1"
        signal = await self.db.fetch_one(signal_query, (signal_id,))

        if not signal:
            return None

        limits_query = (
            f"SELECT {LIMIT_COLUMNS} FROM limits WHERE signal_id = $1 ORDER BY sequence_number"
        )
        limits = await self.db.fetch_all(limits_query, (signal_id,))

        return SignalData.from_db_row(signal, limits)

    async def update_signal_from_edit(
        self, message_id: str, parsed_signal: ParsedSignal
    ) -> tuple[bool, bool]:
        """Update an existing signal from an edited message.

        Returns (success, alert_invalidated). alert_invalidated is True when the edit
        removed a limit that had already fired an approaching/hit alert (e.g. a mistyped
        price being corrected), so the caller should retract the now-stale embed."""
        try:
            existing = await self.get_signal_by_message_id(message_id)
            if not existing:
                logger.warning(f"No signal found for message {message_id}")
                return False, False

            signal_id = existing["id"]

            truly_final = [s for s in SignalStatus.final_statuses() if s != SignalStatus.CANCELLED]
            if existing["status"] in truly_final:
                logger.warning(
                    f"Cannot update signal {signal_id} in final status {existing['status']}"
                )
                return False, False

            # Re-derive expiry_time only when the expiry_type actually changed,
            # so a same-type edit doesn't silently push the deadline out.
            new_expiry_time = existing.get("expiry_time")
            if parsed_signal.expiry_type != existing.get("expiry_type"):
                new_expiry_time = _parse_dt(calculate_expiry(parsed_signal.expiry_type))

            if parsed_signal.instant_entry:
                await self._update_instant_from_edit(
                    signal_id, existing, parsed_signal, new_expiry_time
                )
                return True, False

            async with self.db.get_connection() as conn:
                existing_limits = await conn.fetch(
                    "SELECT id, price_level, sequence_number, status, "
                    "approaching_alert_sent, hit_alert_sent FROM limits "
                    "WHERE signal_id = $1 ORDER BY sequence_number",
                    signal_id,
                )
                diff = _plan_limit_diff([dict(r) for r in existing_limits], parsed_signal.limits)
                await self._apply_limit_diff(conn, signal_id, diff)

                # A HIT signal whose hit limit(s) were edited away reverts to ACTIVE.
                new_status = existing["status"]
                if existing["status"] == SignalStatus.HIT and diff["hit_count"] == 0:
                    new_status = SignalStatus.ACTIVE

                await conn.execute(
                    """
                    UPDATE signals
                    SET instrument = $1, direction = $2, stop_loss = $3,
                        expiry_type = $4, expiry_time = $5, total_limits = $6,
                        type = $7, limits_hit = $8, status = $9,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $10
                    """,
                    parsed_signal.instrument,
                    parsed_signal.direction,
                    parsed_signal.stop_loss,
                    parsed_signal.expiry_type,
                    new_expiry_time,
                    len(parsed_signal.limits),
                    getattr(parsed_signal, "type", "standard"),
                    diff["hit_count"],
                    new_status,
                    signal_id,
                )

            logger.info(f"Updated signal {signal_id} from edited message")
            return True, diff["alert_invalidated"]

        except Exception as e:
            logger.error(f"Error updating signal from edit: {e}", exc_info=True)
            return False, False

    async def _update_instant_from_edit(
        self,
        signal_id: int,
        existing: dict[str, Any],
        parsed_signal: ParsedSignal,
        new_expiry_time: Any,
    ) -> None:
        """Apply an edit to an instant-entry signal: stop loss, take profit and
        expiry only.

        The entry limit records the market price at which the position was taken,
        so it is never re-derived from an edit — and with it the instrument and
        direction, which the entry price only means anything against.
        """
        if (
            parsed_signal.instrument != existing["instrument"]
            or parsed_signal.direction != existing["direction"]
        ):
            logger.warning(
                f"Edit to instant signal {signal_id} changes instrument/direction "
                f"({existing['instrument']} {existing['direction']} -> "
                f"{parsed_signal.instrument} {parsed_signal.direction}); "
                f"only SL/TP were applied"
            )

        async with self.db.get_connection() as conn:
            await conn.execute(
                """
                UPDATE signals
                SET stop_loss = $1, take_profit = $2, expiry_type = $3,
                    expiry_time = $4, updated_at = CURRENT_TIMESTAMP
                WHERE id = $5
                """,
                parsed_signal.stop_loss,
                parsed_signal.take_profit,
                parsed_signal.expiry_type,
                new_expiry_time,
                signal_id,
            )
        logger.info(f"Updated instant signal {signal_id} SL/TP from edited message")

    async def add_instant_entry(
        self, signal_id: int, entry_price: float, disarm_breakeven: bool = False
    ) -> Optional[int]:
        """Average an instant-entry signal in at the market, already filled.

        Returns the new limit's id, or None when the signal is no longer open or
        already holds the maximum number of entries. Both are read under a row lock
        on the signal: two `add` replies racing would otherwise each see one entry
        and each add one, and a signal auto-TP'd between the handler's read and this
        write would take a fill it can no longer act on — and count it in limits_hit.

        `disarm_breakeven` clears an armed breakeven stop in the same statement that
        records the fill, because the new average is what that stop would be measured
        against; see `_breakeven_disarm_note` for when the caller asks for it.
        """
        now = datetime.now(pytz.UTC)
        async with self.db.get_connection() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT total_limits, status FROM signals WHERE id = $1 FOR UPDATE",
                    signal_id,
                )
                if row is None or row["total_limits"] >= MAX_INSTANT_ENTRIES:
                    return None
                if row["status"] != SignalStatus.HIT:
                    logger.info(f"Signal {signal_id} is {row['status']} — no entry added")
                    return None

                sequence = row["total_limits"] + 1
                limit_id = await conn.fetchval(
                    """
                    INSERT INTO limits (
                        signal_id, price_level, sequence_number, status,
                        hit_time, hit_price, hit_alert_sent
                    )
                    VALUES ($1, $2, $3, $4, $5, $2, TRUE)
                    RETURNING id
                    """,
                    signal_id,
                    entry_price,
                    sequence,
                    LimitStatus.HIT,
                    now,
                )
                await conn.execute(
                    """
                    UPDATE signals
                    SET total_limits = $1, limits_hit = limits_hit + 1,
                        be_stop_armed_at = CASE WHEN $3 THEN NULL ELSE be_stop_armed_at END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2
                    """,
                    sequence,
                    signal_id,
                    disarm_breakeven,
                )

        logger.info(f"Added entry #{sequence} to instant signal {signal_id} at {entry_price}")
        if disarm_breakeven:
            logger.info(f"Signal {signal_id}: breakeven stop disarmed by the added entry")
        return limit_id

    @staticmethod
    async def _apply_limit_diff(conn, signal_id: int, diff: dict[str, Any]) -> None:
        """Apply a _plan_limit_diff result: delete removed rows, renumber kept
        rows, insert new levels, and reset alert flags when a stale alert was
        invalidated."""
        if diff["removed_rows"]:
            await conn.execute(
                "DELETE FROM limits WHERE id = ANY($1::bigint[])",
                [r["id"] for r in diff["removed_rows"]],
            )

        # Vacate the sequence space so kept rows can be renumbered without
        # tripping the (signal_id, sequence_number) unique constraint mid-update.
        kept_ids = list(diff["matched_ids"])
        if kept_ids:
            await conn.execute(
                "UPDATE limits SET sequence_number = sequence_number + 100000 "
                "WHERE id = ANY($1::bigint[])",
                kept_ids,
            )

        for seq, level, matched in diff["plan"]:
            if matched is not None:
                await conn.execute(
                    "UPDATE limits SET sequence_number = $1 WHERE id = $2",
                    seq,
                    matched["id"],
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO limits (signal_id, price_level, sequence_number, status)
                    VALUES ($1, $2, $3, 'pending')
                    """,
                    signal_id,
                    level,
                    seq,
                )

        # When a stale alert was cleared, let the surviving pending limits
        # re-alert by clearing their approaching flag (the shared embed is
        # retracted by the caller, so it must be re-established on re-approach).
        if diff["alert_invalidated"]:
            await conn.execute(
                "UPDATE limits SET approaching_alert_sent = FALSE "
                "WHERE signal_id = $1 AND status = 'pending'",
                signal_id,
            )

    async def get_signals_for_tracking(self) -> list[dict[str, Any]]:
        """Get all signals that need price tracking (wrapper for DB method)."""
        return await self.db.get_active_signals_for_tracking()

    async def get_hit_limits_for_signal(self, signal_id: int) -> list[LimitData]:
        """Return all hit limits for a signal with hit_price for P&L calculations."""
        return await self.db.get_hit_limits_for_signal(signal_id)

    async def get_active_message_ids_for_channel(self, channel_id: int) -> set:
        """Discord message_ids of ACTIVE+HIT signals in the given channel.
        Excludes manual_* ids (no Discord message exists)."""
        rows = await self.db.fetch_all(
            "SELECT message_id FROM signals "
            "WHERE channel_id = $1 AND status IN ('active', 'hit') "
            "AND message_id NOT LIKE 'manual_%'",
            (str(channel_id),),
        )
        return {row["message_id"] for row in rows}

    # === Lifecycle ===

    async def cancel_signal_by_message(self, message_id: str) -> bool:
        """Cancel a signal when its message is deleted or cancelled by user."""
        try:
            logger.debug(f"Starting cancel_signal_by_message for message {message_id}")

            row = await self.get_signal_by_message_id(message_id)
            if not row:
                logger.warning(f"No signal found for message {message_id}")
                return False

            logger.debug(f"Found signal {row['id']} with status {row['status']}")

            if row["status"] == SignalStatus.CANCELLED:
                logger.info(f"Signal {row['id']} is already cancelled")
                return True

            if (
                SignalStatus.is_final(row["status"])
                and row["status"] != SignalStatus.CANCELLED
            ):
                logger.warning(
                    f"Cannot cancel signal {row['id']} in final status {row['status']}"
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
                        row["id"],
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
                        row["id"],
                    )
                    await conn.execute(
                        """
                        INSERT INTO status_changes (signal_id, old_status, new_status, change_type, reason)
                        VALUES ($1, $2, $3, $4, $5)
                    """,
                        row["id"],
                        row["status"],
                        SignalStatus.CANCELLED,
                        "manual",
                        "User cancelled",
                    )
                    await self._snapshot_close_prices(conn, row["id"], row["instrument"])
                logger.info(f"Successfully cancelled signal {row['id']}")
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
        """Reactivate a cancelled or stopped-out signal as if the close never happened."""
        try:
            logger.debug(f"Attempting to reactivate signal {signal_id}")

            signal_query = f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE id = $1"
            row = await self.db.fetch_one(signal_query, (signal_id,))

            if not row:
                logger.error(f"Signal {signal_id} not found")
                return False

            reactivatable = (SignalStatus.CANCELLED, SignalStatus.STOP_LOSS)
            if row["status"] not in reactivatable:
                logger.warning(
                    f"Signal {signal_id} is not reactivatable, status: {row['status']}"
                )
                return False

            old_status = row["status"]
            new_status = (
                SignalStatus.HIT if row.get("limits_hit", 0) > 0 else SignalStatus.ACTIVE
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
                        old_status,
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
    ) -> list[SignalData]:
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
        return [SignalData.from_db_row(dict(r)) for r in rows]

    async def _get_live_price(self, instrument: str) -> Optional[dict[str, Any]]:
        """Fetch current bid/ask/feed from live_prices table."""
        row = await self.db.fetch_one(
            "SELECT bid, ask, feed FROM live_prices WHERE symbol = $1",
            (instrument.upper(),),
        )
        return dict(row) if row else None

    async def _snapshot_close_prices(
        self,
        conn,
        signal_id: int,
        instrument: str,
        price: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Record live bid/ask/feed on a signal at close time.

        Only applies to signals that entered a position (>=1 hit limit) — the
        snapshot gives every such close a mark-to-market exit price, including
        manual cancels and breakevens that otherwise record nothing.

        Runs on the caller's transaction connection rather than acquiring its
        own, so it costs statements instead of a second pool acquire plus its
        own BEGIN/COMMIT pair. Callers that already hold a live price (the
        manual-profit path, which derives tp_price from it) pass it in: that
        skips a redundant round-trip and makes close_bid/close_ask describe the
        same tick as tp_price.

        Failures are logged and swallowed: a missing snapshot must never break
        a status transition. Because the work shares the caller's transaction,
        it runs in a nested one — asyncpg maps that to a SAVEPOINT, so a failed
        statement rolls back the snapshot alone instead of poisoning the
        transaction and taking the status change down with it.
        """
        try:
            async with conn.transaction():
                if price is None:
                    has_hit = await conn.fetchval(
                        "SELECT 1 FROM limits WHERE signal_id = $1 AND status = 'hit' LIMIT 1",
                        signal_id,
                    )
                    if not has_hit:
                        return
                    row = await conn.fetchrow(
                        "SELECT bid, ask, feed FROM live_prices WHERE symbol = $1",
                        instrument.upper(),
                    )
                    price = dict(row) if row else None
                if not price or price.get("bid") is None or price.get("ask") is None:
                    logger.warning(
                        f"No live price for {instrument} — close snapshot skipped for signal {signal_id}"
                    )
                    return
                # The EXISTS clause carries the entered-position rule for callers
                # that supplied a price and so skipped the probe above.
                await conn.execute(
                    """
                    UPDATE signals
                    SET close_bid = $1, close_ask = $2, close_feed = $3
                    WHERE id = $4
                      AND EXISTS (
                          SELECT 1 FROM limits WHERE signal_id = $4 AND status = 'hit'
                      )
                    """,
                    float(price["bid"]),
                    float(price["ask"]),
                    price.get("feed"),
                    signal_id,
                )
        except Exception as e:
            logger.warning(f"Close-price snapshot failed for signal {signal_id}: {e}")

    async def check_reactivation_guard(self, signal_id: int) -> Optional[dict[str, Any]]:
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
        row = await self.db.fetch_one(
            "SELECT id, instrument, direction, status FROM signals WHERE id = $1",
            (signal_id,),
        )
        if not row or row["status"] not in (
            SignalStatus.CANCELLED,
            SignalStatus.STOP_LOSS,
        ):
            return None

        # These are limits that were pending at cancellation/stop-loss time and will
        # become pending again on reactivation (currently stored as 'cancelled').
        pending_limits = await self.db.fetch_all(
            """SELECT id, price_level, sequence_number
               FROM limits
               WHERE signal_id = $1 AND status = 'cancelled'
               ORDER BY sequence_number""",
            (signal_id,),
        )
        if not pending_limits:
            return None

        price_data = await self._get_live_price(row["instrument"])
        if not price_data or price_data.get("bid") is None or price_data.get("ask") is None:
            logger.warning(
                f"No live price for {row['instrument']} — reactivation guard skipped"
            )
            return None

        mid = (float(price_data["bid"]) + float(price_data["ask"])) / 2
        direction = row["direction"]

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
            "instrument": row["instrument"],
            "direction": direction,
        }

    async def set_breakeven_stop(self, signal_id: int, armed: bool) -> bool:
        """Arm or disarm a signal's breakeven stop. Returns True if a row changed.

        Only a signal holding an open position can carry one, so the update is
        scoped to status 'hit' — that also makes a late duplicate command against
        an already-closed signal a no-op rather than a resurrection.
        """
        try:
            updated = await self.db.execute(
                """
                UPDATE signals
                SET be_stop_armed_at = $1, updated_at = NOW()
                WHERE id = $2 AND status = 'hit'
                """,
                (datetime.now(pytz.UTC) if armed else None, signal_id),
            )
            return bool(updated)
        except Exception as e:
            logger.error(f"Failed to set breakeven stop on signal {signal_id}: {e}")
            return False

    async def manually_set_signal_status(
        self,
        signal_id: int,
        new_status: str,
        reason: Optional[str] = None,
        tp_price: Optional[float] = None,
        closed_reason: Optional[str] = None,
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

            row = await self.db.fetch_one(
                f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE id = $1", (signal_id,)
            )

            if not row:
                logger.error(f"Signal {signal_id} not found")
                return False

            old_status = row["status"]

            if old_status == new_status:
                logger.info(f"Signal {signal_id} already has status {new_status}")
                return True

            effective_closed_reason = closed_reason if closed_reason is not None else "manual"

            # Record the market price at the time of a manual profit so the DB always
            # captures the close price (auto-TP supplies its own; manual paths fall here).
            # Reused by the close snapshot below so the transition costs one price read.
            price_data = None
            if new_status == SignalStatus.PROFIT and tp_price is None:
                price_data = await self._get_live_price(row["instrument"])
                if price_data and price_data.get("bid") is not None and price_data.get("ask") is not None:
                    direction = (row["direction"] or "").lower()
                    tp_price = (
                        float(price_data["bid"])
                        if direction == "long"
                        else float(price_data["ask"])
                    )
                else:
                    logger.warning(
                        f"No live price for {row['instrument']} — tp_price will be NULL "
                        f"for manual profit on signal {signal_id}"
                    )

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
                    elif new_status in (SignalStatus.ACTIVE, SignalStatus.HIT):
                        # Restore limits that were cancelled by a prior final-status
                        # transition (e.g. stop_loss or manual cancel) so reactivation
                        # picks them back up as pending.
                        await conn.execute(
                            """
                            UPDATE limits
                            SET status = 'pending'
                            WHERE signal_id = $1 AND status = 'cancelled'
                        """,
                            signal_id,
                        )

                    if SignalStatus.is_final(new_status):
                        if tp_price is not None:
                            await conn.execute(
                                """
                                UPDATE signals
                                SET status = $1, updated_at = $2, closed_at = $3,
                                    closed_reason = $4, tp_price = $5
                                WHERE id = $6
                            """,
                                new_status,
                                now,
                                now,
                                effective_closed_reason,
                                tp_price,
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
                                closed_reason = NULL, tp_price = NULL
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
                    if SignalStatus.is_final(new_status):
                        await self._snapshot_close_prices(
                            conn, signal_id, row["instrument"], price_data
                        )
                logger.info(
                    f"Successfully set signal {signal_id} status: {old_status} -> {new_status}"
                    + (f" (tp_price={tp_price:.5f})" if tp_price is not None else "")
                )
                return True

            except Exception as e:
                logger.error(f"Database error setting status: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error manually setting signal status: {e}", exc_info=True)
            return False

    async def set_manual_tp_price(self, signal_id: int, manual_tp_price: float) -> bool:
        """Set the retrospective manual TP price for a signal.

        Stored in its own column so the original tp_price (the close price
        captured at profit time) is preserved. The report prefers this value
        over tp_price when computing P&L. Returns False if the signal is absent.
        """
        try:
            affected = await self.db.execute(
                "UPDATE signals SET manual_tp_price = $1, updated_at = $2 WHERE id = $3",
                (manual_tp_price, datetime.now(pytz.UTC), signal_id),
            )
            return affected > 0
        except Exception as e:
            logger.error(f"Error setting manual_tp_price for signal {signal_id}: {e}")
            return False

    async def manually_set_signal_to_hit(self, signal_id: int, reason: str) -> bool:
        """
        Manually mark a signal as HIT by marking its first pending limit as hit.
        This mimics the behavior of automatic hit detection.
        """
        try:
            signal_query = f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE id = $1"
            signal_row = await self.db.fetch_one(signal_query, (signal_id,))

            if not signal_row:
                logger.error(f"Signal {signal_id} not found")
                return False

            limits = await self.db.fetch_all(
                f"SELECT {LIMIT_COLUMNS} FROM limits WHERE signal_id = $1 ORDER BY sequence_number",
                (signal_id,),
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
                            closed_at = NULL, closed_reason = NULL, tp_price = NULL
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
                    f"SELECT {LIMIT_COLUMNS} FROM limits WHERE signal_id = $1 ORDER BY sequence_number",
                    (signal_id,),
                )

            elif signal_row["status"] != SignalStatus.ACTIVE:
                logger.warning(
                    f"Signal {signal_id} is not ACTIVE (status: {signal_row['status']}), cannot manually mark as HIT"
                )
                return False

            pending_limits = [lim for lim in limits if lim.get("status") == "pending"]

            if not pending_limits:
                logger.warning(
                    f"Signal {signal_id} has no pending limits, cannot manually mark as HIT"
                )
                return False

            first_limit = min(pending_limits, key=lambda lim: lim.get("sequence_number", 999))

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

    async def process_limit_hit(self, limit_id: int, actual_price: Optional[float] = None) -> dict[str, Any]:
        """Process a limit hit event."""
        result = await self.db.mark_limit_hit(limit_id, actual_price)

        if result["signal_id"]:
            signal = await self.get_signal_with_limits(result["signal_id"])
            result["signal"] = signal

            if signal and len(signal.hit_limits) == signal.total_limits:
                result["all_limits_hit"] = True
                logger.info(f"All limits hit for signal {signal.signal_id}")
            else:
                result["all_limits_hit"] = False

        return result

    async def manually_set_signal_expiry(
        self, signal_id: int, expiry_type: str, custom_datetime: Optional[str] = None
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

            row = await self.db.fetch_one(
                f"SELECT {SIGNAL_COLUMNS} FROM signals WHERE id = $1", (signal_id,)
            )

            if not row:
                logger.error(f"Signal {signal_id} not found")
                return False

            if SignalStatus.is_final(row["status"]):
                logger.warning(
                    f"Cannot modify expiry for signal {signal_id} in final status {row['status']}"
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
                        row["status"],
                        row["status"],
                        "manual",
                        f"Expiry changed from {row['expiry_type']} to {expiry_type}",
                    )

                old_expiry = row["expiry_type"] or "none"
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
            SELECT id, status, expiry_type, instrument FROM signals
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
        for row in expired:
            signal_id = row["id"]
            old_status = row["status"]
            instrument = row.get("instrument") or ""

            # HIT signals normally roll over to the next expiry window. Non-crypto
            # HIT signals heading into the weekend gap (Fri ≥ 4:45 PM or Sat/Sun)
            # are cancelled instead — markets are closed and the next rollover
            # would land on Saturday. Crypto runs 24/7 so weekend rollover is fine.
            should_rollover = (
                old_status == SignalStatus.HIT
                and not (is_weekend_window() and not _is_crypto_symbol(instrument))
            )

            if should_rollover:
                next_expiry = calculate_expiry(row["expiry_type"])
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
                        await self._snapshot_close_prices(conn, signal_id, instrument)
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
