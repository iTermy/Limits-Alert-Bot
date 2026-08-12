"""
Signal excursion database operations.

Pure backend analytics — never read by alerting or signal-lifecycle code.
Records per-signal MFE/MAE, approach behaviour, and the market context at
entry, plus a bounded per-minute volume/ATR time series. See ExcursionMonitor
for the in-memory side.
"""

from datetime import datetime
from typing import Any, Optional

from utils.logger import get_logger

logger = get_logger("excursion_db")


class ExcursionDatabase:
    """Handles all signal-excursion database operations."""

    def __init__(self, db_manager):
        self.db = db_manager

    async def start_approach(
        self,
        signal_id: int,
        instrument: str,
        direction: str,
        signal_type: str,
        pip_size: float,
        start_price: float,
        start_time: datetime,
    ) -> None:
        """Insert (or reset) the approach-phase row for a signal.

        A row can already exist if the signal re-approached after a retraction
        or was reactivated — upsert resets it to a fresh approach row so the new
        cycle isn't blocked by the primary key or polluted by stale extremes.
        """
        await self.db.execute(
            """
            INSERT INTO signal_excursions (
                signal_id, instrument, direction, signal_type, pip_size,
                phase, approach_start_price, approach_start_time
            )
            VALUES ($1, $2, $3, $4, $5, 'approach', $6, $7)
            ON CONFLICT (signal_id) DO UPDATE SET
                instrument = EXCLUDED.instrument,
                direction = EXCLUDED.direction,
                signal_type = EXCLUDED.signal_type,
                pip_size = EXCLUDED.pip_size,
                phase = 'approach',
                approach_start_price = EXCLUDED.approach_start_price,
                approach_start_time = EXCLUDED.approach_start_time,
                approach_velocity = NULL,
                pre_hit_mae = NULL,
                entry_price = NULL, entry_time = NULL,
                mfe_price = NULL, mfe_pips = NULL, mfe_atr_mult = NULL, mfe_time = NULL,
                mae_price = NULL, mae_pips = NULL, mae_atr_mult = NULL, mae_time = NULL,
                exit_price = NULL, exit_time = NULL, exit_reason = NULL
            """,
            (signal_id, instrument, direction, signal_type, pip_size, start_price, start_time),
        )

    async def update_pre_hit_mae(self, signal_id: int, pre_hit_mae: float) -> None:
        await self.db.execute(
            "UPDATE signal_excursions SET pre_hit_mae = $1 WHERE signal_id = $2 AND phase = 'approach'",
            (pre_hit_mae, signal_id),
        )

    async def set_entry(
        self,
        signal_id: int,
        instrument: str,
        direction: str,
        signal_type: str,
        pip_size: float,
        entry_price: float,
        entry_time: datetime,
        approach_velocity: Optional[float],
        pre_hit_mae: Optional[float],
    ) -> None:
        """Transition to in_trade, seeding MFE/MAE at the entry price.

        Inserts when no approach row exists (e.g. a manually-set HIT that never
        approached) so excursion tracking still starts at entry.
        """
        await self.db.execute(
            """
            INSERT INTO signal_excursions (
                signal_id, instrument, direction, signal_type, pip_size,
                phase, entry_price, entry_time, approach_velocity, pre_hit_mae,
                mfe_price, mfe_pips, mfe_atr_mult, mfe_time,
                mae_price, mae_pips, mae_atr_mult, mae_time
            )
            VALUES ($1, $2, $3, $4, $5, 'in_trade', $6, $7, $8, $9,
                    $6, 0, 0, $7, $6, 0, 0, $7)
            ON CONFLICT (signal_id) DO UPDATE SET
                phase = 'in_trade',
                entry_price = EXCLUDED.entry_price,
                entry_time = EXCLUDED.entry_time,
                approach_velocity = EXCLUDED.approach_velocity,
                pre_hit_mae = EXCLUDED.pre_hit_mae,
                mfe_price = EXCLUDED.entry_price, mfe_pips = 0, mfe_atr_mult = 0,
                mfe_time = EXCLUDED.entry_time,
                mae_price = EXCLUDED.entry_price, mae_pips = 0, mae_atr_mult = 0,
                mae_time = EXCLUDED.entry_time
            """,
            (
                signal_id, instrument, direction, signal_type, pip_size,
                entry_price, entry_time, approach_velocity, pre_hit_mae,
            ),
        )

    async def set_entry_context(self, signal_id: int, context: dict[str, Any]) -> None:
        """Fill the market-context columns sampled at entry (bar-derived)."""
        await self.db.execute(
            """
            UPDATE signal_excursions SET
                atr_at_hit = $1, rsi_at_hit = $2, ema_distance_atr = $3,
                wick_rejection = $4, htf_trend = $5, htf_aligned = $6,
                volume_at_hit = $7, avg_volume = $8, volume_spike_ratio = $9,
                spread_at_hit = $10, session = $11
            WHERE signal_id = $12
            """,
            (
                context.get("atr_at_hit"),
                context.get("rsi_at_hit"),
                context.get("ema_distance_atr"),
                context.get("wick_rejection"),
                context.get("htf_trend"),
                context.get("htf_aligned"),
                context.get("volume_at_hit"),
                context.get("avg_volume"),
                context.get("volume_spike_ratio"),
                context.get("spread_at_hit"),
                context.get("session"),
                signal_id,
            ),
        )

    async def update_mfe(
        self, signal_id: int, price: float, pips: float, atr_mult: Optional[float], ts: datetime
    ) -> None:
        await self.db.execute(
            """
            UPDATE signal_excursions
            SET mfe_price = $1, mfe_pips = $2, mfe_atr_mult = $3, mfe_time = $4
            WHERE signal_id = $5 AND phase = 'in_trade'
            """,
            (price, pips, atr_mult, ts, signal_id),
        )

    async def update_mae(
        self, signal_id: int, price: float, pips: float, atr_mult: Optional[float], ts: datetime
    ) -> None:
        await self.db.execute(
            """
            UPDATE signal_excursions
            SET mae_price = $1, mae_pips = $2, mae_atr_mult = $3, mae_time = $4
            WHERE signal_id = $5 AND phase = 'in_trade'
            """,
            (price, pips, atr_mult, ts, signal_id),
        )

    async def finalize(
        self,
        signal_id: int,
        exit_price: float,
        exit_time: datetime,
        exit_reason: str,
        mfe_pips: Optional[float] = None,
        mfe_price: Optional[float] = None,
        mfe_time: Optional[datetime] = None,
        mae_pips: Optional[float] = None,
        mae_price: Optional[float] = None,
        mae_time: Optional[datetime] = None,
    ) -> None:
        """Close the row, keeping the larger of the stored and caller's extremes.

        The caller passes the extremes it observed in memory. Per-tick ratchet
        writes are best-effort, so the stored value can lag; GREATEST repairs it
        here, and the matching price/time move only when the caller's excursion
        is the bigger one (every SET reads the pre-UPDATE row, so the comparison
        against the old pips value is sound). All extreme arguments may be None
        — GREATEST skips NULLs and the CASE falls through to the stored value.
        """
        await self.db.execute(
            """
            UPDATE signal_excursions
            SET phase = 'closed', exit_price = $1, exit_time = $2, exit_reason = $3,
                mfe_price = CASE WHEN $4 > COALESCE(mfe_pips, 0) THEN $5 ELSE mfe_price END,
                mfe_time  = CASE WHEN $4 > COALESCE(mfe_pips, 0) THEN $6 ELSE mfe_time END,
                mfe_pips  = GREATEST(mfe_pips, $4),
                mae_price = CASE WHEN $7 > COALESCE(mae_pips, 0) THEN $8 ELSE mae_price END,
                mae_time  = CASE WHEN $7 > COALESCE(mae_pips, 0) THEN $9 ELSE mae_time END,
                mae_pips  = GREATEST(mae_pips, $7)
            WHERE signal_id = $10 AND phase <> 'closed'
            """,
            (
                exit_price, exit_time, exit_reason,
                mfe_pips, mfe_price, mfe_time,
                mae_pips, mae_price, mae_time,
                signal_id,
            ),
        )

    async def close_orphaned(self) -> int:
        """Close excursion rows whose signal has already reached a final status.

        Every close path finalizes its own row with the tick price that closed
        the trade; this is the safety net for the closes that have no tick price
        to offer — expiry, message deletion, or a manual command issued after the
        signal left the monitor's in-memory set. Without it those rows sit in
        'approach'/'in_trade' forever and silently drop out of any analysis that
        filters on a recorded exit.

        The derived exit follows the same rules as the analysis exit price:
        stop-losses take the stop level, everything else prefers tp_price and
        falls back to the close-snapshot mid. exit_reason carries a ':reconciled'
        suffix so a derived exit is never mistaken for an observed one.

        Returns the number of rows closed.
        """
        return await self.db.execute(
            """
            UPDATE signal_excursions e
            SET phase = 'closed',
                exit_price = COALESCE(
                    e.exit_price,
                    CASE
                        WHEN s.status = 'stop_loss' THEN s.stop_loss
                        ELSE COALESCE(s.tp_price, (s.close_bid + s.close_ask) / 2)
                    END
                ),
                exit_time = COALESCE(e.exit_time, s.closed_at, NOW()),
                exit_reason = COALESCE(s.closed_reason, s.status) || ':reconciled'
            FROM signals s
            WHERE s.id = e.signal_id
              AND e.phase <> 'closed'
              AND s.status IN ('profit', 'breakeven', 'stop_loss', 'cancelled')
            """
        )

    async def set_mae_before_mfe(self, signal_id: int, mae_first: bool) -> None:
        """One-shot ordering verdict; never overwrites an existing value."""
        await self.db.execute(
            """
            UPDATE signal_excursions
            SET mae_before_mfe = $1
            WHERE signal_id = $2 AND mae_before_mfe IS NULL
            """,
            (mae_first, signal_id),
        )

    async def update_post_exit_mfe(self, signal_id: int, pips: float) -> None:
        await self.db.execute(
            "UPDATE signal_excursions SET post_exit_mfe_pips = $1 WHERE signal_id = $2",
            (pips, signal_id),
        )

    async def set_post_exit_end(self, signal_id: int, end_time: datetime) -> None:
        await self.db.execute(
            "UPDATE signal_excursions SET post_exit_end_time = $1 WHERE signal_id = $2",
            (end_time, signal_id),
        )

    async def insert_volume_sample(
        self,
        signal_id: int,
        phase: str,
        price: Optional[float],
        volume: Optional[float],
        atr: Optional[float],
    ) -> None:
        await self.db.execute(
            """
            INSERT INTO signal_volume_samples (signal_id, phase, price, volume, atr)
            VALUES ($1, $2, $3, $4, $5)
            """,
            (signal_id, phase, price, volume, atr),
        )

    async def get_excursion(self, signal_id: int) -> Optional[dict[str, Any]]:
        """Fetch an open excursion row for restart resume; None if absent/closed."""
        return await self.db.fetch_one(
            """
            SELECT signal_id, instrument, direction, signal_type, pip_size, phase,
                   approach_start_price, approach_start_time, pre_hit_mae,
                   entry_price, entry_time, atr_at_hit,
                   mfe_price, mfe_pips, mae_price, mae_pips, mae_before_mfe
            FROM signal_excursions
            WHERE signal_id = $1 AND phase <> 'closed'
            """,
            (signal_id,),
        )
