"""
Signal excursion database operations.

Pure backend analytics — never read by alerting or signal-lifecycle code.
Records per-signal MFE/MAE, approach behaviour, and the market context at
entry, plus a bounded per-minute volume/ATR time series. See ExcursionMonitor
for the in-memory side.
"""

from datetime import datetime
from typing import Any, Dict, Optional

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

    async def set_entry_context(self, signal_id: int, context: Dict[str, Any]) -> None:
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
        self, signal_id: int, exit_price: float, exit_time: datetime, exit_reason: str
    ) -> None:
        await self.db.execute(
            """
            UPDATE signal_excursions
            SET phase = 'closed', exit_price = $1, exit_time = $2, exit_reason = $3
            WHERE signal_id = $4 AND phase <> 'closed'
            """,
            (exit_price, exit_time, exit_reason, signal_id),
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

    async def get_excursion(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """Fetch an open excursion row for restart resume; None if absent/closed."""
        return await self.db.fetch_one(
            """
            SELECT signal_id, instrument, direction, signal_type, pip_size, phase,
                   approach_start_price, approach_start_time, pre_hit_mae,
                   entry_price, entry_time, atr_at_hit,
                   mfe_price, mfe_pips, mae_price, mae_pips
            FROM signal_excursions
            WHERE signal_id = $1 AND phase <> 'closed'
            """,
            (signal_id,),
        )
