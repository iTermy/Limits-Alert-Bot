"""
Trailing-stop shadow simulation database operations.

Pure data logging for later analysis — never read by alerting or signal
lifecycle code. See TrailingStopMonitor for the in-memory side.
"""

from datetime import datetime
from typing import Any, Dict, List

from utils.logger import get_logger

logger = get_logger("trailing_db")


class TrailingDatabase:
    """Handles all trailing-stop-simulation database operations."""

    def __init__(self, db_manager):
        self.db = db_manager

    async def create_tracking_rows(self, signal_id: int, levels: Dict[str, Dict[str, Any]]) -> None:
        """Insert one row per level for a signal that just went HIT.

        levels: {level_name: {distance_value, distance_type, anchor_price}}
        """
        for level, cfg in levels.items():
            await self.db.execute(
                """
                INSERT INTO trailing_simulations (
                    signal_id, level, distance_value, distance_type,
                    anchor_price, high_water_mark, hwm_updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $5, NOW())
                ON CONFLICT (signal_id, level) DO NOTHING
                """,
                (
                    signal_id,
                    level,
                    cfg["distance_value"],
                    cfg["distance_type"],
                    cfg["anchor_price"],
                ),
            )

    async def update_high_water_mark(
        self, signal_id: int, level: str, price: float, ts: datetime
    ) -> None:
        await self.db.execute(
            """
            UPDATE trailing_simulations
            SET high_water_mark = $1, hwm_updated_at = $2
            WHERE signal_id = $3 AND level = $4 AND stopped_out = FALSE
            """,
            (price, ts, signal_id, level),
        )

    async def finalize_level(
        self,
        signal_id: int,
        level: str,
        stop_price: float,
        stop_time: datetime,
        stop_reason: str,
        pnl_at_stop: float,
    ) -> None:
        await self.db.execute(
            """
            UPDATE trailing_simulations
            SET stopped_out = TRUE, stop_price = $1, stop_time = $2,
                stop_reason = $3, pnl_at_stop = $4
            WHERE signal_id = $5 AND level = $6 AND stopped_out = FALSE
            """,
            (stop_price, stop_time, stop_reason, pnl_at_stop, signal_id, level),
        )

    async def get_open_levels_for_signal(self, signal_id: int) -> List[str]:
        rows = await self.db.fetch_all(
            "SELECT level FROM trailing_simulations WHERE signal_id = $1 AND stopped_out = FALSE",
            (signal_id,),
        )
        return [r["level"] for r in rows]

    async def get_incomplete_shadow_signals(self) -> List[Dict[str, Any]]:
        """Signals closed via auto-TP that still have at least one open trail level.

        Used at startup to re-hydrate shadow tracking after a restart.
        Returns one dict per signal with a nested "levels" list.
        """
        rows = await self.db.fetch_all(
            """
            SELECT s.id AS signal_id, s.instrument, s.direction, s.type,
                   t.level, t.distance_value, t.distance_type,
                   t.anchor_price, t.high_water_mark, t.stopped_out
            FROM signals s
            JOIN trailing_simulations t ON t.signal_id = s.id
            WHERE s.status = 'profit' AND s.closed_reason = 'automatic'
              AND s.id IN (
                  SELECT signal_id FROM trailing_simulations WHERE stopped_out = FALSE
              )
            ORDER BY s.id, t.level
            """
        )

        signals: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            signal_id = row["signal_id"]
            if signal_id not in signals:
                signals[signal_id] = {
                    "signal_id": signal_id,
                    "instrument": row["instrument"],
                    "direction": row["direction"],
                    "type": row["type"],
                    "levels": [],
                }
            signals[signal_id]["levels"].append(
                {
                    "level": row["level"],
                    "distance_value": row["distance_value"],
                    "distance_type": row["distance_type"],
                    "anchor_price": row["anchor_price"],
                    "high_water_mark": row["high_water_mark"],
                    "stopped_out": row["stopped_out"],
                }
            )
        return list(signals.values())
