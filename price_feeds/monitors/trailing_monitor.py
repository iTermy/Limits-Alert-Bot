"""
Trailing-stop shadow simulation.

Once a signal reaches its auto-TP threshold, runs three parallel what-if
trailing stops (tight/medium/loose) entirely in-memory and logs results to
trailing_simulations for later analysis. The trail begins at the TP price —
the what-if it answers is "instead of closing at the fixed auto-TP, what if I
had trailed the runner?" — with P&L anchored to the deepest hit limit. Never
touches alerts, embeds, or signal status; pure data collection alongside the
real AutoTPMonitor.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from database.trailing_ops import TrailingDatabase
from models.signal import SignalData

from price_feeds.config.trailing_config import LEVELS, TrailingStopConfig

logger = logging.getLogger(__name__)


@dataclass
class TrailingState:
    signal_id: int
    instrument: str
    direction: str
    signal_type: str
    distance_type: str  # "pips" | "dollars"
    pip_size: float
    anchor_price: float
    high_water_mark: dict[str, float]
    stopped: dict[str, bool] = field(default_factory=dict)
    distance_raw: dict[str, float] = field(default_factory=dict)  # stop distance in price units


class TrailingStopMonitor:
    """Tracks per-signal high-water-mark trailing simulations."""

    def __init__(self, trailing_config: TrailingStopConfig, trailing_db: TrailingDatabase):
        self._config = trailing_config
        self._db = trailing_db
        self._tracking: dict[int, TrailingState] = {}

    def has_open_levels(self, signal_id: int) -> bool:
        state = self._tracking.get(signal_id)
        return state is not None and not all(state.stopped.values())

    async def start(self, signal: dict, tp_price: float) -> None:
        """Begin shadow tracking when a signal reaches its auto-TP threshold.

        The trail starts at `tp_price` (the price at the TP trigger), so the
        high-water-mark ratchets up from there. P&L is anchored to the deepest
        hit limit — the runner the strategy trails — so `pnl_at_stop` is the
        full trade result from entry, comparable to the fixed-TP outcome.
        """
        signal_id = signal.signal_id
        if signal_id in self._tracking:
            return

        hit_limits = signal.hit_limits
        if not hit_limits:
            return

        direction = signal.direction.lower()
        anchor_price = self._deepest_hit_price(hit_limits, direction)

        symbol = signal.instrument
        signal_type = signal.type

        levels = self._config.get_levels(symbol, signal_type)
        distance_type = self._config.get_distance_type(symbol, signal_type)
        pip_size = self._config.get_pip_size(symbol) if distance_type == "pips" else 1.0
        distance_raw = {level: levels[level] * pip_size for level in LEVELS}

        self._tracking[signal_id] = TrailingState(
            signal_id=signal_id,
            instrument=symbol,
            direction=direction,
            signal_type=signal_type,
            distance_type=distance_type,
            pip_size=pip_size,
            anchor_price=anchor_price,
            high_water_mark={level: tp_price for level in LEVELS},
            stopped={level: False for level in LEVELS},
            distance_raw=distance_raw,
        )

        await self._db.create_tracking_rows(
            signal_id,
            {
                level: {
                    "distance_value": levels[level],
                    "distance_type": distance_type,
                    "anchor_price": anchor_price,
                    "high_water_mark": tp_price,
                }
                for level in LEVELS
            },
        )

    async def update(self, signal: dict, bid: float, ask: float) -> bool:
        """Advance shadow tracking on a price tick. Returns True once every level has stopped out."""
        signal_id = signal.signal_id
        state = self._tracking.get(signal_id)
        if state is None:
            return False

        close_price = bid if state.direction == "long" else ask
        now = datetime.now(timezone.utc)

        for level in LEVELS:
            if state.stopped[level]:
                continue

            hwm = state.high_water_mark[level]
            if state.direction == "long":
                if close_price > hwm:
                    hwm = close_price
                    state.high_water_mark[level] = hwm
                    await self._db.update_high_water_mark(signal_id, level, hwm, now)
                triggered = close_price <= hwm - state.distance_raw[level]
            else:
                if close_price < hwm:
                    hwm = close_price
                    state.high_water_mark[level] = hwm
                    await self._db.update_high_water_mark(signal_id, level, hwm, now)
                triggered = close_price >= hwm + state.distance_raw[level]

            if triggered:
                state.stopped[level] = True
                pnl = self._pnl(state, close_price)
                await self._db.finalize_level(signal_id, level, close_price, now, "trail_triggered", pnl)

        if all(state.stopped.values()):
            self._tracking.pop(signal_id, None)
            return True
        return False

    @staticmethod
    def _deepest_hit_price(hit_limits: list[dict], direction: str) -> float:
        """Anchor on the deepest fill — the runner the TP strategy trails.

        Long: lowest hit price; short: highest. Earlier (shallower) limits
        exit at breakeven, so both P&L and the trail are measured from the
        deepest entry rather than the first one hit.
        """
        prices = [lim.hit_price or lim.price_level for lim in hit_limits]
        return min(prices) if direction == "long" else max(prices)

    async def finalize_with_price(self, signal_id: int, price: float, reason: str) -> None:
        """Close out any still-open levels at `price` (real SL / manual close / cancel / expiry)."""
        state = self._tracking.get(signal_id)
        if state is None:
            return

        now = datetime.now(timezone.utc)
        pnl = self._pnl(state, price)
        for level in LEVELS:
            if state.stopped[level]:
                continue
            state.stopped[level] = True
            await self._db.finalize_level(signal_id, level, price, now, reason, pnl)

    def evict_signal(self, signal_id: int) -> None:
        self._tracking.pop(signal_id, None)

    async def recover_incomplete_signals(self) -> list[SignalData]:
        """Re-hydrate shadow tracking after a restart.

        Returns lightweight ``SignalData`` objects (marked ``shadow_only``)
        ready for the caller to insert into its own active-signal/symbol
        routing.
        """
        shadow_signals = await self._db.get_incomplete_shadow_signals()
        restored = []
        for shadow in shadow_signals:
            self.restore(shadow)
            restored.append(
                SignalData(
                    signal_id=shadow["signal_id"],
                    instrument=shadow["instrument"],
                    direction=shadow["direction"],
                    type=shadow.get("type") or "standard",
                    status="profit",
                    shadow_only=True,
                )
            )
        if restored:
            logger.info(f"Recovered {len(restored)} shadow trailing simulation(s) after restart")
        return restored

    def restore(self, shadow_signal: dict) -> None:
        """Rehydrate in-memory state for a signal whose shadow tracking survived a restart."""
        signal_id = shadow_signal["signal_id"]
        symbol = shadow_signal["instrument"]
        direction = shadow_signal["direction"].lower()

        high_water_mark: dict[str, float] = {}
        stopped: dict[str, bool] = {}
        distance_raw: dict[str, float] = {}
        distance_type: Optional[str] = None
        pip_size = 1.0
        anchor_price = 0.0

        for lvl in shadow_signal["levels"]:
            level = lvl["level"]
            distance_type = lvl["distance_type"]
            anchor_price = lvl["anchor_price"]
            high_water_mark[level] = lvl["high_water_mark"]
            stopped[level] = lvl["stopped_out"]
            pip_size = self._config.get_pip_size(symbol) if distance_type == "pips" else 1.0
            distance_raw[level] = (
                lvl["distance_value"] * pip_size if distance_type == "pips" else lvl["distance_value"]
            )

        self._tracking[signal_id] = TrailingState(
            signal_id=signal_id,
            instrument=symbol,
            direction=direction,
            signal_type=shadow_signal.get("type") or "standard",
            distance_type=distance_type or "dollars",
            pip_size=pip_size,
            anchor_price=anchor_price,
            high_water_mark=high_water_mark,
            stopped=stopped,
            distance_raw=distance_raw,
        )
        logger.info(f"Restored shadow trailing tracking for signal {signal_id}")

    @staticmethod
    def _pnl(state: TrailingState, exit_price: float) -> float:
        raw_diff = (
            exit_price - state.anchor_price
            if state.direction == "long"
            else state.anchor_price - exit_price
        )
        if state.distance_type == "pips":
            return raw_diff / state.pip_size
        return raw_diff
