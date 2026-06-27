"""
Signal excursion tracking.

For every signal it records, in-memory and per-tick, how far price travels in
its favour (MFE) and against it (MAE) — first through the approach phase, then
from the limit-hit entry to close — plus a one-shot market-context snapshot at
entry and a bounded per-minute volume/ATR time series. Results land in
signal_excursions / signal_volume_samples for later strategy analysis.

Pure data collection alongside AutoTPMonitor / TrailingStopMonitor: it never
touches alerts, embeds, or signal status. Per-tick cost is a couple of float
comparisons; bar fetches (context snapshot, volume sampling) run off the tick
path in executors or a slow background loop.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from price_feeds._base_config import BaseThresholdConfig

logger = logging.getLogger(__name__)

# Per-minute volume sampler cadence and how long after approach start we keep
# sampling — the edge plays out in minutes, so an hour-and-a-half cap keeps the
# time series bounded even for multi-day rollover signals.
_SAMPLE_INTERVAL_SECONDS = 60
_MAX_SAMPLE_WINDOW_SECONDS = 90 * 60


@dataclass
class ExcursionState:
    signal_id: int
    instrument: str
    direction: str  # "long" | "short"
    signal_type: str
    pip_size: float
    phase: str  # "approach" | "in_trade"

    approach_start_price: Optional[float] = None
    approach_start_time: Optional[datetime] = None
    pre_hit_mae_pips: float = 0.0

    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None
    atr_at_hit: Optional[float] = None

    mfe_pips: float = 0.0
    mae_pips: float = 0.0
    started_at: float = 0.0  # monotonic; bounds the volume sampler window


class ExcursionMonitor:
    """Tracks per-signal favourable/adverse excursions and market context."""

    def __init__(self, excursion_db, market_context):
        self._db = excursion_db
        self._context = market_context
        self._tracking: Dict[int, ExcursionState] = {}
        self._snapshot_tasks: set = set()
        self._running = True

    def is_tracking(self, signal_id: int) -> bool:
        return signal_id in self._tracking

    # === Lifecycle ===

    async def start_approach(self, signal: Dict, current_price: float) -> None:
        """Begin approach-phase tracking when the first approaching alert fires."""
        signal_id = signal["signal_id"]
        if signal_id in self._tracking:
            return

        now = datetime.now(timezone.utc)
        symbol = signal["instrument"]
        direction = signal["direction"].lower()
        signal_type = signal.get("type") or "standard"
        pip_size = BaseThresholdConfig.get_pip_size(symbol)

        self._tracking[signal_id] = ExcursionState(
            signal_id=signal_id,
            instrument=symbol,
            direction=direction,
            signal_type=signal_type,
            pip_size=pip_size,
            phase="approach",
            approach_start_price=current_price,
            approach_start_time=now,
            started_at=asyncio.get_event_loop().time(),
        )

        try:
            await self._db.start_approach(
                signal_id, symbol, direction, signal_type, pip_size, current_price, now
            )
        except Exception as e:
            logger.error("Failed to start approach excursion for %s: %s", signal_id, e)

    async def update_approach(self, signal: Dict, current_price: float) -> None:
        """Track the worst move away from the limit before it's hit."""
        state = self._tracking.get(signal["signal_id"])
        if state is None or state.phase != "approach":
            return

        # Adverse during approach = price drifting away from the (yet-unhit)
        # limit relative to where the approach began.
        if state.direction == "long":
            adverse = current_price - state.approach_start_price
        else:
            adverse = state.approach_start_price - current_price

        adverse_pips = max(0.0, adverse / state.pip_size)
        if adverse_pips > state.pre_hit_mae_pips:
            state.pre_hit_mae_pips = adverse_pips
            try:
                await self._db.update_pre_hit_mae(state.signal_id, adverse_pips)
            except Exception as e:
                logger.debug("pre_hit_mae update failed for %s: %s", state.signal_id, e)

    async def mark_entry(self, signal: Dict) -> None:
        """Transition to in_trade at the first hit limit's price. No-op if already entered."""
        signal_id = signal["signal_id"]
        state = self._tracking.get(signal_id)
        if state is not None and state.phase == "in_trade":
            return

        entry_price = self._first_hit_price(signal)
        if entry_price is None:
            return

        now = datetime.now(timezone.utc)
        symbol = signal["instrument"]
        direction = signal["direction"].lower()
        signal_type = signal.get("type") or "standard"
        pip_size = BaseThresholdConfig.get_pip_size(symbol)

        velocity = None
        pre_hit_mae = None
        if state is not None:
            pre_hit_mae = state.pre_hit_mae_pips
            if state.approach_start_time is not None:
                elapsed_min = (now - state.approach_start_time).total_seconds() / 60.0
                if elapsed_min > 0:
                    velocity = (
                        abs(entry_price - state.approach_start_price) / pip_size / elapsed_min
                    )

        self._tracking[signal_id] = ExcursionState(
            signal_id=signal_id,
            instrument=symbol,
            direction=direction,
            signal_type=signal_type,
            pip_size=pip_size,
            phase="in_trade",
            approach_start_price=state.approach_start_price if state else None,
            approach_start_time=state.approach_start_time if state else None,
            pre_hit_mae_pips=pre_hit_mae or 0.0,
            entry_price=entry_price,
            entry_time=now,
            started_at=state.started_at if state else asyncio.get_event_loop().time(),
        )

        try:
            await self._db.set_entry(
                signal_id, symbol, direction, signal_type, pip_size,
                entry_price, now, velocity, pre_hit_mae,
            )
        except Exception as e:
            logger.error("Failed to set excursion entry for %s: %s", signal_id, e)

        self._schedule_context_snapshot(signal_id, symbol, direction, signal.get("current_spread"))

    async def update(self, signal: Dict, bid: float, ask: float) -> None:
        """Per-tick MFE/MAE update for an in-trade signal."""
        state = self._tracking.get(signal["signal_id"])
        if state is None or state.phase != "in_trade":
            return

        close_price = bid if state.direction == "long" else ask
        if state.direction == "long":
            favorable = close_price - state.entry_price
            adverse = state.entry_price - close_price
        else:
            favorable = state.entry_price - close_price
            adverse = close_price - state.entry_price

        now = datetime.now(timezone.utc)
        fav_pips = favorable / state.pip_size
        if fav_pips > state.mfe_pips:
            state.mfe_pips = fav_pips
            try:
                await self._db.update_mfe(
                    state.signal_id, close_price, fav_pips, self._atr_mult(state, favorable), now
                )
            except Exception as e:
                logger.debug("mfe update failed for %s: %s", state.signal_id, e)

        adv_pips = adverse / state.pip_size
        if adv_pips > state.mae_pips:
            state.mae_pips = adv_pips
            try:
                await self._db.update_mae(
                    state.signal_id, close_price, adv_pips, self._atr_mult(state, adverse), now
                )
            except Exception as e:
                logger.debug("mae update failed for %s: %s", state.signal_id, e)

    async def finalize(self, signal_id: int, exit_price: float, reason: str) -> None:
        """Record the exit and stop tracking."""
        if signal_id not in self._tracking:
            return
        try:
            await self._db.finalize(signal_id, exit_price, datetime.now(timezone.utc), reason)
        except Exception as e:
            logger.error("Failed to finalize excursion for %s: %s", signal_id, e)
        self._tracking.pop(signal_id, None)

    def evict_signal(self, signal_id: int) -> None:
        self._tracking.pop(signal_id, None)

    async def resume_if_tracked(self, signal: Dict) -> None:
        """Re-hydrate in-memory excursion state from its open DB row after a restart."""
        signal_id = signal["signal_id"]
        if signal_id in self._tracking:
            return
        try:
            row = await self._db.get_excursion(signal_id)
        except Exception as e:
            logger.debug("excursion resume fetch failed for %s: %s", signal_id, e)
            return
        if not row:
            return

        self._tracking[signal_id] = ExcursionState(
            signal_id=signal_id,
            instrument=row["instrument"],
            direction=row["direction"].lower(),
            signal_type=row["signal_type"],
            pip_size=row["pip_size"],
            phase=row["phase"],
            approach_start_price=row["approach_start_price"],
            approach_start_time=row["approach_start_time"],
            pre_hit_mae_pips=row["pre_hit_mae"] or 0.0,
            entry_price=row["entry_price"],
            entry_time=row["entry_time"],
            atr_at_hit=row["atr_at_hit"],
            mfe_pips=row["mfe_pips"] or 0.0,
            mae_pips=row["mae_pips"] or 0.0,
            started_at=asyncio.get_event_loop().time(),
        )
        logger.info("Resumed excursion tracking for signal %s (%s)", signal_id, row["phase"])

    # === Volume sampler ===

    async def run_volume_sampler(self) -> None:
        """Per-minute volume/ATR sampling for every tracked signal within the window."""
        while self._running:
            await asyncio.sleep(_SAMPLE_INTERVAL_SECONDS)
            try:
                await self._sample_all()
            except Exception as e:
                logger.error("Volume sampler cycle failed: %s", e)

    def stop(self) -> None:
        self._running = False

    async def _sample_all(self) -> None:
        if not self._tracking:
            return
        now_mono = asyncio.get_event_loop().time()

        # One bar fetch per distinct instrument; reused across its signals.
        states = [
            s
            for s in self._tracking.values()
            if (now_mono - s.started_at) <= _MAX_SAMPLE_WINDOW_SECONDS
        ]
        samples: Dict[str, Optional[Dict]] = {}
        for state in states:
            if state.instrument not in samples:
                samples[state.instrument] = await self._context.sample_volume(state.instrument)
            sample = samples[state.instrument]
            if sample is None:
                continue
            try:
                await self._db.insert_volume_sample(
                    state.signal_id, state.phase, sample["price"], sample["volume"], sample["atr"]
                )
            except Exception as e:
                logger.debug("volume sample insert failed for %s: %s", state.signal_id, e)

    # === Helpers ===

    def _schedule_context_snapshot(
        self, signal_id: int, symbol: str, direction: str, spread: Optional[float]
    ) -> None:
        """Run the bar-derived context snapshot off the tick path."""
        task = asyncio.create_task(
            self._take_context_snapshot(signal_id, symbol, direction, spread)
        )
        self._snapshot_tasks.add(task)
        task.add_done_callback(self._snapshot_tasks.discard)

    async def _take_context_snapshot(
        self, signal_id: int, symbol: str, direction: str, spread: Optional[float]
    ) -> None:
        context = await self._context.snapshot(symbol, direction, spread)
        if context is None:
            return
        state = self._tracking.get(signal_id)
        if state is not None:
            state.atr_at_hit = context.get("atr_at_hit")
        try:
            await self._db.set_entry_context(signal_id, context)
        except Exception as e:
            logger.debug("entry context write failed for %s: %s", signal_id, e)

    @staticmethod
    def _atr_mult(state: ExcursionState, price_distance: float) -> Optional[float]:
        if not state.atr_at_hit:
            return None
        return price_distance / state.atr_at_hit

    @staticmethod
    def _first_hit_price(signal: Dict) -> Optional[float]:
        hit_limits = signal.get("hit_limits") or []
        if not hit_limits:
            return None
        first = sorted(hit_limits, key=lambda lim: lim["sequence_number"])[0]
        return first.get("hit_price") or first.get("price_level")
