"""
Auto Take-Profit Monitor

Evaluates TP conditions on every price tick for signals that have at least
one limit hit (status = HIT).

Logic per price tick:
  - Identify the "last" hit limit (highest sequence_number).
  - Calculate its P&L from the limit price → current price.
  - If only 1 limit is hit: last_pnl >= tp_threshold → auto-profit.
  - If 2+ limits hit: last_pnl >= tp_threshold AND sum(pnl of all others) >= 0.

P&L is in native units (pips for forex, dollars for everything else).
Uses bid price for long P&L (what you could close at), ask for short.
"""

import asyncio
from typing import Dict, List

from models.signal import LimitData, SignalData
from utils.logger import get_logger

logger = get_logger("tp_monitor")


class AutoTPMonitor:
    """
    Evaluates auto take-profit conditions on every price tick.
    Integrated into StreamingPriceMonitor via _check_signal().

    All state is kept in-memory; hit limits are fetched from DB once
    per limit-hit event and cached until the signal closes.
    """

    def __init__(self, tp_config, signal_db, db, alert_system=None):
        """
        Args:
            tp_config:    TPConfig instance
            signal_db:    SignalDatabase instance
            db:           DatabaseManager instance
            alert_system: AlertSystem instance for sending Discord alerts
        """
        self.tp_config = tp_config
        self.signal_db = signal_db
        self.db = db
        self.alert_system = alert_system

        # signal_id -> hit limits (with hit_price, ordered by sequence_number)
        self._hit_limits_cache: Dict[int, List[LimitData]] = {}

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    async def refresh_hit_limits(self, signal_id: int):
        """
        (Re)load hit limits from DB for a signal.
        Call this immediately after a limit is marked hit.
        """
        try:
            limits = await self.signal_db.get_hit_limits_for_signal(signal_id)
            self._hit_limits_cache[signal_id] = limits
        except Exception as e:
            logger.error(f"Failed to refresh hit limits cache for {signal_id}: {e}")

    def evict_signal(self, signal_id: int):
        """Remove a signal from the cache (call when signal closes)."""
        self._hit_limits_cache.pop(signal_id, None)

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    async def check_signal(
        self,
        signal: SignalData,
        current_bid: float,
        current_ask: float,
    ) -> bool:
        """
        Evaluate TP conditions for a signal on a price tick.

        Returns:
            True if TP was triggered (signal marked as profit), else False.
        """
        signal_id = signal.signal_id
        instrument = signal.instrument
        direction = signal.direction.lower()

        hit_limits = self._hit_limits_cache.get(signal_id)
        if not hit_limits:
            return False

        # Guard: re-fetch current signal status before doing any TP evaluation.
        # If the signal was manually closed (profit, cancelled, stop_loss, breakeven)
        # between ticks, evict it from cache and abort — prevents double-marking.
        try:
            current = await self.signal_db.get_signal_with_limits(signal_id)
            if current and current.status not in ("hit", "active"):
                logger.info(
                    f"Signal {signal_id}: status is '{current.status}', "
                    f"skipping auto-TP and evicting from cache"
                )
                self.evict_signal(signal_id)
                return False
        except Exception as e:
            logger.warning(f"Signal {signal_id}: could not verify status before auto-TP: {e}")

        num_hit = len(hit_limits)

        # Separate last vs earlier limits (ordered by sequence_number)
        last_limit = hit_limits[-1]
        earlier_limits = hit_limits[:-1]

        # Long: close at bid; Short: close at ask
        close_price = current_bid if direction == "long" else current_ask

        signal_type = signal.type
        last_pnl = self.tp_config.calculate_pnl(
            instrument, direction, last_limit.price_level, close_price, signal_type=signal_type
        )
        tp_threshold = self.tp_config.get_tp_value(instrument, signal_type=signal_type)

        # Tiny epsilon to guard against floating-point rounding errors
        EPSILON = 1e-9

        # Last limit must clear the TP threshold
        if last_pnl < tp_threshold - EPSILON:
            return False

        # If there are earlier limits, their COMBINED P&L must be >= 0
        if earlier_limits:
            combined_earlier_pnl = sum(
                self.tp_config.calculate_pnl(
                    instrument, direction, lim.price_level, close_price, signal_type=signal_type
                )
                for lim in earlier_limits
            )
            if combined_earlier_pnl < -EPSILON:
                return False

        # Cumulative P&L = last limit P&L + all earlier limits P&L at current price
        cumulative_pnl = last_pnl + sum(
            self.tp_config.calculate_pnl(
                instrument, direction, lim.price_level, close_price, signal_type=signal_type
            )
            for lim in earlier_limits
        )

        success = await self._trigger_auto_profit(
            signal, hit_limits, last_pnl, num_hit, cumulative_pnl, close_price
        )
        return success

    async def _trigger_auto_profit(
        self,
        signal: SignalData,
        hit_limits: List[LimitData],
        last_pnl: float,
        limits_hit: int,
        cumulative_pnl: float = None,
        close_price: float = None,
    ) -> bool:
        """
        Mark signal as profit, send alerts, and clean up.

        cumulative_pnl: total P&L across all hit limits at the TP price.
                        Used for display only; DB stores close_price as tp_price.
        close_price:    the market price at TP trigger, stored as tp_price and used to calculate per-limit pnl.

        Returns True if successfully marked as profit, False on any failure.
        """
        signal_id = signal.signal_id
        instrument = signal.instrument
        direction = signal.direction.lower()
        signal_type = signal.type

        display_pnl = cumulative_pnl if cumulative_pnl is not None else last_pnl
        pnl_display = self.tp_config.format_value(instrument, display_pnl)
        reason = f"Auto TP: {limits_hit} limit(s) hit, +{pnl_display} cumulative profit"

        # Build per-limit pnl map: sequence_number -> formatted pnl string
        limit_pnl_map = {}
        if close_price is not None:
            for lim in hit_limits:
                pnl = self.tp_config.calculate_pnl(
                    instrument, direction, lim.price_level, close_price, signal_type=signal_type
                )
                limit_pnl_map[lim.sequence_number] = self.tp_config.format_value(instrument, pnl)

        logger.info(f"Signal {signal_id} ({instrument}): auto-TP triggered — {reason}")

        try:
            success = await asyncio.wait_for(
                self.signal_db.manually_set_signal_status(
                    signal_id,
                    "profit",
                    reason,
                    tp_price=close_price,
                    closed_reason="automatic",
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            logger.error(f"Signal {signal_id}: DB timeout while marking auto-TP profit")
            return False
        except Exception as e:
            logger.error(f"Signal {signal_id}: error marking auto-TP profit: {e}", exc_info=True)
            return False

        if not success:
            logger.error(
                f"Signal {signal_id}: manually_set_signal_status returned False for auto-TP"
            )
            return False

        self.evict_signal(signal_id)
        signal.tp_price = close_price
        logger.info(f"Signal {signal_id}: marked as PROFIT via auto-TP")

        # Send Discord alerts (alert channel + profit channel)
        if self.alert_system:
            try:
                await self.alert_system.send_auto_tp_alert(
                    signal,
                    hit_limits,
                    last_pnl,
                    self.tp_config,
                    cumulative_pnl=cumulative_pnl,
                    limit_pnl_map=limit_pnl_map,
                )
            except Exception as e:
                logger.error(
                    f"Signal {signal_id}: failed to send auto-TP alert: {e}", exc_info=True
                )

        return True
