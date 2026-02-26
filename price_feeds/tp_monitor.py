"""
Auto Take-Profit Monitor

Evaluates TP conditions on every price tick for signals that have at least
one limit hit (status = HIT).

Logic per price tick:
  - Identify the "last" hit limit (highest sequence_number).
  - Calculate its P&L from hit_price → current price.
  - If only 1 limit is hit: last_pnl >= tp_threshold → auto-profit.
  - If 2+ limits hit: last_pnl >= tp_threshold AND sum(pnl of all others) >= 0.

P&L is in native units (pips for forex, dollars for everything else).
Uses bid price for long P&L (what you could close at), ask for short.
"""

import asyncio
from typing import Dict, List

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

        # signal_id -> List[Dict]  (hit limits with hit_price, ordered by seq)
        self._hit_limits_cache: Dict[int, List[Dict]] = {}

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
        signal: Dict,
        current_bid: float,
        current_ask: float,
    ) -> bool:
        """
        Evaluate TP conditions for a signal on a price tick.

        Returns:
            True if TP was triggered (signal marked as profit), else False.
        """
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]
        direction = signal["direction"].lower()

        hit_limits = self._hit_limits_cache.get(signal_id)
        if not hit_limits:
            return False

        num_hit = len(hit_limits)

        # Separate last vs earlier limits (ordered by sequence_number)
        last_limit = hit_limits[-1]
        earlier_limits = hit_limits[:-1]

        # Long: close at bid; Short: close at ask
        close_price = current_bid if direction == "long" else current_ask

        # P&L for the last limit
        last_entry = last_limit.get("hit_price") or last_limit.get("price_level")
        if last_entry is None:
            logger.warning(f"Signal {signal_id}: last limit has no entry price, skipping TP check")
            return False

        last_pnl = self.tp_config.calculate_pnl(instrument, direction, last_entry, close_price)
        tp_threshold = self.tp_config.get_tp_value(instrument)

        # Tiny epsilon to guard against floating-point rounding errors
        EPSILON = 1e-9

        # Last limit must clear the TP threshold
        if last_pnl < tp_threshold - EPSILON:
            return False

        # If there are earlier limits, their COMBINED P&L must be >= 0
        if earlier_limits:
            combined_earlier_pnl = 0.0
            for lim in earlier_limits:
                entry = lim.get("hit_price") or lim.get("price_level")
                if entry is None:
                    logger.warning(f"Signal {signal_id}: limit {lim.get('limit_id')} has no entry price")
                    continue
                combined_earlier_pnl += self.tp_config.calculate_pnl(
                    instrument, direction, entry, close_price
                )

            if combined_earlier_pnl < -EPSILON:
                return False

        success = await self._trigger_auto_profit(signal, hit_limits, last_pnl, num_hit)
        return success

    async def _trigger_auto_profit(self, signal: Dict, hit_limits: list,
                                    last_pnl: float, limits_hit: int) -> bool:
        """
        Mark signal as profit, send alerts, and clean up.

        Returns True if successfully marked as profit, False on any failure.
        """
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]

        pnl_display = self.tp_config.format_value(instrument, last_pnl)
        reason = f"Auto TP: {limits_hit} limit(s) hit, last limit +{pnl_display} profit"

        logger.info(f"Signal {signal_id} ({instrument}): auto-TP triggered — {reason}")

        try:
            success = await asyncio.wait_for(
                self.signal_db.manually_set_signal_status(
                    signal_id,
                    "profit",
                    reason,
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
            logger.error(f"Signal {signal_id}: manually_set_signal_status returned False for auto-TP")
            return False

        self.evict_signal(signal_id)
        logger.info(f"Signal {signal_id}: marked as PROFIT via auto-TP")

        # Send Discord alerts (alert channel + profit channel)
        if self.alert_system:
            try:
                await self.alert_system.send_auto_tp_alert(
                    signal,
                    hit_limits,
                    last_pnl,
                    self.tp_config,
                )
            except Exception as e:
                logger.error(f"Signal {signal_id}: failed to send auto-TP alert: {e}", exc_info=True)

        return True