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
Both directions are measured on the bid — the price the chart shows — so a $4
threshold fires on $4 of visible movement rather than $4 plus the spread.
"""

import asyncio
from typing import Optional

from database.signal_ops import STATUS_WRITE_TIMEOUT
from models.signal import LimitData, SignalData
from utils.logger import get_logger

logger = get_logger("tp_monitor")


def _fixed_tp_reached(direction: str, close_price: float, take_profit: float) -> bool:
    """Whether price has reached a signal's own take-profit level.

    Measured on the bid like every other TP evaluation, so the level fires where
    the chart shows it rather than a spread away.
    """
    if direction == "long":
        return close_price >= take_profit
    return close_price <= take_profit


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
        self._hit_limits_cache: dict[int, list[LimitData]] = {}

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

    async def check_signal(self, signal: SignalData, current_bid: float) -> bool:
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

        if signal.status not in ("hit", "active"):
            self.evict_signal(signal_id)
            return False

        num_hit = len(hit_limits)

        # Separate last vs earlier limits (ordered by sequence_number)
        last_limit = hit_limits[-1]
        earlier_limits = hit_limits[:-1]

        # Both directions close on the bid, matching what the chart displays.
        close_price = current_bid

        signal_type = signal.type
        last_pnl = self.tp_config.calculate_pnl(
            instrument, direction, last_limit.price_level, close_price, signal_type=signal_type
        )

        # Tiny epsilon to guard against floating-point rounding errors
        EPSILON = 1e-9

        if signal.take_profit is not None:
            # A signal carrying its own take-profit price exits there and nowhere
            # else — the configured threshold does not apply to it.
            if not _fixed_tp_reached(direction, close_price, signal.take_profit):
                return False
        else:
            tp_threshold = self.tp_config.get_tp_value(instrument, signal_type=signal_type)

            # Last limit must clear the TP threshold
            if last_pnl < tp_threshold - EPSILON:
                return False

            # If there are earlier limits, their COMBINED P&L must be >= 0
            if earlier_limits:
                combined_earlier_pnl = sum(
                    self.tp_config.calculate_pnl(
                        instrument, direction, lim.price_level, close_price,
                        signal_type=signal_type,
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

        # The threshold is cleared, so confirm against the DB that nothing closed
        # this signal since the last tick — a manual close from a command can take
        # up to a periodic-refresh cycle to reach the in-memory copy, and acting on
        # a stale one would double-mark the signal. This runs once per trigger, not
        # once per tick, so it stays off the hot path.
        if await self._closed_elsewhere(signal_id):
            return False

        success = await self._trigger_auto_profit(
            signal, hit_limits, last_pnl, num_hit, cumulative_pnl, close_price
        )
        return success

    async def _closed_elsewhere(self, signal_id: int) -> bool:
        """True if the DB shows this signal already closed; evicts it when so."""
        try:
            current = await self.signal_db.get_signal_with_limits(signal_id)
        except Exception as e:
            logger.warning(f"Signal {signal_id}: could not verify status before auto-TP: {e}")
            return False

        if current is None or current.status in ("hit", "active"):
            return False

        logger.info(
            f"Signal {signal_id}: status is '{current.status}', "
            f"skipping auto-TP and evicting from cache"
        )
        self.evict_signal(signal_id)
        return True

    async def _did_profit_land(self, signal_id: int) -> bool:
        """Re-fetch a signal after a write timeout; True if it committed as profit."""
        try:
            current = await self.signal_db.get_signal_with_limits(signal_id)
        except Exception as e:
            logger.error(f"Signal {signal_id}: could not verify status after timeout: {e}")
            return False
        return bool(current and current.status == "profit")

    async def _trigger_auto_profit(
        self,
        signal: SignalData,
        hit_limits: list[LimitData],
        last_pnl: float,
        limits_hit: int,
        cumulative_pnl: Optional[float] = None,
        close_price: Optional[float] = None,
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
                timeout=STATUS_WRITE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            # The Supabase pooler write can exceed the timeout yet still commit.
            # Verify the actual status: if it landed as profit, continue to the
            # embed edit so the DB and the embed don't diverge (which otherwise
            # leaves the embed showing HIT forever, since the next tick's status
            # guard evicts the signal before anything retries the alert).
            #
            # The budget matters as much as the verify: on the old 5 s one the
            # write was routinely still in flight when this re-read ran, so the
            # check reported a false negative on a profit that did land.
            logger.warning(
                f"Signal {signal_id}: DB timeout while marking auto-TP profit — verifying"
            )
            success = await self._did_profit_land(signal_id)
            if not success:
                logger.error(f"Signal {signal_id}: auto-TP profit write did not land after timeout")
                return False
            logger.info(f"Signal {signal_id}: auto-TP profit write landed despite timeout")
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
                sent = await self.alert_system.send_auto_tp_alert(
                    signal,
                    hit_limits,
                    last_pnl,
                    self.tp_config,
                    cumulative_pnl=cumulative_pnl,
                    limit_pnl_map=limit_pnl_map,
                )
                if not sent:
                    self.alert_system.queue_delivery_retry(
                        f"auto_tp:{signal_id}",
                        self.alert_system.send_auto_tp_alert,
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
