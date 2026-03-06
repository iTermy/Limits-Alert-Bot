"""
Near-Miss (NM) Monitor

Tracks price movement for signals that have had an approaching alert sent
and detects near-miss patterns using the LINEAR BOUNCE MODEL from NMConfig.

Detection logic per price tick:
  1. If price has never come within max_proximity of the first limit -> ignore.
  2. Once price enters the proximity zone, record closest_distance.
  3. On each tick, update closest_distance downward if price gets closer.
  4. A near-miss is confirmed when:
         current_distance - closest_distance >= required_bounce
     where:
         required_bounce = closest_distance + base_bounce  (linear formula)

  In plain English: the closer price got to the limit, the less "extra" bounce
  is needed to confirm it reacted — but you always need at least base_bounce.

Only signals with approaching_alert_sent=True on their first limit are tracked,
since those are the only ones the bot has an embed for.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional

from utils.logger import get_logger

logger = get_logger("nm_monitor")


@dataclass
class NMTrackingState:
    """Per-signal near-miss tracking state."""
    signal_id: int
    instrument: str
    direction: str            # 'long' or 'short'
    first_limit_price: float

    # True once price has entered the proximity zone (< max_proximity from limit)
    in_proximity: bool = False

    # Closest distance achieved to the first limit, in absolute price units
    closest_distance: float = float('inf')

    # The price at which the closest approach was reached (for logging)
    closest_price: float = 0.0


class NearMissMonitor:
    """
    Evaluates near-miss conditions on every price tick for signals
    that have approaching_alert_sent=True on their first limit.

    Integrated into StreamingPriceMonitor via _check_signal().
    State is entirely in-memory; rebuilt naturally on restart.
    """

    def __init__(self, nm_config, signal_db, db, alert_system=None):
        self.nm_config = nm_config
        self.signal_db = signal_db
        self.db = db
        self.alert_system = alert_system

        # signal_id -> NMTrackingState
        self._tracking: Dict[int, NMTrackingState] = {}

        # Signals currently being processed (dedup guard against rapid ticks)
        self._processing: set = set()

        # signal_ids that have been manually reactivated after an NM cancel.
        # These are immune to auto-NM for the rest of their life — they will
        # only close via a real hit, profit, stop-loss, or manual cancel.
        self._nm_immune: set = set()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def evict_signal(self, signal_id: int):
        """Remove a signal from NM tracking (call when signal closes)."""
        self._tracking.pop(signal_id, None)
        self._processing.discard(signal_id)
        self._nm_immune.discard(signal_id)

    def mark_immune(self, signal_id: int):
        """
        Mark a signal as immune to auto-NM cancellation.
        Call this when a signal is manually reactivated after an NM cancel.
        The signal will remain immune until it closes (evict_signal clears it).
        """
        self._tracking.pop(signal_id, None)   # clear any stale tracking state
        self._processing.discard(signal_id)
        self._nm_immune.add(signal_id)
        logger.info(f"Signal {signal_id} marked NM-immune (manually reactivated)")

    def get_tracking_state(self, signal_id: int) -> Optional[NMTrackingState]:
        return self._tracking.get(signal_id)

    def get_tracked_count(self) -> int:
        return len(self._tracking)

    def get_immune_count(self) -> int:
        return len(self._nm_immune)

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    def update(self, signal: Dict, current_price: float) -> bool:
        """
        Update NM tracking state for a signal on a price tick.

        Should only be called for 'active' signals whose first limit has
        approaching_alert_sent=True.

        Returns:
            True if a near-miss is confirmed this tick (caller should cancel).
            False otherwise.
        """
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]

        # Dedup guard
        if signal_id in self._processing:
            return False

        # Immunity guard — signal was manually reactivated after an NM cancel
        if signal_id in self._nm_immune:
            return False

        # Find the first pending limit
        pending_limits = signal.get("pending_limits", [])
        first_limit = next(
            (l for l in sorted(pending_limits, key=lambda x: x.get("sequence_number", 99))
             if l.get("sequence_number") == 1),
            None
        )
        if first_limit is None:
            return False

        # Only track if approaching alert was already sent
        if not first_limit.get("approaching_alert_sent", False):
            return False

        first_limit_price = first_limit["price_level"]
        current_distance = abs(current_price - first_limit_price)

        # Get thresholds from config
        max_proximity = self.nm_config.get_max_proximity(instrument)

        # Get or create tracking state
        state = self._tracking.get(signal_id)
        if state is None:
            # Don't create state until price enters the proximity zone
            if current_distance > max_proximity:
                return False
            state = NMTrackingState(
                signal_id=signal_id,
                instrument=instrument,
                direction=signal["direction"].lower(),
                first_limit_price=first_limit_price,
                in_proximity=True,
                closest_distance=current_distance,
                closest_price=current_price,
            )
            self._tracking[signal_id] = state
            logger.info(
                f"NM tracking started: signal {signal_id} ({instrument} {signal['direction'].upper()}) "
                f"limit @ {first_limit_price} | "
                f"entered proximity at {self.nm_config.format_value(instrument, current_distance)} away"
            )
            return False  # Need at least one more tick to confirm a bounce

        # If price gets closer -> update closest approach
        if current_distance < state.closest_distance:
            state.closest_distance = current_distance
            state.closest_price = current_price
            logger.debug(
                f"NM closest updated: signal {signal_id} ({instrument}) "
                f"@ {current_price:.5f}, distance={self.nm_config.format_value(instrument, current_distance)}"
            )
            return False

        # Price has moved away from its closest point.
        # Check the linear bounce condition:
        #   bounce_so_far >= closest_distance + base_bounce
        bounce_so_far = current_distance - state.closest_distance
        required_bounce = self.nm_config.get_required_bounce(instrument, state.closest_distance)

        if bounce_so_far >= required_bounce:
            logger.info(
                f"NEAR-MISS confirmed: signal {signal_id} ({instrument}) | "
                f"closest={self.nm_config.format_value(instrument, state.closest_distance)} "
                f"from limit | "
                f"bounce={self.nm_config.format_value(instrument, bounce_so_far)} "
                f"(needed {self.nm_config.format_value(instrument, required_bounce)})"
            )
            self._processing.add(signal_id)
            return True

        return False

    # ------------------------------------------------------------------
    # Trigger
    # ------------------------------------------------------------------

    async def trigger_near_miss(self, signal: Dict) -> bool:
        """
        Cancel a signal due to near-miss, update embed, send ping.

        Returns True if successfully cancelled.
        """
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]
        state = self._tracking.get(signal_id)

        closest_str = self.nm_config.format_value(instrument, state.closest_distance) if state else "N/A"
        required_str = (
            self.nm_config.format_value(instrument,
                self.nm_config.get_required_bounce(instrument, state.closest_distance))
            if state else "N/A"
        )

        logger.info(
            f"Signal {signal_id} ({instrument}): executing near-miss auto-cancel "
            f"[closest={closest_str}, required_bounce={required_str}]"
        )

        try:
            success = await asyncio.wait_for(
                self.signal_db.manually_set_signal_status(
                    signal_id,
                    "cancelled",
                    reason=f"near_miss_auto_cancel:closest={closest_str}",
                    closed_reason="near_miss",
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            logger.error(f"Signal {signal_id}: DB timeout during near-miss cancel")
            self._processing.discard(signal_id)
            return False
        except Exception as e:
            logger.error(f"Signal {signal_id}: error during near-miss cancel: {e}", exc_info=True)
            self._processing.discard(signal_id)
            return False

        if not success:
            logger.error(f"Signal {signal_id}: DB returned False for near-miss cancel")
            self._processing.discard(signal_id)
            return False

        # Clean up state before sending Discord alerts
        tracking_state = self._tracking.get(signal_id)
        self.evict_signal(signal_id)
        logger.info(f"Signal {signal_id} cancelled via near-miss")

        if self.alert_system:
            try:
                await self.alert_system.send_near_miss_cancel_alert(signal, tracking_state)
            except Exception as e:
                logger.error(f"Signal {signal_id}: failed to send NM cancel alert: {e}", exc_info=True)

        return True