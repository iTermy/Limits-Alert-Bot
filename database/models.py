"""
Database models, constants, and enums
"""
from enum import Enum
from typing import List


class SignalStatus:
    """Signal status constants matching the database schema"""
    ACTIVE = 'active'
    HIT = 'hit'
    PROFIT = 'profit'
    BREAKEVEN = 'breakeven'
    STOP_LOSS = 'stop_loss'
    CANCELLED = 'cancelled'

    # Final statuses for analytics
    FINAL_STATUSES = [PROFIT, BREAKEVEN, STOP_LOSS, CANCELLED]

    # Trackable statuses (need price monitoring)
    TRACKABLE_STATUSES = [ACTIVE, HIT]

    @classmethod
    def is_final(cls, status: str) -> bool:
        """Check if status is final"""
        return status in cls.FINAL_STATUSES

    @classmethod
    def is_trackable(cls, status: str) -> bool:
        """Check if status requires price tracking"""
        return status in cls.TRACKABLE_STATUSES

    @classmethod
    def is_valid(cls, status: str) -> bool:
        """Check if status is valid"""
        return status in [cls.ACTIVE, cls.HIT, cls.PROFIT,
                         cls.BREAKEVEN, cls.STOP_LOSS, cls.CANCELLED]


class LimitStatus:
    """Limit status constants"""
    PENDING = 'pending'
    HIT = 'hit'
    CANCELLED = 'cancelled'

    @classmethod
    def is_valid(cls, status: str) -> bool:
        """Check if limit status is valid"""
        return status in [cls.PENDING, cls.HIT, cls.CANCELLED]


class ChangeType:
    """Status change types"""
    AUTOMATIC = 'automatic'
    MANUAL = 'manual'


class Direction:
    """Trade direction constants"""
    LONG = 'long'
    SHORT = 'short'

    @classmethod
    def is_valid(cls, direction: str) -> bool:
        """Check if direction is valid"""
        return direction in [cls.LONG, cls.SHORT]


class StatusTransitions:
    """Valid status transition rules"""

    VALID_TRANSITIONS = {
        SignalStatus.ACTIVE: [
            SignalStatus.HIT,
            SignalStatus.CANCELLED,
            SignalStatus.STOP_LOSS
        ],
        SignalStatus.HIT: [
            SignalStatus.PROFIT,
            SignalStatus.BREAKEVEN,
            SignalStatus.STOP_LOSS,
            SignalStatus.CANCELLED
        ],
        SignalStatus.CANCELLED: [
            SignalStatus.HIT,
            SignalStatus.ACTIVE  # Can revert cancellation
        ],
        # Final statuses can transition to cancelled for corrections
        SignalStatus.PROFIT: [SignalStatus.CANCELLED],
        SignalStatus.BREAKEVEN: [SignalStatus.CANCELLED],
        SignalStatus.STOP_LOSS: [SignalStatus.CANCELLED]
    }

    @classmethod
    def is_valid_transition(cls, old_status: str, new_status: str) -> bool:
        """
        Check if a status transition is valid

        Args:
            old_status: Current status
            new_status: Desired new status

        Returns:
            Whether transition is valid
        """
        return new_status in cls.VALID_TRANSITIONS.get(old_status, [])