"""Enums and status constants for signals, limits, and lifecycle transitions."""

from enum import Enum
from typing import ClassVar


class SignalStatus(str, Enum):
    ACTIVE = "active"
    HIT = "hit"
    PROFIT = "profit"
    BREAKEVEN = "breakeven"
    STOP_LOSS = "stop_loss"
    CANCELLED = "cancelled"

    @classmethod
    def final_statuses(cls) -> list["SignalStatus"]:
        return [cls.PROFIT, cls.BREAKEVEN, cls.STOP_LOSS, cls.CANCELLED]

    @classmethod
    def trackable_statuses(cls) -> list["SignalStatus"]:
        return [cls.ACTIVE, cls.HIT]

    @classmethod
    def is_final(cls, status: str) -> bool:
        return status in {s.value for s in cls.final_statuses()}

    @classmethod
    def is_trackable(cls, status: str) -> bool:
        return status in {s.value for s in cls.trackable_statuses()}

    @classmethod
    def is_valid(cls, status: str) -> bool:
        return status in {s.value for s in cls}


class LimitStatus(str, Enum):
    PENDING = "pending"
    HIT = "hit"
    CANCELLED = "cancelled"

    @classmethod
    def is_valid(cls, status: str) -> bool:
        return status in {s.value for s in cls}


class ChangeType(str, Enum):
    AUTOMATIC = "automatic"
    MANUAL = "manual"


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"

    @classmethod
    def is_valid(cls, direction: str) -> bool:
        return direction in {s.value for s in cls}


class StatusTransitions:
    VALID_TRANSITIONS: ClassVar[dict[str, list[str]]] = {
        SignalStatus.ACTIVE: [SignalStatus.HIT, SignalStatus.CANCELLED, SignalStatus.STOP_LOSS],
        SignalStatus.HIT: [
            SignalStatus.PROFIT,
            SignalStatus.BREAKEVEN,
            SignalStatus.STOP_LOSS,
            SignalStatus.CANCELLED,
        ],
        SignalStatus.CANCELLED: [SignalStatus.HIT, SignalStatus.ACTIVE],
        SignalStatus.PROFIT: [SignalStatus.CANCELLED],
        SignalStatus.BREAKEVEN: [SignalStatus.CANCELLED],
        SignalStatus.STOP_LOSS: [SignalStatus.ACTIVE, SignalStatus.HIT, SignalStatus.CANCELLED],
    }

    @classmethod
    def is_valid_transition(cls, old_status: str, new_status: str) -> bool:
        return new_status in cls.VALID_TRANSITIONS.get(old_status, [])
