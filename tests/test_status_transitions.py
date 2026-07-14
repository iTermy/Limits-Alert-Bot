"""Status state-machine tests for models.enums."""

import pytest

from models.enums import Direction, LimitStatus, SignalStatus, StatusTransitions

ALL_STATUSES = [s.value for s in SignalStatus]

LEGAL_EDGES = [
    ("active", "hit"),
    ("active", "cancelled"),
    ("active", "stop_loss"),
    ("hit", "profit"),
    ("hit", "breakeven"),
    ("hit", "stop_loss"),
    ("hit", "cancelled"),
    ("cancelled", "hit"),  # reactivation
    ("cancelled", "active"),  # reactivation
    ("profit", "cancelled"),  # admin correction
    ("breakeven", "cancelled"),
    ("stop_loss", "cancelled"),
    ("stop_loss", "active"),
    ("stop_loss", "hit"),
]


@pytest.mark.parametrize("old,new", LEGAL_EDGES)
def test_legal_transitions(old, new):
    assert StatusTransitions.is_valid_transition(old, new)


@pytest.mark.parametrize(
    "old,new",
    [
        (old, new)
        for old in ALL_STATUSES
        for new in ALL_STATUSES
        if (old, new) not in LEGAL_EDGES
    ],
)
def test_illegal_transitions(old, new):
    assert not StatusTransitions.is_valid_transition(old, new)


def test_unknown_status_has_no_transitions():
    assert not StatusTransitions.is_valid_transition("bogus", "active")


class TestSignalStatus:
    def test_final_statuses(self):
        assert SignalStatus.is_final("profit")
        assert SignalStatus.is_final("breakeven")
        assert SignalStatus.is_final("stop_loss")
        assert SignalStatus.is_final("cancelled")
        assert not SignalStatus.is_final("active")
        assert not SignalStatus.is_final("hit")

    def test_trackable_statuses(self):
        assert SignalStatus.is_trackable("active")
        assert SignalStatus.is_trackable("hit")
        assert not SignalStatus.is_trackable("profit")

    def test_is_valid(self):
        assert SignalStatus.is_valid("active")
        assert not SignalStatus.is_valid("open")


def test_limit_status_values():
    assert LimitStatus.is_valid("pending")
    assert LimitStatus.is_valid("hit")
    assert LimitStatus.is_valid("cancelled")
    assert not LimitStatus.is_valid("filled")


def test_direction_values():
    assert Direction.is_valid("long")
    assert Direction.is_valid("short")
    assert not Direction.is_valid("buy")
