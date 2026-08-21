"""Expiry-timestamp tests for database.utils.calculate_expiry.

The weekend cases are the point: a position holding an expiry through Friday
rolls over rather than closing, so the next expiry has to be a day the market
actually trades. A Saturday 4:45 PM would expire the signal into a shut market.
"""

from datetime import datetime

import pytest
import pytz

from database import utils
from database.utils import calculate_expiry

EST = pytz.timezone("America/New_York")

# 2026-08-10 is a Monday, so the offsets below land on known weekdays.
MONDAY = 10
THURSDAY = 13
FRIDAY = 14
SATURDAY = 15
SUNDAY = 16


@pytest.fixture
def at(monkeypatch):
    """Freeze calculate_expiry's clock at a given EST wall time."""

    def _at(day, hour, minute=0, month=8, year=2026):
        frozen = EST.localize(datetime(year, month, day, hour, minute))

        class _Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return frozen.astimezone(tz) if tz else frozen

        monkeypatch.setattr(utils, "datetime", _Clock)
        return frozen

    return _at


def expiry(*args, **kwargs):
    return datetime.fromisoformat(calculate_expiry(*args, **kwargs))


def test_no_expiry_has_no_timestamp():
    assert calculate_expiry("no_expiry") is None


@pytest.mark.parametrize(
    "day,hour,expected_day,label",
    [
        (MONDAY, 9, MONDAY, "before today's close"),
        (MONDAY, 17, MONDAY + 1, "after today's close"),
        (THURSDAY, 17, FRIDAY, "Thursday evening rolls to Friday"),
        (FRIDAY, 9, FRIDAY, "Friday before the close"),
        (FRIDAY, 17, MONDAY + 7, "Friday evening skips the weekend"),
        (SATURDAY, 12, MONDAY + 7, "Saturday lands on Monday"),
        (SUNDAY, 12, MONDAY + 7, "Sunday lands on Monday"),
        (SUNDAY, 20, MONDAY + 7, "Sunday after the reopen still lands on Monday"),
    ],
)
def test_day_end_skips_the_weekend(at, day, hour, expected_day, label):
    at(day, hour)
    result = expiry("day_end")
    assert result == EST.localize(datetime(2026, 8, expected_day, 16, 45)), label


def test_unknown_expiry_type_behaves_like_day_end(at):
    at(FRIDAY, 17)
    assert expiry("nonsense") == expiry("day_end")


@pytest.mark.parametrize(
    "day,hour,expected_day,label",
    [
        (MONDAY, 9, FRIDAY, "mid-week points at this Friday"),
        (FRIDAY, 9, FRIDAY, "Friday before the close is still today"),
        (FRIDAY, 17, FRIDAY + 7, "Friday after the close rolls a week"),
        (SATURDAY, 12, FRIDAY + 7, "Saturday points at next Friday"),
        (SUNDAY, 12, FRIDAY + 7, "Sunday points at next Friday"),
    ],
)
def test_week_end(at, day, hour, expected_day, label):
    at(day, hour)
    assert expiry("week_end") == EST.localize(datetime(2026, 8, expected_day, 16, 45)), label


def test_month_end_walks_back_to_a_weekday(at):
    # August 2026 ends on a Monday; its last weekday is the 31st.
    at(MONDAY, 9)
    assert expiry("month_end") == EST.localize(datetime(2026, 8, 31, 16, 45))


def test_expiry_across_a_dst_change_is_still_4_45_pm(at):
    # US DST ends Sunday 2026-11-01, so a Friday roll-over crosses the boundary.
    at(30, 17, month=10)  # Friday 2026-10-30, after the close
    result = expiry("day_end")
    assert result == EST.localize(datetime(2026, 11, 2, 16, 45))
    assert result.astimezone(EST).hour == 16
