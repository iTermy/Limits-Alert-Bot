"""Session-boundary tests for FeedHealthMonitor.is_market_open.

The gate decides whether a silent feed is a real outage or just a closed
market. Getting the wrap-around week wrong made the bot declare OANDA down
every ~6 minutes through Sunday daytime and Friday night, DMing a failure
alert and reconnecting a stream that had nothing to deliver.
"""

from datetime import datetime

import pytest
import pytz

from price_feeds.monitors.feed_health_monitor import FeedHealthMonitor

EST = pytz.timezone("America/New_York")

# 2026-08-10 is a Monday, so the offsets below land on known weekdays.
MONDAY = 10
TUESDAY = 11
WEDNESDAY = 12
THURSDAY = 13
FRIDAY = 14
SATURDAY = 15
SUNDAY = 16


@pytest.fixture
def monitor():
    """A monitor with only the fields is_market_open touches."""
    m = FeedHealthMonitor.__new__(FeedHealthMonitor)
    m.est = EST
    m.us_market_holidays = ["2026-08-12"]
    return m


def at(day, hour, minute=0):
    return EST.localize(datetime(2026, 8, day, hour, minute))


@pytest.mark.parametrize("asset_class", ["forex", "metals", "indices", "oil"])
@pytest.mark.parametrize(
    "day,hour,expected,label",
    [
        # The weekend closes Friday 17:00 and does not reopen until Sunday 18:00.
        (FRIDAY, 16, True, "Friday before the close"),
        (FRIDAY, 17, False, "Friday at the close"),
        (FRIDAY, 20, False, "Friday night — was open before the fix"),
        (SATURDAY, 3, False, "small hours of Saturday"),
        (SATURDAY, 12, False, "Saturday midday"),
        (SATURDAY, 23, False, "late Saturday"),
        (SUNDAY, 0, False, "Sunday midnight — was open before the fix"),
        (SUNDAY, 12, False, "Sunday midday — was open before the fix"),
        (SUNDAY, 17, False, "Sunday an hour before the reopen"),
        (SUNDAY, 18, True, "Sunday at the reopen"),
        (SUNDAY, 22, True, "Sunday evening"),
        # Mid-week the session runs continuously across midnight.
        (MONDAY, 3, True, "Monday small hours"),
        (MONDAY, 12, True, "Monday midday"),
        (WEDNESDAY, 2, True, "Wednesday small hours"),
        # The daily 17:00-18:00 break is the spread hour.
        (TUESDAY, 17, False, "spread hour start"),
        (TUESDAY, 17, False, "inside spread hour"),
        (TUESDAY, 18, True, "spread hour end"),
        (THURSDAY, 16, True, "before spread hour"),
    ],
)
def test_wrapping_week(monitor, asset_class, day, hour, expected, label):
    assert monitor.is_market_open(asset_class, at=at(day, hour)) is expected, label


def test_spread_hour_is_closed_but_reopens_same_evening(monitor):
    assert monitor.is_market_open("forex", at=at(TUESDAY, 16, 59)) is True
    assert monitor.is_market_open("forex", at=at(TUESDAY, 17, 0)) is False
    assert monitor.is_market_open("forex", at=at(TUESDAY, 17, 59)) is False
    assert monitor.is_market_open("forex", at=at(TUESDAY, 18, 0)) is True


def test_forex_jpy_follows_forex(monitor):
    assert monitor.is_market_open("forex_jpy", at=at(SUNDAY, 12)) is False
    assert monitor.is_market_open("forex_jpy", at=at(MONDAY, 12)) is True


@pytest.mark.parametrize(
    "day,hour,expected,label",
    [
        (MONDAY, 12, True, "Monday is a trading day — was closed before the fix"),
        (FRIDAY, 12, True, "Friday midday"),
        (SATURDAY, 12, False, "Saturday — was open before the fix"),
        (SUNDAY, 12, False, "Sunday"),
        (MONDAY, 9, False, "before the 09:30 open"),
        (MONDAY, 17, False, "at the 17:00 close"),
        (WEDNESDAY, 12, False, "US market holiday"),
    ],
)
def test_stock_week(monitor, day, hour, expected, label):
    assert monitor.is_market_open("stocks", at=at(day, hour)) is expected, label


def test_crypto_is_always_open(monitor):
    for day in (FRIDAY, SATURDAY, SUNDAY, MONDAY):
        assert monitor.is_market_open("crypto", at=at(day, 3)) is True


def test_unknown_asset_class_assumes_open(monitor):
    assert monitor.is_market_open("widgets", at=at(SATURDAY, 12)) is True


def test_reproduces_the_logged_false_positive(monitor):
    """The production log's 21:05 PDT Saturday alert, in the bot's own terms.

    The PC ran on US Pacific, so local Saturday 21:05 was Sunday 00:05 ET —
    the first health check after the ET date rolled over, which is exactly
    when the six-minute OANDA failure cycle began.
    """
    pacific = pytz.timezone("America/Los_Angeles")
    local = pacific.localize(datetime(2026, 8, 15, 21, 5))
    assert monitor.is_market_open("indices", at=local) is False
