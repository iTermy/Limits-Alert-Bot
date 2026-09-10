"""When a news window opens and closes.

The window is deliberately asymmetric: it opens LEAD_MINUTES ahead of the release
because positioning and spread widening start well before the print, and closes
window_minutes after it, once the book is back to normal.
"""

from datetime import datetime, timedelta, timezone

from core.news_manager import LEAD_MINUTES, NewsEvent

NEWS_TIME = datetime(2026, 9, 10, 12, 30, tzinfo=timezone.utc)


def _event(window_minutes: int = 15, **kwargs) -> NewsEvent:
    return NewsEvent(
        category="USD",
        news_time=NEWS_TIME,
        window_minutes=window_minutes,
        created_by="test",
        **kwargs,
    )


def test_window_opens_lead_minutes_before_the_release():
    event = _event()
    assert event.start_time == NEWS_TIME - timedelta(minutes=LEAD_MINUTES)
    assert not event.is_active(NEWS_TIME - timedelta(minutes=LEAD_MINUTES + 1))
    assert event.is_active(NEWS_TIME - timedelta(minutes=LEAD_MINUTES - 1))


def test_window_closes_window_minutes_after_the_release():
    event = _event()
    assert event.end_time == NEWS_TIME + timedelta(minutes=15)
    assert event.is_active(NEWS_TIME + timedelta(minutes=14))
    assert not event.is_active(NEWS_TIME + timedelta(minutes=16))


def test_now_mode_starts_when_it_was_set():
    """`!news now` means now — there is no release ahead of it to lead into."""
    event = _event(window_minutes=0, is_now_mode=True)
    assert event.start_time == NEWS_TIME
    assert event.is_active(NEWS_TIME)
