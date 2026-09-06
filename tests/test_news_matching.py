"""Which instruments a news category covers, and getting the flag to the EX bot.

The matching rules here are mirrored by the execution bot's `instrument_under_news`
/ `news_names_asset`. Where the two disagree, one bot keeps tracking a signal whose
position the other has already flattened at market — so a case added on one side
belongs on the other too.
"""

import asyncio
from datetime import datetime, timezone

import pytest

from core.news_manager import NewsEvent, NewsManager


def _event(category: str) -> NewsEvent:
    return NewsEvent(
        category=category,
        news_time=datetime.now(timezone.utc),
        window_minutes=10,
        created_by="test",
    )


@pytest.mark.parametrize(
    "category,instrument",
    [
        # A dollar release moves every dollar-denominated market, whether or not the
        # symbol spells the currency out.
        ("USD", "EURUSD"),
        ("USD", "USDJPY"),
        ("USD", "XAUUSD"),
        ("USD", "XAGUSD"),
        ("USD", "GCZ26_CFD"),
        ("USD", "USOILSPOT"),
        ("USD", "XTIUSD"),
        ("USD", "AMD.NAS"),
        ("USD", "NAS100USD"),
        ("USD", "SPX500USD"),
        ("EUR", "EURUSD"),
        ("EUR", "DE30EUR"),
        ("JPY", "GBPJPY"),
        # Gold trades as spot and as CFDs on the futures contracts.
        ("GOLD", "XAUUSD"),
        ("GOLD", "GCZ26_CFD"),
        ("XAU", "GCZ26_CFD"),
        # Oil trades under a different symbol on each feed.
        ("OIL", "USOILSPOT"),
        ("OIL", "XTIUSD"),
        ("BTC", "BTCUSDT"),
        ("ETH", "ETHUSDT"),
        ("CRYPTO", "BTCUSDT"),
        ("CRYPTO", "ETHUSDT"),
        ("ALL", "GCZ26_CFD"),
        ("ALL", "BTCUSDT"),
    ],
)
def test_category_covers_instrument(category, instrument):
    assert _event(category).instrument_affected(instrument) is True


@pytest.mark.parametrize(
    "category,instrument",
    [
        ("USD", "EURGBP"),
        ("USD", "DE40"),
        ("USD", "DE30EUR"),
        # A 24/7 book doesn't halt for a scheduled release, and every crypto symbol
        # carries a currency code it would otherwise match on.
        ("USD", "BTCUSDT"),
        ("EUR", "BTCUSDT"),
        ("EUR", "XAUUSD"),
        ("CHF", "EURUSD"),
        # AUS2000 is the Australian small-cap index and contains 'US2000'.
        ("USD", "AUS2000"),
        ("GOLD", "XAGUSD"),
        ("GOLD", "EURUSD"),
        ("OIL", "XAUUSD"),
        ("BTC", "ETHUSDT"),
        # A crypto window must not sweep up the dollar-quoted indices.
        ("CRYPTO", "NAS100USD"),
        ("CRYPTO", "SPX500USD"),
        ("CRYPTO", "XAUUSD"),
        ("CRYPTO", "USOILSPOT"),
        ("CRYPTO", "EURUSD"),
    ],
)
def test_category_does_not_cover_instrument(category, instrument):
    assert _event(category).instrument_affected(instrument) is False


class _FailingDB:
    """Rejects the first write, accepts the rest."""

    def __init__(self, failures: int = 1):
        self.remaining_failures = failures
        self.writes: list = []

    async def set_news_mode(self, value):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RuntimeError("pooler timeout")
        self.writes.append(value)


def _manager_with_active_news(db) -> NewsManager:
    manager = NewsManager()
    manager.set_db(db)
    manager._events = [_event("ALL")]
    return manager


def test_failed_news_mode_write_is_retried():
    # Marking a write that never landed as synced skips every later write of the
    # same value, and the EX bot trades the whole window unguarded.
    db = _FailingDB(failures=1)
    manager = _manager_with_active_news(db)

    asyncio.run(manager.reconcile_news_mode())
    assert db.writes == []
    assert manager._news_mode_synced is False

    asyncio.run(manager.reconcile_news_mode())
    assert db.writes == ["ALL"]
    assert manager._news_mode_synced is True


def test_successful_news_mode_write_is_not_repeated():
    db = _FailingDB(failures=0)
    manager = _manager_with_active_news(db)

    asyncio.run(manager.reconcile_news_mode())
    asyncio.run(manager.reconcile_news_mode())

    assert db.writes == ["ALL"]
