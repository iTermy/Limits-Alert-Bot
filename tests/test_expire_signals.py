"""expire_old_signals holds open positions and cancels only resting ones.

An open position reaching its expiry rolls the expiry forward instead of
closing — weekend included, where it used to be cancelled and force-closed by
the execution client at the Friday close. Only the ids it genuinely cancelled
come back, because the caller's post-expiry cleanup (archive the embed, react
❌, finalize the analytics trackers) must not touch a signal that is still open.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace

import pytz

from core.expiry_manager import ExpiryManager
from database.signal_ops import SignalDatabase
from utils.logger import get_logger

EST = pytz.timezone("America/New_York")


class _StubConnection:
    def __init__(self):
        self.calls = []

    async def execute(self, query, *params):
        self.calls.append((query, params))

    async def fetchrow(self, query, *params):
        # No row: the close-price snapshot finds no live price and skips.
        self.calls.append((query, params))

    def statements(self, fragment):
        return [c for c in self.calls if fragment in c[0]]


class _StubDB:
    def __init__(self, conn, rows):
        self._conn = conn
        self._rows = rows

    async def fetch_all(self, query, params=None):
        return self._rows

    @asynccontextmanager
    async def get_connection(self):
        yield self._conn


def _expire(rows):
    conn = _StubConnection()
    cancelled = asyncio.run(SignalDatabase(_StubDB(conn, rows)).expire_old_signals())
    return cancelled, conn


def _row(signal_id, status, instrument="XAUUSD", expiry_type="day_end"):
    return {
        "id": signal_id,
        "status": status,
        "instrument": instrument,
        "expiry_type": expiry_type,
    }


def test_hit_signal_rolls_over_instead_of_closing(monkeypatch):
    # Friday evening — the case that used to cancel and flatten the position.
    monkeypatch.setattr(
        "database.signal_ops.calculate_expiry",
        lambda _type: EST.localize(datetime(2026, 8, 17, 16, 45)).isoformat(),
    )
    cancelled, conn = _expire([_row(1, "hit")])

    assert cancelled == [], "a rolled-over signal must not be reported as expired"
    assert not conn.statements("SET status = 'cancelled'"), "limits were cancelled"
    assert not conn.statements("closed_reason"), "the signal was closed"

    update = conn.statements("UPDATE signals")
    assert len(update) == 1
    assert update[0][1][0] == EST.localize(datetime(2026, 8, 17, 16, 45))

    audit = conn.statements("INSERT INTO status_changes")
    assert audit[0][1] == (1, "hit", "hit", "automatic", "rollover")


def test_hit_crypto_rolls_over_too(monkeypatch):
    monkeypatch.setattr(
        "database.signal_ops.calculate_expiry",
        lambda _type: EST.localize(datetime(2026, 8, 17, 16, 45)).isoformat(),
    )
    cancelled, conn = _expire([_row(2, "hit", instrument="BTCUSDT")])

    assert cancelled == []
    assert len(conn.statements("UPDATE signals")) == 1


def test_active_signal_is_cancelled_and_reported():
    cancelled, conn = _expire([_row(3, "active")])

    assert cancelled == [3], "the caller needs this id to clean up the embed"
    assert conn.statements("SET status = 'cancelled'"), "pending limits stayed pending"
    assert conn.statements("closed_reason"), "the signal was not closed"


def test_only_the_cancelled_ids_come_back(monkeypatch):
    monkeypatch.setattr(
        "database.signal_ops.calculate_expiry",
        lambda _type: EST.localize(datetime(2026, 8, 17, 16, 45)).isoformat(),
    )
    cancelled, _ = _expire([_row(4, "hit"), _row(5, "active"), _row(6, "hit")])

    assert cancelled == [5]


def test_nothing_expired_is_an_empty_list():
    cancelled, conn = _expire([])

    assert cancelled == []
    assert conn.calls == []


def test_expiry_manager_cleans_up_only_the_cancelled_signals(monkeypatch):
    """The rolled-over signal is still open: archiving its embed and finalizing
    its trackers would close out a position mid-trade."""
    cleaned = []

    async def _expire_old_signals():
        return [5]

    async def _record(self, sig_id, alert_system, monitor):
        cleaned.append(sig_id)

    manager = ExpiryManager.__new__(ExpiryManager)
    manager.logger = get_logger("test_expiry_manager")
    manager.bot = SimpleNamespace(
        services=SimpleNamespace(
            monitor=object(),
            alert_system=object(),
            signal_db=SimpleNamespace(expire_old_signals=_expire_old_signals),
        )
    )
    monkeypatch.setattr(ExpiryManager, "_handle_expired_signal", _record)

    asyncio.run(ExpiryManager.check_expiry.coro(manager))

    assert cleaned == [5]
