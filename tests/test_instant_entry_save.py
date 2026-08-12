"""save_signal writes an instant-entry signal as an already-filled position.

The row must never exist in the shape an execution client reads as "a limit
resting at this price": a pending limit under an active signal. Instant signals
record a fill that already happened, so they are born hit — anything less leaves
a window in which downstream clients place a real order at the market price.
"""

import asyncio
from contextlib import asynccontextmanager

from core.parser import ParsedSignal
from database.signal_ops import SignalDatabase


class _StubConnection:
    """Captures the statements save_signal issues, with their bound parameters."""

    def __init__(self):
        self.calls = []

    async def fetchval(self, query, *params):
        self.calls.append(("fetchval", query, params))
        return 777

    async def execute(self, query, *params):
        self.calls.append(("execute", query, params))

    async def executemany(self, query, params):
        self.calls.append(("executemany", query, params))

    async def fetchrow(self, query, *params):
        self.calls.append(("fetchrow", query, params))
        return None

    def statement(self, table_fragment):
        """The single statement whose query contains `table_fragment`."""
        matches = [c for c in self.calls if table_fragment in c[1]]
        assert len(matches) == 1, f"expected one {table_fragment} statement, got {len(matches)}"
        return matches[0]


class _StubDB:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def get_connection(self):
        yield self._conn


def _save(parsed):
    conn = _StubConnection()
    ok, signal_id = asyncio.run(
        SignalDatabase(_StubDB(conn)).save_signal(parsed, "msg-1", "chan-1")
    )
    assert ok and signal_id == 777, "save_signal did not report success"
    return conn


def _instant_signal():
    return ParsedSignal(
        instrument="XAUUSD",
        direction="short",
        limits=[4085.5],
        stop_loss=5001.0,
        expiry_type="day_end",
        raw_text="short gold sl 5001 tp 4080",
        parse_method="instant",
        type="pa",
        take_profit=4080.0,
        instant_entry=True,
    )


def _ladder_signal():
    return ParsedSignal(
        instrument="EURUSD",
        direction="long",
        limits=[1.0850, 1.0840],
        stop_loss=1.0800,
        expiry_type="day_end",
        raw_text="long eurusd 1.0850 1.0840 sl 1.0800",
        parse_method="high_confidence",
    )


# The pre-1.6.6 execution client's placeable-set filter: an active/hit signal
# with a limit still pending. This is the predicate that placed a live order on
# a user's account, so it is the one the fix has to defeat.
def _placeable_pre_166(row):
    return row["signal_status"] in ("active", "hit") and row["limit_status"] == "pending"


# 1.6.6 admits instant entries despite the limit being hit, keyed on take_profit.
def _placeable_166(row):
    return row["signal_status"] in ("active", "hit") and (
        row["limit_status"] == "pending" or row["take_profit"] is not None
    )


def _row_as_clients_see_it(conn):
    """Reconstruct the joined signals/limits row the execution clients fetch."""
    _, _, signal_params = conn.statement("INSERT INTO signals")
    _, _, limit_params = conn.statement("INSERT INTO limits")
    return {
        "signal_status": signal_params[8],
        "take_profit": signal_params[10],
        "limits_hit": signal_params[14],
        "first_limit_hit_time": signal_params[15],
        "limit_status": limit_params[2],
        "hit_time": limit_params[3],
        "price_level": limit_params[1],
    }


def test_instant_signal_is_born_filled():
    row = _row_as_clients_see_it(_save(_instant_signal()))

    assert row["signal_status"] == "hit"
    assert row["limit_status"] == "hit"
    assert row["limits_hit"] == 1
    assert row["first_limit_hit_time"] is not None
    assert row["hit_time"] is not None, "hit_time gates how late 1.6.6 may still enter"
    assert row["price_level"] == 4085.5


def test_instant_signal_is_invisible_to_pre_166_clients():
    row = _row_as_clients_see_it(_save(_instant_signal()))

    assert not _placeable_pre_166(row), "pre-1.6.6 would place a limit order for this row"
    assert _placeable_166(row), "1.6.6 must still take the instant entry"


def test_ladder_signal_still_saves_pending_limits():
    conn = _save(_ladder_signal())
    _, _, signal_params = conn.statement("INSERT INTO signals")
    _, _, limit_rows = conn.statement("INSERT INTO limits")

    assert signal_params[8] == "active"
    assert signal_params[14] == 0
    assert signal_params[15] is None
    assert [r[3] for r in limit_rows] == ["pending", "pending"]
    assert [r[1] for r in limit_rows] == [1.0850, 1.0840]
