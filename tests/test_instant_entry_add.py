"""A second market entry averaged into an instant-entry signal ("add" reply).

The added entry is a fill that already happened, so it lands in the same shape
the first one did — status 'hit', hit_time and hit_price stamped — and bumps the
signal's counters. The cap is enforced in the DB write under a row lock, because
two replies arriving together would otherwise both read one entry and both add.
"""

import asyncio
from contextlib import asynccontextmanager

from database.signal_ops import MAX_INSTANT_ENTRIES, SignalDatabase


class _StubConnection:
    def __init__(self, total_limits):
        self._total_limits = total_limits
        self.calls = []

    async def fetchrow(self, query, *params):
        self.calls.append(("fetchrow", query, params))
        return {"total_limits": self._total_limits}

    async def fetchval(self, query, *params):
        self.calls.append(("fetchval", query, params))
        return 4242

    async def execute(self, query, *params):
        self.calls.append(("execute", query, params))

    @asynccontextmanager
    async def transaction(self):
        yield

    def statement(self, fragment):
        matches = [c for c in self.calls if fragment in c[1]]
        assert len(matches) == 1, f"expected one {fragment!r} statement, got {len(matches)}"
        return matches[0]


class _StubDB:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def get_connection(self):
        yield self._conn


def _add(total_limits, entry_price=4085.5):
    conn = _StubConnection(total_limits)
    limit_id = asyncio.run(SignalDatabase(_StubDB(conn)).add_instant_entry(777, entry_price))
    return conn, limit_id


def test_added_entry_is_written_already_filled():
    conn, limit_id = _add(total_limits=1)

    assert limit_id == 4242
    _, _, params = conn.statement("INSERT INTO limits")
    signal_id, price_level, sequence, status, hit_time = params
    assert signal_id == 777
    assert price_level == 4085.5
    assert sequence == 2
    assert status == "hit"
    assert hit_time is not None, "hit_time gates how late the execution bot may enter"


def test_added_entry_bumps_the_signal_counters():
    conn, _ = _add(total_limits=1)

    _, query, params = conn.statement("UPDATE signals")
    assert params == (2, 777)
    assert "limits_hit = limits_hit + 1" in query


def test_the_row_is_locked_before_the_count_is_read():
    conn, _ = _add(total_limits=1)

    _, query, _ = conn.statement("SELECT total_limits")
    assert "FOR UPDATE" in query, "two concurrent adds would both see one entry"


def test_a_full_signal_takes_no_further_entry():
    conn, limit_id = _add(total_limits=MAX_INSTANT_ENTRIES)

    assert limit_id is None
    assert not [c for c in conn.calls if "INSERT INTO limits" in c[1]]
    assert not [c for c in conn.calls if "UPDATE signals" in c[1]]
