"""A second market entry averaged into an instant-entry signal ("add" reply).

The added entry is a fill that already happened, so it lands in the same shape
the first one did — status 'hit', hit_time and hit_price stamped — and bumps the
signal's counters. The cap and the signal's status are both re-read in the DB
write under a row lock, because two replies arriving together would otherwise
both read one entry and both add, and an auto-TP landing mid-flight would leave
a fill on a closed signal.

Averaging moves the breakeven point, so an armed breakeven stop that the new mean
would trip is disarmed by the same write — see _breakeven_disarm_note.
"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from database.signal_ops import MAX_INSTANT_ENTRIES, SignalDatabase
from discord_handlers.message_handler import MessageHandler
from models.signal import LimitData, SignalData


class _StubConnection:
    def __init__(self, total_limits, status="hit"):
        self._total_limits = total_limits
        self._status = status
        self.calls = []

    async def fetchrow(self, query, *params):
        self.calls.append(("fetchrow", query, params))
        return {"total_limits": self._total_limits, "status": self._status}

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


def _add(total_limits, entry_price=4085.5, status="hit", disarm_breakeven=False):
    conn = _StubConnection(total_limits, status)
    limit_id = asyncio.run(
        SignalDatabase(_StubDB(conn)).add_instant_entry(
            777, entry_price, disarm_breakeven=disarm_breakeven
        )
    )
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
    assert params == (2, 777, False)
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


def test_a_signal_closed_mid_flight_takes_no_entry():
    # The handler checked the status seconds ago; an auto-TP landing in between must
    # not leave a fill on a closed signal — and limits_hit counting it.
    conn, limit_id = _add(total_limits=1, status="profit")

    assert limit_id is None
    assert not [c for c in conn.calls if "INSERT INTO limits" in c[1]]
    assert not [c for c in conn.calls if "UPDATE signals" in c[1]]


def test_the_status_is_re_read_under_the_same_lock():
    conn, _ = _add(total_limits=1)

    _, query, _ = conn.statement("SELECT total_limits")
    assert "status" in query, "a status read outside the lock is the race being closed"


def test_a_disarm_rides_in_the_same_write_as_the_fill():
    # Separate round trips would leave a window where the signal is averaged but still
    # armed — exactly the tick that closes it flat.
    conn, _ = _add(total_limits=1, disarm_breakeven=True)

    _, query, params = conn.statement("UPDATE signals")
    assert params == (2, 777, True)
    assert "be_stop_armed_at" in query


# ---------------------------------------------------------------------------
# Whether the add takes the breakeven stop off with it
# ---------------------------------------------------------------------------


def _handler(bid):
    """A MessageHandler wired to just enough bot for _live_bid to answer."""
    handler = MessageHandler.__new__(MessageHandler)
    price = None if bid is None else {"bid": bid, "ask": bid + 0.5}

    async def get_latest_price(instrument):
        return price

    handler.bot = SimpleNamespace(
        services=SimpleNamespace(
            stream_manager=SimpleNamespace(get_latest_price=get_latest_price)
        )
    )
    return handler


def _armed_signal(direction, fills, armed=True):
    return SignalData(
        signal_id=777,
        instrument="XAUUSD",
        direction=direction,
        status="hit",
        take_profit=4200.0 if direction == "long" else 3900.0,
        be_stop_armed_at="2026-08-19T12:00:00+00:00" if armed else None,
        total_limits=len(fills),
        limits_hit=len(fills),
        limits=[
            LimitData(id=10 + i, price_level=p, sequence_number=i + 1, status="hit")
            for i, p in enumerate(fills)
        ],
    )


def _note(direction, fills, entry, bid, armed=True):
    signal = _armed_signal(direction, fills, armed)
    return asyncio.run(_handler(bid)._breakeven_disarm_note(signal, entry))


def test_averaging_down_past_the_market_takes_the_floor_off():
    # Long filled at 4000, armed, price back at 3980: the new mean of 3990 is above
    # the bid, so the stop would close the trade on the very next tick.
    note = _note("long", [4000.0], entry=3980.0, bid=3980.0)

    assert note is not None
    assert "3990" in note and "removed" in note


def test_a_short_averaging_up_is_the_same_shape():
    note = _note("short", [4000.0], entry=4020.0, bid=4020.0)

    assert note is not None
    assert "4010" in note


def test_an_add_that_leaves_the_trade_in_profit_keeps_the_floor():
    # Long filled at 4000, price ran to 4100, adding there means a mean of 4050 —
    # still below the bid, so the stop stays armed and keeps protecting.
    assert _note("long", [4000.0], entry=4100.0, bid=4100.0) is None


def test_an_unarmed_signal_has_nothing_to_say():
    assert _note("long", [4000.0], entry=3980.0, bid=3980.0, armed=False) is None


def test_no_live_bid_disarms_rather_than_guesses():
    # We cannot prove the new mean is safe, and the failure mode we are avoiding is
    # an instant close — so the floor comes off and the ping says why.
    note = _note("long", [4000.0], entry=3980.0, bid=None)

    assert note is not None
    assert "no live price" in note
