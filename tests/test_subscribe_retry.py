"""A subscribe a feed refused must be retried, not abandoned.

Through September 2026 stock signals "only showed hit when I restarted the bot".
MT5 is process-global and served a second, unserialized caller at the time
(the stock parser's own connection, and MarketContextProvider's bar fetches), so
`symbol_info()` landed mid-sweep and came back None for a ticker the broker does
list — AVGO.NAS-24, AMD.NAS-24, ADBE.NAS-24 and a dozen more all logged
"not found in MT5" within seconds of their signal being saved.

Three things then conspired to make that one bad moment permanent: the feed
raised before adding the symbol to its poll set, the manager marked the whole
batch subscribed anyway, and a symbol is only ever subscribed once — when its
first signal appears. So the signals sat there with nothing pricing them until a
restart re-ran the subscribe, which promptly succeeded and fired every level the
price had crossed in the meantime.
"""

import pytest

from price_feeds.feeds import price_stream_manager as psm
from price_feeds.feeds.icmarkets_stream import ICMarketsStream, UnlistedSymbolError

SYMBOL = "AVGO.NAS"
FEED_SYMBOL = "AVGO.NAS-24"


class PickyFeed:
    """Refuses every subscribe until `refuse` is cleared."""

    def __init__(self, refuse=True):
        self.refuse = refuse
        self.subscribed: set[str] = set()
        self.attempts: list[str] = []

    async def subscribe(self, symbol):
        self.attempts.append(symbol)
        if self.refuse:
            raise Exception(f"Symbol {symbol} not found in MT5")
        self.subscribed.add(symbol)

    async def bulk_subscribe(self, symbols):
        accepted = []
        for symbol in symbols:
            try:
                await self.subscribe(symbol)
            except Exception:
                continue
            accepted.append(symbol)
        return accepted

    async def unsubscribe(self, symbol):
        self.subscribed.discard(symbol)


def make_manager(feed):
    """A stream manager with only the fields the subscribe paths touch."""
    manager = psm.PriceStreamManager.__new__(psm.PriceStreamManager)
    manager.symbol_mapper = psm.SymbolMapper()
    manager.feeds = {"icmarkets": feed}
    manager.feed_status = {"icmarkets": True}
    manager.subscribed_symbols = set()
    manager.symbol_to_feed = {}
    manager._pending_subscriptions = {}
    manager.latest_prices = {}
    manager.price_lock = _NullLock()
    manager.health_monitor = None
    return manager


class _NullLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_a_refused_bulk_subscribe_is_not_counted_as_subscribed():
    """Marking the whole batch made a refused symbol look identical to a live
    one — the monitor kept the signal and the manager reported it watched."""
    feed = PickyFeed()
    manager = make_manager(feed)

    await manager.bulk_subscribe([SYMBOL])

    assert SYMBOL not in manager.subscribed_symbols
    assert manager._pending_subscriptions == {SYMBOL: "icmarkets"}


@pytest.mark.asyncio
async def test_a_refused_single_subscribe_is_queued_for_retry():
    feed = PickyFeed()
    manager = make_manager(feed)

    await manager.subscribe_symbol(SYMBOL)

    assert SYMBOL not in manager.subscribed_symbols
    assert manager._pending_subscriptions == {SYMBOL: "icmarkets"}


@pytest.mark.asyncio
async def test_the_retry_subscribes_once_the_feed_stops_refusing():
    """The transient case: a restart used to be the only thing that re-probed."""
    feed = PickyFeed()
    manager = make_manager(feed)
    await manager.bulk_subscribe([SYMBOL])

    feed.refuse = False
    await manager.retry_pending_subscriptions()

    assert SYMBOL in manager.subscribed_symbols
    assert manager.symbol_to_feed[SYMBOL] == "icmarkets"
    assert not manager._pending_subscriptions
    assert FEED_SYMBOL in feed.subscribed


@pytest.mark.asyncio
async def test_a_still_refusing_feed_keeps_the_symbol_pending():
    feed = PickyFeed()
    manager = make_manager(feed)
    await manager.bulk_subscribe([SYMBOL])

    await manager.retry_pending_subscriptions()

    assert manager._pending_subscriptions == {SYMBOL: "icmarkets"}
    assert len(feed.attempts) == 2


@pytest.mark.asyncio
async def test_a_down_feed_is_not_probed():
    """An outage must not turn into one refusal log per symbol per refresh."""
    feed = PickyFeed()
    manager = make_manager(feed)
    await manager.bulk_subscribe([SYMBOL])
    manager.feed_status["icmarkets"] = False

    await manager.retry_pending_subscriptions()

    assert len(feed.attempts) == 1


@pytest.mark.asyncio
async def test_unsubscribing_stops_the_retries():
    """The signals on it closed; nothing needs the symbol any more."""
    feed = PickyFeed()
    manager = make_manager(feed)
    await manager.bulk_subscribe([SYMBOL])

    await manager.unsubscribe_symbol(SYMBOL)
    await manager.retry_pending_subscriptions()

    assert not manager._pending_subscriptions
    assert len(feed.attempts) == 1


@pytest.mark.asyncio
async def test_an_accepted_symbol_is_never_pending():
    feed = PickyFeed(refuse=False)
    manager = make_manager(feed)

    await manager.bulk_subscribe([SYMBOL])

    assert SYMBOL in manager.subscribed_symbols
    assert not manager._pending_subscriptions


# ── The MT5 feed must not cache a miss it may have imagined ─────────────────


class StubInfo:
    visible = True


def make_ic_stream(answers):
    """An ICMarkets feed whose MT5 calls come from a scripted answer table.

    `answers` maps an MT5 symbol name to a list of results, popped per call —
    None for "not found", StubInfo() for a listing. Every symbol_info call is
    recorded in `probes`.
    """
    stream = ICMarketsStream.__new__(ICMarketsStream)
    stream.connected = True
    stream.subscribed_symbols = set()
    stream._stock_symbol_cache = {}
    stream._unlisted_symbols = set()
    stream.probes: list[str] = []

    async def run_mt5(func, *args):
        symbol = args[0]
        stream.probes.append(symbol)
        results = answers.get(symbol, [])
        return results.pop(0) if results else None

    stream.run_mt5 = run_mt5
    return stream


@pytest.mark.asyncio
async def test_a_double_miss_is_re_probed_next_time():
    """MT5 hands back None for a listing it does carry when a call lands
    mid-sweep. Caching that verdict is what made one bad moment permanent."""
    answers = {FEED_SYMBOL: [], SYMBOL: []}
    stream = make_ic_stream(answers)

    with pytest.raises(UnlistedSymbolError):
        await stream.subscribe(FEED_SYMBOL)

    # The broker answers properly this time; nothing should short-circuit it.
    answers[FEED_SYMBOL] = [StubInfo(), StubInfo()]
    await stream.subscribe(FEED_SYMBOL)

    assert FEED_SYMBOL in stream.subscribed_symbols
    assert FEED_SYMBOL not in stream._unlisted_symbols


@pytest.mark.asyncio
async def test_a_resolved_bare_fallback_is_still_cached():
    """A stock with no 24-hour contract is a real, stable answer — probing it
    on every subscribe would put pointless calls in front of the poll sweep."""
    answers = {SYMBOL: [StubInfo()] * 3}
    stream = make_ic_stream(answers)

    await stream.subscribe(FEED_SYMBOL)
    probes_after_first = len(stream.probes)
    await stream.subscribe(FEED_SYMBOL)

    assert stream._stock_symbol_cache[FEED_SYMBOL] == SYMBOL
    assert stream.subscribed_symbols == {SYMBOL}
    # Only the subscribe's own symbol_info, not the two resolution probes.
    assert len(stream.probes) == probes_after_first + 1


@pytest.mark.asyncio
async def test_bulk_subscribe_reports_only_what_it_accepted():
    answers = {"EURUSD": [StubInfo()]}
    stream = make_ic_stream(answers)

    accepted = await stream.bulk_subscribe(["EURUSD", FEED_SYMBOL])

    assert accepted == ["EURUSD"]
