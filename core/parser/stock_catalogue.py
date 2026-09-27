"""
stock_catalogue.py
Broker stock listings, shared between the price feed that reads them and the
parser that resolves tickers against them.

The MetaTrader5 package is process-global and not thread-safe, so exactly one
component may call it: the ICMarkets feed, which serialises every call onto its
own executor. The stock parser used to open a second MT5 connection and call
symbols_get()/symbol_info() straight from the event loop. The two clients raced,
symbols_get() came back with no stocks in it, and every stock message fell
through to the AI fallback — which guesses the exchange suffix (XOM.NAS for a
NYSE listing) and invents tickers that do not exist at all. Neither can ever be
priced, so the signal sat there silent.

The feed loads a snapshot; the parser reads it and makes no MT5 calls of its own.
"""

from typing import Iterable, Optional

from utils.logger import get_logger

logger = get_logger("stock_catalogue")

# US equities carry an exchange suffix on this broker.
STOCK_SUFFIXES = (".NYSE", ".NAS", ".NASDAQ")

# Most listings also expose a 24-hour contract (AAPL.NAS-24) that quotes outside
# US market hours. The database stores the bare name; SymbolMapper appends the
# suffix back on and ICMarketsStream falls back to the bare name when a listing
# has no 24-hour twin.
HOURS_24_SUFFIX = "-24"


def bare_symbol(name: str) -> str:
    """Strip the 24-hour contract suffix, giving the database form of a symbol."""
    upper = name.upper()
    return upper[: -len(HOURS_24_SUFFIX)] if upper.endswith(HOURS_24_SUFFIX) else upper


class StockCatalogue:
    """Ticker → broker symbol, plus descriptions for name-based matching."""

    def __init__(self):
        self._tickers: dict[str, str] = {}
        self._descriptions: dict[str, str] = {}

    @property
    def loaded(self) -> bool:
        return bool(self._tickers)

    @property
    def tickers(self) -> dict[str, str]:
        return self._tickers

    @property
    def descriptions(self) -> dict[str, str]:
        return self._descriptions

    def resolve(self, ticker: str) -> Optional[str]:
        """Return the database symbol for a bare ticker (AAPL -> AAPL.NAS)."""
        return self._tickers.get(ticker.upper())

    def canonical(self, name: str) -> Optional[str]:
        """Return the database symbol for a full name, or None if unlisted.

        Accepts either contract (AAPL.NAS, AAPL.NAS-24) and normalises the
        24-hour one onto the bare form the database stores.
        """
        symbol = bare_symbol(name)
        return symbol if symbol in self._descriptions else None

    def replace(self, entries: Iterable[tuple[str, str]]) -> int:
        """Rebuild the catalogue from (symbol name, description) pairs.

        Returns the number of distinct listings kept.
        """
        descriptions: dict[str, str] = {}
        has_24_hour: set[str] = set()

        for name, description in entries:
            symbol = bare_symbol(name)
            if not symbol.endswith(STOCK_SUFFIXES):
                continue
            if name.upper().endswith(HOURS_24_SUFFIX):
                has_24_hour.add(symbol)
            if description or symbol not in descriptions:
                descriptions[symbol] = description or descriptions.get(symbol, "")

        tickers: dict[str, str] = {}
        for symbol in sorted(descriptions):
            ticker = symbol.split(".")[0]
            incumbent = tickers.get(ticker)
            # A handful of tickers are listed on both exchanges because the
            # broker keeps the delisted side around (HON and PANW both moved to
            # NASDAQ). Only the current listing gets a 24-hour contract, so that
            # is the one to trade.
            if incumbent is None or (symbol in has_24_hour and incumbent not in has_24_hour):
                tickers[ticker] = symbol

        self._descriptions = descriptions
        self._tickers = tickers
        return len(descriptions)


stock_catalogue = StockCatalogue()
