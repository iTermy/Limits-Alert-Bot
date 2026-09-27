"""The stock catalogue resolves tickers to symbols the broker actually lists.

The parser used to build its ticker map from the bare listings only, and to read
them over its own MT5 connection racing the price feed's. Both failures produced
a signal nothing could price: a ticker whose only contract is the 24-hour one was
invisible, and when the race emptied the symbol list every stock message fell to
the AI fallback, which guesses the exchange suffix.
"""

import pytest

from core.parser.pattern_parsers import StockPatternParser
from core.parser.stock_catalogue import StockCatalogue, stock_catalogue


def _catalogue(*names):
    catalogue = StockCatalogue()
    catalogue.replace((name, f"{name.split('.')[0]} Inc") for name in names)
    return catalogue


def test_unloaded_catalogue_resolves_nothing():
    catalogue = StockCatalogue()
    assert not catalogue.loaded
    assert catalogue.resolve("AAPL") is None


def test_bare_listing_resolves():
    catalogue = _catalogue("AAPL.NAS", "AAPL.NAS-24")
    assert catalogue.loaded
    assert catalogue.resolve("AAPL") == "AAPL.NAS"


def test_ticker_with_only_a_24_hour_contract_is_listed():
    # 2,672 of the broker's tickers are in this shape. The old parser filtered
    # them out, so every one of them fell through to the AI fallback.
    catalogue = _catalogue("ACHR.NYSE-24")
    assert catalogue.resolve("ACHR") == "ACHR.NYSE"


def test_ticker_with_only_a_plain_contract_is_listed():
    catalogue = _catalogue("ABUS.NAS")
    assert catalogue.resolve("ABUS") == "ABUS.NAS"


@pytest.mark.parametrize(
    ("listings", "expected"),
    [
        # The broker keeps the delisted side of a move around. Only the current
        # listing gets a 24-hour contract, so that is the tradable one.
        (("PANW.NAS-24", "PANW.NYSE"), "PANW.NAS"),
        (("GPC.NAS", "GPC.NYSE-24"), "GPC.NYSE"),
        (("DPZ.NAS", "DPZ.NAS-24", "DPZ.NYSE"), "DPZ.NAS"),
    ],
)
def test_cross_listed_ticker_picks_the_current_exchange(listings, expected):
    assert _catalogue(*listings).resolve(expected.split(".")[0]) == expected


def test_resolution_is_case_insensitive():
    catalogue = _catalogue("AAPL.NAS-24")
    assert catalogue.resolve("aapl") == "AAPL.NAS"


def test_canonical_accepts_either_contract():
    catalogue = _catalogue("AAPL.NAS", "AAPL.NAS-24")
    assert catalogue.canonical("AAPL.NAS-24") == "AAPL.NAS"
    assert catalogue.canonical("aapl.nas") == "AAPL.NAS"
    assert catalogue.canonical("GLID.NAS") is None


def test_non_equity_symbols_are_ignored():
    catalogue = _catalogue("AAPL.NAS", "EURUSD", "XAUUSD", "GCZ26_CFD")
    assert set(catalogue.descriptions) == {"AAPL.NAS"}


def test_replace_swaps_the_whole_set():
    catalogue = _catalogue("AAPL.NAS")
    catalogue.replace([("TSLA.NAS", "Tesla Inc")])
    assert catalogue.resolve("AAPL") is None
    assert catalogue.resolve("TSLA") == "TSLA.NAS"


def test_description_survives_a_blank_on_one_contract():
    # Only one of the two contracts carries the description on some listings;
    # name matching reads it, so it must not be overwritten with the blank.
    catalogue = StockCatalogue()
    catalogue.replace([("AAPL.NAS-24", ""), ("AAPL.NAS", "Apple Inc")])
    assert catalogue.descriptions["AAPL.NAS"] == "Apple Inc"


class TestStockParserUsesTheCatalogue:
    """The parser reads the catalogue and makes no MT5 calls of its own."""

    @pytest.fixture(autouse=True)
    def _restore_shared_catalogue(self):
        yield
        stock_catalogue.replace([])

    @staticmethod
    def _parser(*listings):
        stock_catalogue.replace((name, f"{name.split('.')[0]} Inc") for name in listings)
        return StockPatternParser({})

    def test_ticker_resolves_to_the_listed_exchange(self):
        parser = self._parser("XOM.NYSE", "XOM.NYSE-24")
        signal = parser.parse("XOM short 115.2 115.8 stops 118.5", "stocks-trades")
        assert signal.instrument == "XOM.NYSE"

    def test_message_may_name_the_full_symbol(self):
        parser = self._parser("AAPL.NAS", "AAPL.NAS-24")
        signal = parser.parse("AAPL.NAS long 230 228 stops 225", "stocks-trades")
        assert signal.instrument == "AAPL.NAS"

    def test_unlisted_ticker_is_not_parsed(self):
        parser = self._parser("AAPL.NAS-24")
        assert parser.parse("GLID long 5.2 5.0 stops 4.5", "stocks-trades") is None

    def test_empty_catalogue_parses_nothing(self):
        parser = self._parser()
        assert parser.parse("AAPL long 230 228 stops 225", "stocks-trades") is None
