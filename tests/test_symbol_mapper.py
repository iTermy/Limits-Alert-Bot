"""SymbolMapper tests: asset-class detection, feed routing, and the
internal <-> feed-specific symbol translations (including the Exness oil
mapping that requires both forward and reverse entries)."""

import pytest

from price_feeds.symbol_mapper import SymbolMapper


@pytest.fixture(scope="module")
def mapper():
    return SymbolMapper()  # loads config/symbol_mappings.json


class TestAssetClass:
    @pytest.mark.parametrize(
        "symbol,expected",
        [
            ("EURUSD", "forex"),
            ("USDJPY", "forex_jpy"),
            ("XAUUSD", "metals"),
            ("GCQ26", "metals"),
            ("XAGUSD", "metals"),
            ("BTCUSDT", "crypto"),
            ("SOLUSDT", "crypto"),
            ("USOILSPOT", "oil"),
            ("XTIUSD", "oil"),
            ("SPX500USD", "indices"),
            ("NAS100USD", "indices"),
            ("JP225", "indices"),
            ("AMD.NAS", "stocks"),
            ("KO.NYSE", "stocks"),
        ],
    )
    def test_determine_asset_class(self, mapper, symbol, expected):
        assert mapper.determine_asset_class(symbol) == expected


class TestFeedRouting:
    @pytest.mark.parametrize(
        "symbol,feed",
        [
            ("EURUSD", "icmarkets"),
            ("XAUUSD", "icmarkets"),
            ("AMD.NAS", "icmarkets"),
            ("SPX500USD", "oanda"),
            ("BTCUSDT", "binance"),
            ("USOILSPOT", "exness"),
            ("XTIUSD", "icmarkets"),  # IC oil stays on ICMarkets
        ],
    )
    def test_get_best_feed(self, mapper, symbol, feed):
        assert mapper.get_best_feed(symbol) == feed


class TestFeedSymbolTranslation:
    def test_exness_oil_forward(self, mapper):
        assert mapper.get_feed_symbol("USOILSPOT", "exness") == "USOILm"

    def test_exness_oil_reverse(self, mapper):
        # Without this reverse mapping, prices arrive under USOILM and never
        # match signals.
        assert mapper.get_internal_symbol("USOILm", "exness") == "USOILSPOT"

    def test_binance_is_lowercase(self, mapper):
        assert mapper.get_feed_symbol("BTCUSDT", "binance") == "btcusdt"

    def test_binance_reverse_is_uppercase(self, mapper):
        assert mapper.get_internal_symbol("btcusdt", "binance") == "BTCUSDT"

    def test_oanda_forex_underscore(self, mapper):
        assert mapper.get_feed_symbol("EURUSD", "oanda") == "EUR_USD"

    def test_oanda_index_underscore(self, mapper):
        assert mapper.get_feed_symbol("SPX500USD", "oanda") == "SPX500_USD"

    def test_oanda_reverse_strips_underscore(self, mapper):
        assert mapper.get_internal_symbol("EUR_USD", "oanda") == "EURUSD"
        assert mapper.get_internal_symbol("SPX500_USD", "oanda") == "SPX500USD"

    def test_icmarkets_stock_24_suffix_round_trip(self, mapper):
        assert mapper.get_feed_symbol("AMD.NAS", "icmarkets") == "AMD.NAS-24"
        assert mapper.get_internal_symbol("AMD.NAS-24", "icmarkets") == "AMD.NAS"

    def test_reverse_always_uppercase(self, mapper):
        for feed_symbol, feed in [
            ("eurusd", "icmarkets"),
            ("ethusdt", "binance"),
            ("eur_usd", "oanda"),
        ]:
            result = mapper.get_internal_symbol(feed_symbol, feed)
            assert result == result.upper()


class TestValidateSymbol:
    def test_valid_symbols(self, mapper):
        for symbol in ("EURUSD", "XAUUSD", "BTCUSDT", "USOILSPOT", "SPX500USD"):
            ok, reason = mapper.validate_symbol(symbol)
            assert ok, f"{symbol}: {reason}"

    def test_empty_symbol(self, mapper):
        ok, _ = mapper.validate_symbol("")
        assert not ok
