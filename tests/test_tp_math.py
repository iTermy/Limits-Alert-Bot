"""TPConfig tests: threshold resolution order and P&L math.

Each test builds its config under tmp_path so the repo's live
tp_configuration.json is never touched.
"""

import pytest

from price_feeds.config.tp_config import TPConfig


@pytest.fixture
def tp(tmp_path):
    # Nonexistent path -> TPConfig writes its default config there.
    return TPConfig(config_path=str(tmp_path / "tp.json"))


class TestResolutionOrder:
    def test_type_default_used(self, tp):
        # scalp metals default is $2 vs standard $5
        assert tp.get_tp_value("XAUUSD", signal_type="scalp") == 2.0
        assert tp.get_tp_value("XAUUSD", signal_type="standard") == 5.0

    def test_per_type_symbol_override_wins(self, tp):
        tp.set_override("XAUUSD", 7.5, "dollars", signal_type="scalp")
        assert tp.get_tp_value("XAUUSD", signal_type="scalp") == 7.5
        # Other types unaffected
        assert tp.get_tp_value("XAUUSD", signal_type="standard") == 5.0

    def test_standard_symbol_override_falls_through_to_other_types(self, tp):
        tp.set_override("XAUUSD", 9.0, "dollars", signal_type="standard")
        # swing has no XAUUSD override; the standard symbol override outranks
        # the swing asset-class default.
        assert tp.get_tp_value("XAUUSD", signal_type="swing") == 9.0

    def test_per_type_override_beats_standard_override(self, tp):
        tp.set_override("XAUUSD", 9.0, "dollars", signal_type="standard")
        tp.set_override("XAUUSD", 3.0, "dollars", signal_type="scalp")
        assert tp.get_tp_value("XAUUSD", signal_type="scalp") == 3.0

    def test_asset_class_fallback_to_standard(self, tp):
        # 1-1 only defines metals; forex falls back to the standard default.
        assert tp.get_tp_value("XAUUSD", signal_type="1-1") == 10.0
        assert tp.get_tp_value("EURUSD", signal_type="1-1") == tp.get_tp_value(
            "EURUSD", signal_type="standard"
        )

    def test_unknown_type_treated_as_standard(self, tp):
        assert tp.get_tp_value("EURUSD", signal_type="nonsense") == tp.get_tp_value(
            "EURUSD", signal_type="standard"
        )

    def test_legacy_scalp_bool(self, tp):
        assert tp.get_tp_value("XAUUSD", signal_type=True) == tp.get_tp_value(
            "XAUUSD", signal_type="scalp"
        )
        assert tp.get_tp_value("XAUUSD", signal_type=False) == tp.get_tp_value(
            "XAUUSD", signal_type="standard"
        )

    def test_risky_seeded_from_scalp(self, tp):
        assert tp.get_tp_value("XAUUSD", signal_type="risky") == tp.get_tp_value(
            "XAUUSD", signal_type="scalp"
        )


class TestUnits:
    def test_forex_is_pips(self, tp):
        assert tp.get_tp_type("EURUSD") == "pips"
        assert tp.get_tp_type("USDJPY") == "pips"

    def test_metals_indices_crypto_are_dollars(self, tp):
        assert tp.get_tp_type("XAUUSD") == "dollars"
        assert tp.get_tp_type("SPX500USD") == "dollars"
        assert tp.get_tp_type("BTCUSDT") == "dollars"


class TestCalculatePnl:
    def test_forex_long_pips(self, tp):
        pnl = tp.calculate_pnl("EURUSD", "long", entry_price=1.1000, current_price=1.1010)
        assert pnl == pytest.approx(10.0)  # 10 pips

    def test_forex_short_pips(self, tp):
        pnl = tp.calculate_pnl("EURUSD", "short", entry_price=1.1000, current_price=1.1010)
        assert pnl == pytest.approx(-10.0)

    def test_jpy_pip_size(self, tp):
        pnl = tp.calculate_pnl("USDJPY", "long", entry_price=155.00, current_price=155.50)
        assert pnl == pytest.approx(50.0)  # 0.01 pip size

    def test_gold_dollars(self, tp):
        pnl = tp.calculate_pnl("XAUUSD", "long", entry_price=3300.0, current_price=3305.5)
        assert pnl == pytest.approx(5.5)

    def test_gold_short_dollars(self, tp):
        pnl = tp.calculate_pnl("XAUUSD", "short", entry_price=3300.0, current_price=3295.0)
        assert pnl == pytest.approx(5.0)


class TestOverrideManagement:
    def test_set_and_remove_override(self, tp):
        assert tp.set_override("EURUSD", 8.0, "pips")
        assert tp.get_tp_value("EURUSD") == 8.0
        assert tp.remove_override("EURUSD")
        assert tp.get_tp_value("EURUSD") == 5.0  # back to default
        assert not tp.remove_override("EURUSD")  # nothing left to remove

    def test_rejects_invalid_values(self, tp):
        assert not tp.set_override("EURUSD", -1.0, "pips")
        assert not tp.set_override("EURUSD", 5.0, "points")
