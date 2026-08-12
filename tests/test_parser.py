"""Parser tests: signal-type detection, limit/stop determination, and
end-to-end parsing through CorePatternParser.

The parsers read the gold-tolls / risky-gold SL offsets from settings.json
with a 30 s cache; tests pin them via monkeypatch so results don't depend on
the local config.
"""

import pytest

from core.parser import INSTRUMENT_MAPPINGS, LimitsOrderError, RejectedSignal, SignalParser
from core.parser import pattern_parsers as pp
from core.parser.pattern_parsers import (
    CorePatternParser,
    determine_limits_and_stop,
    get_signal_type,
    parse_instant_signal,
    validate_limits_order,
)
from core.parser.validators import detect_channel_type, uses_gold_tolls_sl


@pytest.fixture
def pinned_offsets(monkeypatch):
    monkeypatch.setattr(pp, "get_gold_tolls_sl_offset", lambda: 5.0)
    monkeypatch.setattr(pp, "get_risky_gold_sl_offset", lambda: 10.0)


class _StubConfigLoader:
    """Feeds SignalParser an explicit channel config instead of channels.json."""

    def __init__(self, channel_settings=None):
        self._channel_settings = channel_settings or {}

    def load(self, filename):
        return {"channel_settings": self._channel_settings}


@pytest.fixture
def core_parser():
    return CorePatternParser(channel_config={})


# ---------------------------------------------------------------------------
# Signal type detection
# ---------------------------------------------------------------------------


class TestGetSignalType:
    @pytest.mark.parametrize(
        "channel,expected",
        [
            ("scalps", "scalp"),
            ("swing-trades", "swing"),
            ("gold-swings", "swing"),
            ("gold-tolls-map", "toll"),
            ("general-tolls", "toll"),
            ("oil-tolls", "toll"),
            ("gold-pa-signals", "pa"),
            ("price-action-trades", "pa"),
            ("gold-1-1-rr", "1-1"),
            ("risky-gold", "risky"),
        ],
    )
    def test_channel_map_wins(self, channel, expected):
        assert get_signal_type("gold long 3300 3290", channel) == expected

    def test_channel_beats_body_keyword(self):
        assert get_signal_type("swing trade here", "scalps") == "scalp"

    def test_body_keyword_fallback(self):
        assert get_signal_type("eu long swing 1.17 1.16", "random-channel") == "swing"
        assert get_signal_type("eu long scalp 1.17 1.16", "random-channel") == "scalp"

    def test_default_standard(self):
        assert get_signal_type("eu long 1.17 1.16", "random-channel") == "standard"
        assert get_signal_type("eu long 1.17 1.16", None) == "standard"


# ---------------------------------------------------------------------------
# Limit ordering rules
# ---------------------------------------------------------------------------


class TestValidateLimitsOrder:
    def test_single_limit_always_passes(self):
        assert validate_limits_order([1.1], "long")
        assert validate_limits_order([1.1], "short")

    def test_long_requires_descending(self):
        assert validate_limits_order([1.1820, 1.1810, 1.1800], "long")
        assert not validate_limits_order([1.1800, 1.1810], "long")

    def test_short_requires_ascending(self):
        assert validate_limits_order([1.1800, 1.1810, 1.1820], "short")
        assert not validate_limits_order([1.1820, 1.1810], "short")


class TestDetermineLimitsAndStop:
    def test_standard_last_number_is_stop(self):
        limits, stop = determine_limits_and_stop([1.1820, 1.1810, 1.1750], "long")
        assert limits == [1.1820, 1.1810]
        assert stop == 1.1750

    def test_standard_first_number_stop_fallback(self):
        # Stop listed first (alternative convention): short with stop above.
        limits, stop = determine_limits_and_stop([1.19, 1.17, 1.18], "short")
        assert stop == 1.19
        assert limits == [1.17, 1.18]

    def test_standard_needs_two_numbers(self):
        assert determine_limits_and_stop([1.18], "long") == (None, None)

    def test_out_of_order_limits_raise(self):
        with pytest.raises(LimitsOrderError):
            determine_limits_and_stop([1.1800, 1.1810, 1.1750], "long")

    def test_gold_tolls_auto_sl_long(self, pinned_offsets):
        limits, stop = determine_limits_and_stop(
            [3310.0, 3305.0], "long", channel_name="gold-tolls-map", raw_text="gold long 3310 3305"
        )
        assert limits == [3310.0, 3305.0]
        assert stop == 3300.0  # min - 5.0 offset

    def test_gold_tolls_auto_sl_short(self, pinned_offsets):
        _, stop = determine_limits_and_stop(
            [3305.0, 3310.0],
            "short",
            channel_name="gold-tolls-map",
            raw_text="gold short 3305 3310",
        )
        assert stop == 3315.0  # max + 5.0 offset

    def test_gold_tolls_single_number_valid(self, pinned_offsets):
        limits, stop = determine_limits_and_stop(
            [3310.0], "long", channel_name="gold-tolls-map", raw_text="gold long 3310"
        )
        assert limits == [3310.0]
        assert stop == 3305.0

    def test_gold_tolls_explicit_sl_overrides_offset(self, pinned_offsets):
        limits, stop = determine_limits_and_stop(
            [3310.0, 3290.0],
            "long",
            channel_name="gold-tolls-map",
            raw_text="gold long 3310 sl 3290",
        )
        assert limits == [3310.0]
        assert stop == 3290.0

    def test_risky_gold_uses_risky_offset(self, pinned_offsets):
        _, stop = determine_limits_and_stop(
            [3310.0], "long", channel_name="risky-gold", raw_text="gold long 3310"
        )
        assert stop == 3300.0  # min - 10.0 risky offset

    def test_general_tolls_auto_sl_per_instrument(self):
        _, stop_spx = determine_limits_and_stop(
            [6000.0],
            "long",
            channel_name="general-tolls",
            raw_text="spx long 6000",
            instrument="SPX500USD",
        )
        assert stop_spx == 5990.0  # SPX offset $10
        _, stop_nas = determine_limits_and_stop(
            [21000.0],
            "long",
            channel_name="general-tolls",
            raw_text="nas long 21000",
            instrument="NAS100USD",
        )
        assert stop_nas == 20970.0  # NAS offset $30

    def test_general_tolls_explicit_sl(self):
        limits, stop = determine_limits_and_stop(
            [6000.0, 5980.0],
            "long",
            channel_name="general-tolls",
            raw_text="spx long 6000 sl 5980",
            instrument="SPX500USD",
        )
        assert limits == [6000.0]
        assert stop == 5980.0


# ---------------------------------------------------------------------------
# Channel helpers
# ---------------------------------------------------------------------------


class TestChannelHelpers:
    def test_uses_gold_tolls_sl(self):
        assert uses_gold_tolls_sl("gold-tolls-map")
        assert uses_gold_tolls_sl("oil-tolls")
        assert uses_gold_tolls_sl("risky-gold")
        assert not uses_gold_tolls_sl("general-tolls")  # has its own auto-SL mode
        assert not uses_gold_tolls_sl("scalps")
        assert not uses_gold_tolls_sl(None)

    def test_detect_channel_type(self):
        assert detect_channel_type("stock-signals") == "stock"
        assert detect_channel_type("crypto-alts") == "crypto"
        assert detect_channel_type("gold-tolls-map") == "core"
        assert detect_channel_type(None) == "core"


# ---------------------------------------------------------------------------
# End-to-end parses through CorePatternParser
# ---------------------------------------------------------------------------


class TestCoreParserEndToEnd:
    def test_forex_long(self, core_parser):
        signal = core_parser.parse("EU long 1.1820 1.1810 sl 1.1750", "forex-signals")
        assert signal is not None
        assert signal.instrument == "EURUSD"
        assert signal.direction == "long"
        assert signal.limits == [1.1820, 1.1810]
        assert signal.stop_loss == 1.1750
        assert signal.type == "standard"

    def test_gold_short(self, core_parser):
        signal = core_parser.parse("gold short 3305 3310 stops 3320", "gold-signals")
        assert signal is not None
        assert signal.instrument == "XAUUSD"
        assert signal.direction == "short"
        assert signal.limits == [3305.0, 3310.0]
        assert signal.stop_loss == 3320.0

    def test_crypto(self, core_parser):
        signal = core_parser.parse("btc long 118000 117000 sl 115000", "crypto-signals")
        assert signal is not None
        assert signal.instrument == "BTCUSDT"
        assert signal.limits == [118000.0, 117000.0]

    def test_swing_keyword_sets_type_and_expiry(self, core_parser):
        signal = core_parser.parse("eu long swing 1.1820 1.1750", "forex-signals")
        assert signal is not None
        assert signal.type == "swing"
        assert signal.expiry_type == "week_end"

    def test_tolls_auto_sl_end_to_end(self, pinned_offsets, core_parser):
        signal = core_parser.parse("long 3310 3305", "gold-tolls-map")
        assert signal is not None
        assert signal.instrument == "XAUUSD"  # gold channel default
        assert signal.limits == [3310.0, 3305.0]
        assert signal.stop_loss == 3300.0
        assert signal.type == "toll"

    def test_non_signal_returns_none(self, core_parser):
        assert core_parser.parse("what a great day for trading", "forex-signals") is None

    def test_no_direction_returns_none(self, core_parser):
        assert core_parser.parse("eu 1.1820 1.1750", "forex-signals") is None


class TestSignalParserRouting:
    def test_rejected_signal_on_out_of_order_limits(self):
        parser = SignalParser(config_loader=_StubConfigLoader())
        result = parser.parse("eu long 1.1800 1.1810 sl 1.1750", "forex-signals")
        assert isinstance(result, RejectedSignal)
        assert not result  # stays falsy for `if result:` guards

    def test_excluded_instrument(self):
        parser = SignalParser(config_loader=_StubConfigLoader())
        assert parser.parse("dxy long 105 104", "forex-signals") is None

    def test_default_instrument_from_channel_config(self):
        parser = SignalParser(
            config_loader=_StubConfigLoader({"my-gold-room": {"default_instrument": "XAUUSD"}})
        )
        signal = parser.parse("long 3310 3305 sl 3300", "my-gold-room")
        assert signal is not None
        assert signal.instrument == "XAUUSD"


class TestIndexSymbolCanonicalization:
    """DAX/FTSE mentions must canonicalize to the OANDA-fed symbols, while DE40 and
    UK100 stay as the ICMarkets contracts they literally name. The two families price
    ~2 points apart, and the EX bot decides whether to apply a broker offset from the
    symbol alone — so collapsing them puts orders in the wrong price frame."""

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("dax long 24500 sl 24400", "DE30EUR"),
            ("dax30 long 24500 sl 24400", "DE30EUR"),
            ("de30 long 24500 sl 24400", "DE30EUR"),
            ("de40 long 24500 sl 24400", "DE40"),
            ("ftse short 10500 sl 10550", "UK100GBP"),
            ("uk100gbp short 10500 sl 10550", "UK100GBP"),
            ("uk100 short 10500 sl 10550", "UK100"),
        ],
    )
    def test_explicit_instrument_keeps_families_distinct(self, text, expected):
        assert pp._find_explicit_instrument(text) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("DE40", "DE40"),
            ("UK100", "UK100"),
            ("DAX", "DE30EUR"),
            ("FTSE", "UK100GBP"),
            ("UK100GBP", "UK100GBP"),
            # The model has emitted this non-existent symbol; it reached the DB raw
            # and produced a signal with no feed behind it.
            ("UK100USD", "UK100GBP"),
        ],
    )
    def test_ai_output_canonicalizes(self, raw, expected):
        assert INSTRUMENT_MAPPINGS.get(raw.lower(), raw) == expected


class TestInstantEntryParsing:
    """semi-swing-pa-signals enters at the market: the message names an
    instrument, a direction and labelled SL/TP, and carries no limits."""

    CHANNEL = "semi-swing-pa-signals"
    CONFIG = {CHANNEL: {"default_expiry": "no_expiry"}}

    def _parse(self, text):
        return parse_instant_signal(text, self.CHANNEL, self.CONFIG)

    @pytest.mark.parametrize(
        "text,direction,sl,tp",
        [
            ("short gold sl 5001 tp 4080", "short", 5001.0, 4080.0),
            ("long gold tp 5100 sl 4990", "long", 4990.0, 5100.0),
            ("gold buy stop loss 4990 take profit 5100", "long", 4990.0, 5100.0),
            ("sell gold sl: 5001 tp: 4080", "short", 5001.0, 4080.0),
        ],
    )
    def test_labels_parse_in_any_order(self, text, direction, sl, tp):
        signal = self._parse(text)
        assert signal is not None
        assert (signal.instrument, signal.direction) == ("XAUUSD", direction)
        assert (signal.stop_loss, signal.take_profit) == (sl, tp)
        assert signal.instant_entry is True
        assert signal.limits == []

    def test_type_and_expiry_come_from_channel(self):
        signal = self._parse("short gold sl 5001 tp 4080")
        assert signal.type == "pa"
        assert signal.expiry_type == "no_expiry"

    @pytest.mark.parametrize(
        "text",
        [
            "short gold sl 5001",  # no take profit
            "short gold tp 4080",  # no stop loss
            "gold sl 5001 tp 4080",  # no direction
            "short sl 5001 tp 4080",  # no instrument
        ],
    )
    def test_incomplete_signals_are_not_parsed(self, text):
        assert self._parse(text) is None

    @pytest.mark.parametrize(
        "text",
        [
            "short gold sl 4080 tp 5001",  # TP above SL on a short
            "long gold sl 5001 tp 4080",  # TP below SL on a long
        ],
    )
    def test_take_profit_on_losing_side_is_rejected(self, text):
        with pytest.raises(LimitsOrderError):
            self._parse(text)

    def test_routed_from_the_channel(self):
        parser = SignalParser(config_loader=_StubConfigLoader(self.CONFIG))
        signal = parser.parse("short gold sl 5001 tp 4080", self.CHANNEL)
        assert signal.parse_method == "instant"
        assert signal.take_profit == 4080.0

    def test_typo_reaches_the_caller_as_a_rejection(self):
        parser = SignalParser(config_loader=_StubConfigLoader(self.CONFIG))
        assert isinstance(
            parser.parse("short gold sl 4080 tp 5001", self.CHANNEL), RejectedSignal
        )

    def test_other_channels_keep_limit_parsing(self, pinned_offsets):
        parser = SignalParser(config_loader=_StubConfigLoader({}))
        signal = parser.parse("gold long 4000 3990 sl 3980", "gold-swings")
        assert signal.instant_entry is False
        assert signal.take_profit is None
        assert signal.limits == [4000.0, 3990.0]
