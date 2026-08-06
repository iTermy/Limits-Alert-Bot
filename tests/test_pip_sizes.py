"""Pip-size table tests for BaseThresholdConfig.get_pip_size.

The pip size feeds every excursion/P&L pip calculation; a wrong branch here
silently corrupts analytics (this exact bug corrupted era-1 excursion rows).
"""

import pytest

from price_feeds.config._base_config import BaseThresholdConfig

get_pip_size = BaseThresholdConfig.get_pip_size


@pytest.mark.parametrize(
    "symbol,expected",
    [
        # Stocks — must win over the index-keyword branch (".NAS" contains "NAS")
        ("AMD.NAS", 0.01),
        ("NVDA.NAS", 0.01),
        ("KO.NYSE", 0.01),
        # Forex
        ("EURUSD", 0.0001),
        ("GBPUSD", 0.0001),
        ("USDJPY", 0.01),
        ("EURJPY", 0.01),
        # Metals
        ("XAUUSD", 0.01),
        ("GCZ26_CFD", 0.01),
        ("XAGUSD", 0.001),
        # Crypto
        ("BTCUSDT", 1.0),
        ("ETHUSDT", 0.1),
        ("SOLUSDT", 0.1),
        # Indices
        ("SPX500USD", 1.0),
        ("NAS100USD", 1.0),
        ("US30USD", 1.0),
        ("JP225", 1.0),
        ("US2000USD", 1.0),
        # Oil
        ("USOILSPOT", 0.01),
        ("XTIUSD", 0.01),
        ("UKOIL", 0.01),
        # Unknown symbols fall back to forex pip
        ("UNKNOWN", 0.0001),
    ],
)
def test_pip_size(symbol, expected):
    assert get_pip_size(symbol) == expected


def test_stock_suffix_beats_index_keyword():
    # The .NAS suffix must be evaluated before the "NAS" index keyword.
    assert get_pip_size("ANYTHING.NAS") == 0.01
    assert get_pip_size("NAS100USD") == 1.0
