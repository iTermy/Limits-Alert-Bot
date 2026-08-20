"""Internal instrument names mapped to their ICMarkets MT5 symbol.

The bot priced indices off OANDA, oil off Exness and crypto off Binance, but the
backtest replays everything against ICMarkets ticks. Where the internal name and
the IC name differ the price series carries a broker basis, which is measured per
symbol in the calibration step rather than assumed away.
"""

# Ordered by signal count over the tick-covered window (2026-04-01 onward).
ARCHIVE_SYMBOLS = {
    "XAUUSD": "XAUUSD",
    "GBPUSD": "GBPUSD",
    "EURUSD": "EURUSD",
    "USDJPY": "USDJPY",
    "USDCHF": "USDCHF",
    "EURJPY": "EURJPY",
    "SPX500USD": "US500",
    "NAS100USD": "USTEC",
    "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD",
    "EURGBP": "EURGBP",
    "EURAUD": "EURAUD",
    "EURCAD": "EURCAD",
    "BTCUSDT": "BTCUSD",
    "JP225": "JP225",
    "EURNZD": "EURNZD",
    "EURCHF": "EURCHF",
    "GBPCAD": "GBPCAD",
    "GBPAUD": "GBPAUD",
    "DE30EUR": "DE40",
    # Long tail. Individually thin, but together 441 signals / 70 entered, and
    # they carry the minor crosses and stocks that earlier analyses flagged as
    # loss-making — exactly the population a coverage gap must not hide.
    "CADCHF": "CADCHF",
    "AUDCHF": "AUDCHF",
    "AUDCAD": "AUDCAD",
    "GBPCHF": "GBPCHF",
    "AUDNZD": "AUDNZD",
    "NZDCHF": "NZDCHF",
    "GBPNZD": "GBPNZD",
    "NZDCAD": "NZDCAD",
    "NZDUSD": "NZDUSD",
    "GBPJPY": "GBPJPY",
    "AUDJPY": "AUDJPY",
    "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY",
    "XAGUSD": "XAGUSD",
    "GCZ26_CFD": "GCZ26_CFD",
    "XTIUSD": "XTIUSD",
    "USOILSPOT": "XTIUSD",
    "US30USD": "US30",
    "DE40": "DE40",
    "UK100": "UK100",
    "UK100GBP": "UK100",
    "UK100USD": "UK100",
    "F40": "F40",
    "FR40EUR": "F40",
    "AUS2000": "AUS200",
    "AU200": "AUS200",
    "HK50": "HK50",
    "HKG33": "HK50",
    "JP225USD": "JP225",
    "ETHUSDT": "ETHUSD",
    "AAPL.NAS": "AAPL.NAS",
    "NVDA.NAS": "NVDA.NAS",
    "AMZN.NAS": "AMZN.NAS",
    "TSLA.NAS": "TSLA.NAS",
    "CTAS.NAS": "CTAS.NAS",
    "MSFT.NAS": "MSFT.NAS",
    "AVGO.NAS": "AVGO.NAS",
    "LRCX.NAS": "LRCX.NAS",
    "CRWD.NAS": "CRWD.NAS",
    "AMD.NAS": "AMD.NAS",
    "NFLX.NAS": "NFLX.NAS",
    "ACMR.NAS": "ACMR.NAS",
    "PYPL.NAS": "PYPL.NAS",
    "SNPS.NAS": "SNPS.NAS",
    "INTC.NAS": "INTC.NAS",
    "QCOM.NAS": "QCOM.NAS",
    "PEP.NAS": "PEP.NAS",
    "ABNB.NAS": "ABNB.NAS",
    "COST.NAS": "COST.NAS",
    "CSCO.NAS": "CSCO.NAS",
    "ACLS.NAS": "ACLS.NAS",
    "AAXJ.NAS": "AAXJ.NAS",
    "GOOG.NAS": "GOOG.NAS",
    "GGLS.NAS": "GGLS.NAS",
    "ISRG.NAS": "ISRG.NAS",
    "LIN.NAS": "LIN.NAS",
    "TMUS.NAS": "TMUS.NAS",
    "ANF.NYSE": "ANF.NYSE",
    "NKE.NYSE": "NKE.NYSE",
    "XOM.NYSE": "XOM.NYSE",
    "XYZ.NYSE": "XYZ.NYSE",
    "SPY.NYSE": "SPY.NYSE",
    "BAC.NYSE": "BAC.NYSE",
    "PANW.NYSE": "PANW.NYSE",
}

# Instruments whose live feed was NOT ICMarkets — their replay prices carry a
# broker basis that must be calibrated before their results are trusted.
# Indices came off OANDA, crypto off Binance, USOILSPOT off Exness; forex,
# metals, stocks and XTIUSD were IC-native and calibrate to ~0.
NON_NATIVE = {
    "SPX500USD", "NAS100USD", "JP225", "JP225USD", "DE30EUR", "DE40",
    "US30USD", "UK100", "UK100GBP", "UK100USD", "F40", "FR40EUR",
    "AUS2000", "AU200", "HK50", "HKG33", "BTCUSDT", "ETHUSDT", "USOILSPOT",
}

# Signal instruments with no ICMarkets equivalent, and why. Kept explicit so a
# future coverage check reports a known gap rather than rediscovering it.
UNAVAILABLE = {
    "GCQ26": "expired COMEX gold future; IC carries only the front GCZ26_CFD",
    "EURBGBG": "parser typo (EURGBP)",
    "GBPCFH": "parser typo (GBPCHF)",
    "GPBNZD": "parser typo (GBPNZD)",
    "JPYUSD": "parser typo (USDJPY)",
    "GOOGL.NAS": "IC lists GOOG.NAS only",
    "SNDK.NAS": "not listed on IC",
    "XOM.NAS": "IC lists XOM.NYSE only",
}

IC_TERMINAL = r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe"

# Ticks exist from this date; earlier requests return nothing (verified 2026-08-20).
TICK_HISTORY_START = "2026-04-01"

ARCHIVE_DIR = r"C:\Python Stuff\TM-Backtest-Data\ticks"
