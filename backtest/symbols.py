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
}

# Instruments whose live feed was NOT ICMarkets — their replay prices carry a
# broker basis that must be calibrated before their results are trusted.
NON_NATIVE = {"SPX500USD", "NAS100USD", "JP225", "DE30EUR", "BTCUSDT"}

IC_TERMINAL = r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe"

# Ticks exist from this date; earlier requests return nothing (verified 2026-08-20).
TICK_HISTORY_START = "2026-04-01"

ARCHIVE_DIR = r"C:\Python Stuff\TM-Backtest-Data\ticks"
