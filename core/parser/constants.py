"""
Constants and mappings for the trading signal parser
"""

# Forex pairs that might use large numbers
FOREX_PAIRS = {
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    'EURGBP', 'EURJPY', 'GBPJPY', 'AUDJPY', 'NZDJPY', 'EURAUD', 'EURNZD',
    'GBPAUD', 'GBPNZD', 'EURCHF', 'AUDCAD', 'AUDNZD', 'CADCHF', 'CADJPY',
    'CHFJPY', 'EURCAD', 'EURNZD', 'GBPCAD', 'GBPCHF', 'GBPNZD', 'NZDCAD',
    'NZDCHF', 'NZDJPY', 'AUDCHF', 'EURSGD', 'EURTRY', 'GBPSGD', 'USDMXN',
    'USDNOK', 'USDSEK', 'USDSGD', 'USDTRY', 'USDZAR', 'ZARJPY'
}

# High-value instruments (don't divide)
HIGH_VALUE_INSTRUMENTS = {
    'BTCUSDT', 'BTCUSD', 'ETHUSDT', 'JP225', 'US30USD', 'SPX500USD',
    'NAS100USD', 'US2000USD', 'DE30EUR', 'AUS2000', 'F40'
}

# Instrument mappings with abbreviations
INSTRUMENT_MAPPINGS = {
    # Forex abbreviations
    'eu': 'EURUSD',
    'gu': 'GBPUSD',
    'uj': 'USDJPY',
    'uchf': 'USDCHF',
    'au': 'AUDUSD',
    'ucad': 'USDCAD',
    'nu': 'NZDUSD', 'nzd': 'NZDUSD',
    'eg': 'EURGBP', 'ej': 'EURJPY', 'gj': 'GBPJPY',
    'aj': 'AUDJPY', 'nj': 'NZDJPY', 'ea': 'EURAUD',

    # Full pairs
    'eurusd': 'EURUSD', 'gbpusd': 'GBPUSD', 'usdjpy': 'USDJPY',
    'usdchf': 'USDCHF', 'audusd': 'AUDUSD', 'usdcad': 'USDCAD',
    'nzdusd': 'NZDUSD', 'eurgbp': 'EURGBP', 'eurjpy': 'EURJPY',
    'gbpjpy': 'GBPJPY', 'audjpy': 'AUDJPY', 'nzdjpy': 'NZDJPY',
    'euraud': 'EURAUD', 'eurnzd': 'EURNZD', 'gbpaud': 'GBPAUD',
    'gbpnzd': 'GBPNZD', 'eurchf': 'EURCHF', 'audcad': 'AUDCAD',
    'audnzd': 'AUDNZD', 'cadchf': 'CADCHF', 'cadjpy': 'CADJPY',
    'chfjpy': 'CHFJPY', 'eurcad': 'EURCAD', 'gbpcad': 'GBPCAD',
    'gbpchf': 'GBPCHF', 'nzdcad': 'NZDCAD', 'nzdchf': 'NZDCHF',
    'audchf': 'AUDCHF',

    # Commodities
    'gold': 'XAUUSD', 'xauusd': 'XAUUSD', 'xau': 'XAUUSD',
    'silver': 'XAGUSD', 'xagusd': 'XAGUSD', 'xag': 'XAGUSD',
    'oil': 'USOILSPOT', 'wti': 'USOILSPOT', 'crude': 'USOILSPOT', 'usoil': 'USOILSPOT',
    'brent': 'UKOIL', 'ukoil': 'UKOIL',

    # Indices
    'spx': 'SPX500USD', 'sp500': 'SPX500USD', 's&p': 'SPX500USD', 'spx500': 'SPX500USD',
    'nas': 'NAS100USD', 'nasdaq': 'NAS100USD', 'nas100': 'NAS100USD', 'ndx': 'NAS100USD',
    'dow': 'US30USD', 'us30': 'US30USD', 'djia': 'US30USD',
    'jp225': 'JP225', 'nikkei': 'JP225',
    'dax': 'DE30EUR', 'dax30': 'DE30EUR', 'de30': 'DE30EUR',
    'russell': 'US2000USD', 'us2000': 'US2000USD', 'rut': 'US2000USD',
    'aus200': 'AUS2000', 'asx': 'AUS2000',
    'f40': 'F40', 'cac': 'F40',

    # Crypto
    'btc': 'BTCUSDT', 'bitcoin': 'BTCUSDT', 'btcusdt': 'BTCUSDT',
    'eth': 'ETHUSDT', 'ethereum': 'ETHUSDT', 'ethusdt': 'ETHUSDT',
    'sol': 'SOLUSDT', 'solana': 'SOLUSDT',
    'bnb': 'BNBUSDT', 'ada': 'ADAUSDT', 'xrp': 'XRPUSDT',
    'dot': 'DOTUSDT', 'doge': 'DOGEUSDT', 'avax': 'AVAXUSDT',
    'shib': 'SHIBUSDT', 'matic': 'MATICUSDT', 'uni': 'UNIUSDT',
    'link': 'LINKUSDT', 'ltc': 'LTCUSDT', 'atom': 'ATOMUSDT',
    'etc': 'ETCUSDT', 'xlm': 'XLMUSDT', 'vet': 'VETUSDT',
    'fil': 'FILUSDT', 'trx': 'TRXUSDT', 'theta': 'THETAUSDT',
}

# Direction keywords
LONG_KEYWORDS = ['long', 'buy']
SHORT_KEYWORDS = ['short', 'sell']

# Expiry patterns - UPDATED per requirements
EXPIRY_PATTERNS = {
    'vth': 'week_end',  # Valid Till Week (corrected from Valid Till Hit)
    'vtai': 'no_expiry',  # Valid Till Alien Invasion (no expiry)
    'alien': 'no_expiry',  # Alternative for VTAI
    'vtd': 'day_end',  # Valid Till Day
    'vtw': 'week_end',  # Valid Till Week End
    'vtwe': 'week_end',  # Valid Till Week End (alternative)
    'vtm': 'month_end',  # Valid Till Month End
    'vtme': 'month_end',  # Valid Till Month End (alternative)
    'valid till hit': 'no_expiry',
    'valid till week': 'week_end',
    'valid till day': 'day_end',
    'valid till month': 'month_end',
    'swing': 'week_end',
    'no expiry': 'no_expiry'
}

# Special keywords to track
SPECIAL_KEYWORDS = ['hot', 'semi-swing', 'swing', 'scalp', 'swing-trade', 'intraday', 'position']

# Exclusion keywords
EXCLUSION_KEYWORDS = ['futures', 'future', 'dxy', 'nq', 'es', 'ym', 'rty', 'vix', 'gc', 'gc1', 'gc1!', 'gcz']

# Skip words for stock parsing
STOCK_SKIP_WORDS = {
    'LONG', 'SHORT', 'BUY', 'SELL', 'VTH', 'VTAI', 'VTWE', 'VTD', 'VTME',
    'HOT', 'STOPS', 'SL', 'STOP', 'ALIEN', 'SCALP', 'SWING', 'INTRADAY',
    'POSITION', 'SEMI-SWING', 'DAY-TRADE', 'SWING-TRADE'
}

# Trading keywords for signal detection
TRADING_KEYWORDS = ['stop', 'sl', 'long', 'short', 'buy', 'sell', 'stops']