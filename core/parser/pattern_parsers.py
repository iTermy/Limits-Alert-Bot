"""
pattern_parsers.py
Channel-specific pattern-based parsers for trading signals
"""
import re
from typing import Optional, List, Set, Dict
from utils.logger import get_logger
from . import ParsedSignal, INSTRUMENT_MAPPINGS
from .validators import validate_signal

logger = get_logger("parser.pattern_parsers")

# Optional import for stock parsing
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    logger.warning("MetaTrader5 not available - stock parsing will be disabled")

# ============================================================================
# CONSTANTS
# ============================================================================

FOREX_PAIRS = {
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    'EURGBP', 'EURJPY', 'GBPJPY', 'AUDJPY', 'NZDJPY', 'EURAUD', 'EURNZD',
    'GBPAUD', 'GBPNZD', 'EURCHF', 'AUDCAD', 'AUDNZD', 'CADCHF', 'CADJPY',
    'CHFJPY', 'EURCAD', 'EURNZD', 'GBPCAD', 'GBPCHF', 'GBPNZD', 'NZDCAD',
    'NZDCHF', 'NZDJPY', 'AUDCHF', 'EURSGD', 'EURTRY', 'GBPSGD', 'USDMXN',
    'USDNOK', 'USDSEK', 'USDSGD', 'USDTRY', 'USDZAR', 'ZARJPY'
}

# High-value instruments
HIGH_VALUE_INSTRUMENTS = {
    'BTCUSDT', 'BTCUSD', 'ETHUSDT', 'JP225', 'US30USD', 'SPX500USD',
    'NAS100USD', 'US2000USD', 'DE30EUR', 'AUS2000', 'F40'
}

LONG_KEYWORDS = ['long', 'buy']
SHORT_KEYWORDS = ['short', 'sell']

# Channels that are always treated as scalps regardless of message content
SCALP_CHANNELS = {'scalps', 'gold-pa-signals', 'gold-tolls-map'}

# Expiry patterns
EXPIRY_PATTERNS = {
    'vth': 'week_end',
    'vtai': 'no_expiry',
    'alien': 'no_expiry',
    'vtd': 'day_end',
    'vtw': 'week_end',
    'vtwe': 'week_end',
    'vtm': 'month_end',
    'vtme': 'month_end',
    'valid till hit': 'no_expiry',
    'valid till week': 'week_end',
    'valid till day': 'day_end',
    'valid till month': 'month_end',
    'swing': 'week_end',
    'no expiry': 'no_expiry'
}

SPECIAL_KEYWORDS = [
    'hot', 'semi-swing', 'swing', 'scalp', 'swing-trade',
    'intraday', 'position'
]

STOCK_SKIP_WORDS = {
    'LONG', 'SHORT', 'BUY', 'SELL', 'VTH', 'VTAI', 'VTWE', 'VTD', 'VTME',
    'HOT', 'STOPS', 'SL', 'STOP', 'ALIEN', 'SCALP', 'SWING', 'INTRADAY',
    'POSITION', 'SEMI-SWING', 'DAY-TRADE', 'SWING-TRADE'
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def clean_message(message: str) -> str:
    """Clean and normalize message text"""
    cleaned = message.lower()
    cleaned = re.sub(r'[-—–]+', ' ', cleaned)
    cleaned = re.sub(r'[,/|]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text, excluding numbers inside blacklisted terms"""
    # Remove blacklisted terms
    blacklist = [
        "spx500usd", "nas100usd", "us30usd", "us2000usd",
        "jp225", "nas100", "us30", "spx500", "sp500", "us2000",
        "de30", "dax30", "ger30", "china50", "russel2000",
        "aus200", "f40", "cac40", "ftse100", "hk50", "asx200"
    ]

    for word in blacklist:
        text = re.sub(re.escape(word), "", text, flags=re.IGNORECASE)

    # Extract numbers
    numbers = re.findall(r"\d+\.?\d*", text)
    try:
        return [float(n) for n in numbers]
    except ValueError:
        return []


def scale_forex_numbers(numbers: List[float], instrument: str) -> List[float]:
    """Scale down large forex numbers if needed"""
    if instrument not in FOREX_PAIRS or instrument in HIGH_VALUE_INSTRUMENTS:
        return numbers

    # Only scale if numbers are large (> 10000)
    if any(n > 10000 for n in numbers):
        scaled = [n / 100000 for n in numbers]
        logger.debug(f"Scaled down large forex numbers for {instrument}")
        return scaled

    return numbers


def extract_words_with_boundaries(text: str) -> List[str]:
    """Extract words from text including alphanumeric patterns"""
    return re.findall(r'\b[a-z0-9]+\b', text.lower())


def validate_limits_and_stop(limits: List[float], stop_loss: float,
                             direction: str) -> bool:
    """Validate that limits and stop loss make sense for the direction"""
    if not limits:
        return False

    # For long: limits should be above stop
    if direction == 'long':
        return all(limit > stop_loss for limit in limits)
    # For short: limits should be below stop
    else:
        return all(limit < stop_loss for limit in limits)


# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def extract_instrument(text: str, channel_name: str,
                       channel_config: dict) -> Optional[str]:
    """Extract trading instrument with channel awareness"""
    text_lower = text.lower()

    # Check if this is a crypto-alt channel (has both "crypto" and "alt")
    is_crypto_alt = False
    if channel_name:
        channel_lower = channel_name.lower()
        is_crypto_alt = 'crypto' in channel_lower and 'alt' in channel_lower

    # Check channel configuration for default instrument
    if channel_name and channel_name in channel_config:
        channel_settings = channel_config[channel_name]
        default_instrument = channel_settings.get("default_instrument")

        if default_instrument:
            # Check if another instrument is explicitly mentioned
            other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
            if not other_instrument:
                logger.debug(f"Using default instrument {default_instrument}")
                return default_instrument
            else:
                logger.debug(f"Found explicit instrument {other_instrument}")
                return other_instrument

    # Fallback to channel name detection
    if channel_name:
        channel_based = _extract_from_channel_name(text_lower, channel_name, is_crypto_alt)
        if channel_based:
            return channel_based

    # Look for explicit instrument
    return _find_explicit_instrument(text_lower, is_crypto_alt)


def _extract_from_channel_name(text_lower: str, channel_name: str,
                               is_crypto_alt: bool = False) -> Optional[str]:
    """Extract instrument based on channel name"""
    channel_lower = channel_name.lower()

    # Crypto-alt channel - try to extract any word and append USDT
    if is_crypto_alt:
        alt_symbol = _extract_crypto_alt_symbol(text_lower)
        if alt_symbol:
            logger.debug(f"Crypto-alt channel: {alt_symbol} → {alt_symbol}USDT")
            return f"{alt_symbol}USDT"

    # Gold channel - default to XAUUSD if no other instrument found
    if 'gold' in channel_lower:
        other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
        if not other_instrument:
            logger.debug("Gold channel detected, defaulting to XAUUSD")
            return 'XAUUSD'
        return other_instrument

    # Oil channel - default to USOILSPOT unless IC mentioned
    elif 'oil' in channel_lower:
        if 'ic' in text_lower or 'xti' in text_lower:
            logger.debug("IC oil detected, using XTIUSD")
            return 'XTIUSD'
        other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
        if not other_instrument:
            logger.debug("Oil channel detected, defaulting to USOILSPOT")
            return 'USOILSPOT'
        return other_instrument

    return None


def _find_explicit_instrument(text_lower: str, is_crypto_alt: bool = False) -> Optional[str]:
    """Find explicitly mentioned instrument in text"""
    # For crypto-alt channels, try to find any potential symbol
    if is_crypto_alt:
        alt_symbol = _extract_crypto_alt_symbol(text_lower)
        if alt_symbol:
            logger.debug(f"Crypto-alt auto-append: {alt_symbol} → {alt_symbol}USDT")
            return f"{alt_symbol}USDT"

    # Check for crypto first (standard mappings for BTC, ETH, etc.)
    crypto_found = _find_crypto_symbol(text_lower)
    if crypto_found:
        return crypto_found

    # Check exact word matches for abbreviations
    words = extract_words_with_boundaries(text_lower)

    for word in words:
        if word in INSTRUMENT_MAPPINGS:
            # Make sure it's not part of a longer symbol
            pattern = r'\b' + re.escape(word) + r'\b'
            if re.search(pattern, text_lower):
                logger.debug(f"Found instrument: {word} -> {INSTRUMENT_MAPPINGS[word]}")
                return INSTRUMENT_MAPPINGS[word]

    # Check for full instrument names (6+ characters like 'eurusd')
    for pattern, instrument in INSTRUMENT_MAPPINGS.items():
        if len(pattern) >= 6:  # Full names
            if pattern in text_lower:
                logger.debug(f"Found full instrument: {pattern} -> {instrument}")
                return instrument

    return None


def _find_crypto_symbol(text_lower: str) -> Optional[str]:
    """Find crypto symbols in text"""
    crypto_keys = ['btc', 'eth', 'sol', 'bnb', 'ada', 'xrp', 'dot', 'doge']
    for crypto_key in crypto_keys:
        if re.search(r'\b' + crypto_key + r'\b', text_lower):
            return INSTRUMENT_MAPPINGS.get(crypto_key, crypto_key.upper() + 'USDT')
    return None


def _extract_crypto_alt_symbol(text_lower: str) -> Optional[str]:
    """
    Extract a potential crypto alt symbol from text for auto-USDT appending

    Used in crypto-alt channels to detect any ticker-like word and append USDT
    Example: "dash short" → extracts "dash" → becomes "DASHUSDT"
    """
    # Skip words that are clearly not crypto symbols
    skip_words = {
        'long', 'short', 'buy', 'sell', 'stop', 'stops', 'sl',
        'vth', 'vtai', 'vtwe', 'vtd', 'vtme', 'alien', 'hot',
        'scalp', 'swing', 'intraday', 'position', 'semi',
        'day', 'week', 'month', 'trade', 'limit', 'entry',
        'take', 'profit', 'loss', 'price', 'usdt', 'usd'
    }

    # Get all words from the text
    words = extract_words_with_boundaries(text_lower)

    # Look for a word that could be a crypto symbol
    for word in words:
        # Skip numbers
        if word.replace('.', '').isdigit():
            continue

        # Skip common trading terms
        if word in skip_words:
            continue

        # Skip very short words (< 2 chars) or very long (> 10 chars)
        if len(word) < 2 or len(word) > 10:
            continue

        # If word is all letters and 2-10 chars, it's probably a ticker
        if word.isalpha():
            logger.debug(f"Crypto-alt symbol detected: {word}")
            return word.upper()

    return None


def extract_direction(text: str) -> Optional[str]:
    """Extract trade direction from text"""
    text_lower = text.lower()

    for keyword in LONG_KEYWORDS:
        if re.search(r'\b' + keyword + r'\b', text_lower):
            return 'long'

    for keyword in SHORT_KEYWORDS:
        if re.search(r'\b' + keyword + r'\b', text_lower):
            return 'short'

    return None


def extract_expiry(text: str, channel_name: str,
                   channel_config: dict) -> str:
    """Extract expiry type with channel defaults"""
    text_lower = text.lower()

    # First check for explicit expiry patterns in the text
    for pattern, expiry_type in EXPIRY_PATTERNS.items():
        if pattern in text_lower:
            return expiry_type

    # If no explicit expiry, use channel default from config
    if channel_name and channel_name in channel_config:
        channel_settings = channel_config[channel_name]
        default_expiry = channel_settings.get("default_expiry", "day_end")
        logger.debug(f"Using default expiry {default_expiry} for {channel_name}")
        return default_expiry

    # Default expiry
    return 'day_end'


def extract_keywords(text: str) -> List[str]:
    """Extract special keywords from text"""
    text_lower = text.lower()
    keywords = []

    # Check for compound keywords first
    compound_keywords = ['semi-swing', 'day-trade', 'swing-trade', 'position-trade']
    for keyword in compound_keywords:
        if keyword in text_lower:
            keywords.append(keyword)
        elif keyword.replace('-', ' ') in text_lower:
            keywords.append(keyword)

    # Then check single keywords
    for keyword in SPECIAL_KEYWORDS:
        if keyword in text_lower and keyword not in keywords:
            # Don't add 'swing' if 'semi-swing' is already added
            if keyword == 'swing' and 'semi-swing' in keywords:
                continue
            keywords.append(keyword)

    return keywords


def determine_limits_and_stop(numbers: List[float], direction: str,
                             channel_name: str = None) -> tuple:
    """
    Determine which numbers are limits and which is stop loss

    Special handling for tolls channel:
    - All numbers are treated as limits
    - Stop loss is automatically set to ±5 from the appropriate limit:
      * Long: min(limits) - 5 (5 below the lowest limit)
      * Short: max(limits) + 5 (5 above the highest limit)
    """
    # Check if this is the tolls channel
    is_tolls_channel = channel_name and 'toll' in channel_name.lower()

    if is_tolls_channel:
        # For tolls channel: all numbers are limits, auto-set stop loss
        if len(numbers) < 1:
            return None, None

        limits = numbers
        # Set automatic stop loss based on direction
        if direction == 'long':
            # For long: stop is 5 below the lowest limit
            lowest_limit = min(limits)
            stop_loss = lowest_limit - 5.0
        else:  # short
            # For short: stop is 5 above the highest limit
            highest_limit = max(limits)
            stop_loss = highest_limit + 5.0

        logger.debug(f"Tolls channel: Using all {len(limits)} number(s) as limits, "
                    f"auto-setting stop to {stop_loss} ({direction})")
        return limits, stop_loss

    # Normal channel logic (requires at least 2 numbers)
    if len(numbers) < 2:
        return None, None

    # Try last number as stop loss (most common pattern)
    stop_loss = numbers[-1]
    limits = numbers[:-1]

    if validate_limits_and_stop(limits, stop_loss, direction):
        return limits, stop_loss

    # Try first number as stop loss (alternative pattern)
    stop_loss = numbers[0]
    limits = numbers[1:]

    if validate_limits_and_stop(limits, stop_loss, direction):
        return limits, stop_loss

    # If neither works, try to find the most logical stop
    if direction == 'long':
        stop_loss = min(numbers)
        limits = [n for n in numbers if n != stop_loss]
    else:
        stop_loss = max(numbers)
        limits = [n for n in numbers if n != stop_loss]

    if limits and validate_limits_and_stop(limits, stop_loss, direction):
        return limits, stop_loss

    return None, None


def is_scalp(text: str, channel_name: str = None) -> bool:
    """
    Determine if a signal is a scalp.

    Returns True if:
    - The channel is in SCALP_CHANNELS, OR
    - The message text contains the word 'scalp'
    """
    if channel_name and channel_name.lower() in SCALP_CHANNELS:
        return True
    if re.search(r'\bscalp\b', text, re.IGNORECASE):
        return True
    return False


# ============================================================================
# CORE PATTERN PARSER
# ============================================================================

class CorePatternParser:
    """
    Pattern-based parser for forex, gold, indices, and other core instruments

    This is the main parser used for most channels.
    """

    def __init__(self, channel_config: dict = None):
        self.channel_config = channel_config or {}
        logger.info("Initialized CorePatternParser")

    def parse(self, message: str, channel_name: str = None,
              _internal_call: bool = False) -> Optional[ParsedSignal]:
        """
        Parse using pattern matching for core instruments

        Args:
            message: The message to parse
            channel_name: Channel name for context
            _internal_call: Internal flag to suppress logging when called by subclass

        Returns:
            ParsedSignal or None
        """
        try:
            # Clean the message
            cleaned = clean_message(message)

            # Extract numbers
            numbers = extract_numbers(cleaned)

            # Check if this is the tolls channel
            is_tolls_channel = channel_name and 'toll' in channel_name.lower()

            # For tolls channel, allow single number (just a limit, no stop)
            # For regular channels, require at least 2 numbers (limits + stop)
            min_numbers = 1 if is_tolls_channel else 2

            if len(numbers) < min_numbers:
                if not _internal_call:
                    logger.debug(f"Not enough numbers found (need {min_numbers}, got {len(numbers)})")
                return None

            # Extract instrument
            instrument = extract_instrument(cleaned, channel_name, self.channel_config)
            if not instrument:
                if not _internal_call:
                    logger.debug(f"No instrument found for channel {channel_name}")
                return None

            # Scale numbers if needed for forex
            numbers = scale_forex_numbers(numbers, instrument)
            if not numbers:
                if not _internal_call:
                    logger.warning("No numbers after scaling")
                return None

            # Extract direction
            direction = extract_direction(cleaned)
            if not direction:
                if not _internal_call:
                    logger.debug("No direction found")
                return None

            # Determine limits and stop loss (pass channel_name for tolls handling)
            limits, stop_loss = determine_limits_and_stop(numbers, direction, channel_name)
            if not limits or stop_loss is None:
                if not _internal_call:
                    logger.debug("Could not determine limits and stop loss")
                return None

            # Extract expiry
            expiry_type = extract_expiry(cleaned, channel_name, self.channel_config)

            # Extract keywords
            keywords = extract_keywords(cleaned)

            # Determine if this is a scalp
            scalp = is_scalp(message, channel_name)

            # Create signal
            signal = ParsedSignal(
                instrument=instrument,
                direction=direction,
                limits=sorted(limits, reverse=(direction == 'long')),
                stop_loss=stop_loss,
                expiry_type=expiry_type,
                raw_text=message,
                parse_method='core',
                keywords=keywords,
                channel_name=channel_name,
                scalp=scalp
            )

            # Validate before returning
            if validate_signal(signal):
                if not _internal_call:
                    logger.info(f"Core parse success: {signal.instrument} {signal.direction}")
                return signal

            if not _internal_call:
                logger.debug(f"Signal validation failed")
            return None

        except Exception as e:
            if not _internal_call:
                logger.error(f"Core parsing error: {e}")
            return None


# ============================================================================
# STOCK PATTERN PARSER
# ============================================================================

class StockPatternParser:
    """
    Stock-specific parser with MT5 integration for symbol lookup
    """

    def __init__(self, channel_config: dict = None):
        self.channel_config = channel_config or {}
        self.mt5_initialized = False
        self.available_symbols: Set[str] = set()
        self._initialize_mt5()
        logger.info("Initialized StockPatternParser")

    def _initialize_mt5(self):
        """Initialize MT5 connection for symbol checking"""
        if not MT5_AVAILABLE:
            logger.warning("MT5 module not available, stock parsing disabled")
            return

        try:
            if not mt5.initialize():
                logger.warning("MT5 initialization failed, stock parsing disabled")
                return

            # Get all available symbols
            symbols = mt5.symbols_get()
            if symbols:
                self.available_symbols = {s.name for s in symbols}
                self.mt5_initialized = True
                logger.info(f"MT5 initialized with {len(self.available_symbols)} symbols")
            else:
                logger.warning("No symbols retrieved from MT5")

        except Exception as e:
            logger.error(f"MT5 initialization error: {e}")
            self.mt5_initialized = False

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse a stock trading signal

        Args:
            message: The message to parse (preserves case for stock symbols)
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
        if not self.mt5_initialized:
            logger.warning("MT5 not initialized, cannot parse stocks")
            return None

        try:
            # Clean message for everything except stock extraction
            cleaned = clean_message(message)

            # Extract numbers
            numbers = extract_numbers(cleaned)
            if len(numbers) < 2:
                return None

            # Extract stock symbol from ORIGINAL message (preserves case)
            instrument = self._extract_stock_symbol(message)
            if not instrument:
                logger.debug("No stock symbol found")
                return None

            # Don't scale stock prices (they're already correct)

            # Extract direction from cleaned message
            direction = extract_direction(cleaned)
            if not direction:
                return None

            # Determine limits and stop loss (pass channel_name for tolls handling)
            limits, stop_loss = determine_limits_and_stop(numbers, direction, channel_name)
            if not limits or stop_loss is None:
                return None

            # Extract expiry
            expiry_type = extract_expiry(cleaned, channel_name, self.channel_config)

            # Extract keywords
            keywords = extract_keywords(cleaned)

            # Determine if this is a scalp
            scalp = is_scalp(message, channel_name)

            # Create signal
            signal = ParsedSignal(
                instrument=instrument,
                direction=direction,
                limits=sorted(limits, reverse=(direction == 'long')),
                stop_loss=stop_loss,
                expiry_type=expiry_type,
                raw_text=message,
                parse_method='stock',
                keywords=keywords,
                channel_name=channel_name,
                scalp=scalp
            )

            # Validate before returning
            if validate_signal(signal):
                logger.info(f"Stock parse success: {signal.instrument} {signal.direction}")
                return signal

            return None

        except Exception as e:
            logger.error(f"Stock parsing error: {e}")
            return None

    def _extract_stock_symbol(self, text: str) -> Optional[str]:
        """Extract stock symbol using MT5 integration"""
        if not self.mt5_initialized:
            return None

        # Get words from text
        words_original = text.split()
        words_upper = [w.upper() for w in words_original]

        # Get only stock symbols from available symbols
        stock_symbols = [
            s for s in self.available_symbols
            if s.endswith(('.NYSE', '.NAS', '.NASDAQ'))
        ]

        if not stock_symbols:
            logger.warning("No stock symbols found in MT5")
            return None

        # Step 1: Direct ticker match
        for word in words_upper:
            if word in STOCK_SKIP_WORDS:
                continue

            # Check if this word is a ticker
            for symbol in stock_symbols:
                ticker = symbol.split('.')[0]
                if word == ticker:
                    logger.info(f"Found exact ticker match: {word} -> {symbol}")
                    return symbol

        # Step 2: Check with exchange suffix
        for word in words_upper:
            if word in stock_symbols:
                logger.info(f"Found symbol with exchange: {word}")
                return word

        # Step 3: Description matching
        matches = self._find_by_description(text, stock_symbols)

        if len(matches) == 1:
            match = matches[0]
            logger.info(f"Single description match: {match['symbol']}")
            return match['symbol']
        elif len(matches) > 1:
            # Try to find best match
            best = self._select_best_match(matches)
            if best:
                logger.info(f"Selected best match: {best['symbol']}")
                return best['symbol']

        return None

    def _find_by_description(self, text: str,
                             stock_symbols: List[str]) -> List[Dict]:
        """Find stocks by description matching"""
        # Get meaningful words for search
        words_lower = [
            w.lower() for w in text.split()
            if len(w) >= 3 and not w.replace('.', '').isdigit()
               and w.upper() not in STOCK_SKIP_WORDS
        ]

        if not words_lower:
            return []

        matches = []

        for symbol in stock_symbols:
            try:
                symbol_info = mt5.symbol_info(symbol)
                if not symbol_info or not symbol_info.description:
                    continue

                description_lower = symbol_info.description.lower()

                # Check if any search word is in description
                for word in words_lower:
                    if word in description_lower:
                        matches.append({
                            'symbol': symbol,
                            'description': symbol_info.description,
                            'matched_word': word
                        })
                        break

            except Exception as e:
                logger.debug(f"Error getting info for {symbol}: {e}")
                continue

        return matches

    def _select_best_match(self, matches: List[Dict]) -> Optional[Dict]:
        """Select the best match from multiple candidates"""
        if not matches:
            return None

        best_match = None
        best_score = 0

        for match in matches:
            # Score based on word length and exact matches
            score = len(match['matched_word'])

            # Bonus for exact word match in description
            description_words = match['description'].lower().split()
            if match['matched_word'] in description_words:
                score += 10

            if score > best_score:
                best_score = score
                best_match = match

        # Only return if we have a strong match
        if best_match and best_score >= 10:
            return best_match

        return None

    def cleanup(self):
        """Cleanup MT5 connection"""
        if self.mt5_initialized:
            mt5.shutdown()
            logger.info("MT5 connection closed")


# ============================================================================
# CRYPTO PATTERN PARSER
# ============================================================================

class CryptoPatternParser(CorePatternParser):
    """
    Crypto-specific parser (inherits from CorePatternParser)

    Crypto parsing is essentially the same as core parsing, but with
    crypto-specific defaults and handling.
    """

    def __init__(self, channel_config: dict = None):
        super().__init__(channel_config)
        logger.info("Initialized CryptoPatternParser")

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse using crypto-specific logic

        Args:
            message: The message to parse
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
        # Use parent's parse method with _internal_call flag to suppress duplicate logs
        result = super().parse(message, channel_name, _internal_call=True)

        # If successful, update parse method to 'crypto'
        if result:
            result.parse_method = 'crypto'
            logger.info(f"Crypto parse success: {result.instrument} {result.direction}")

        return result