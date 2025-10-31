"""
validators.py
Signal validation and channel detection for the trading signal parser
"""
import re
from typing import Optional, List, Tuple
from utils.logger import get_logger

logger = get_logger("parser.validators")

# ============================================================================
# MAIN VALIDATION FUNCTIONS (for __init__.py)
# ============================================================================

# Step 1: Check if message is a signal
def is_potential_signal(message: str, trading_keywords: List[str],
                        instrument_mappings: dict) -> bool:
    """
    Check if message could be a trading signal

    Args:
        message: The message to check
        trading_keywords: List of trading-related keywords
        instrument_mappings: Dictionary of instrument mappings

    Returns:
        True if message appears to be a signal
    """
    # Remove index symbols before number extraction
    # US30, SPX500, etc could interfere with numbers
    text = _remove_index_symbols(message)

    numbers = _extract_numbers(text)
    if len(numbers) < 2:
        return False

    # Check for trading-related keywords
    text_lower = text.lower()
    all_keywords = trading_keywords + list(instrument_mappings.keys())
    if not any(keyword in text_lower for keyword in all_keywords):
        return False

    return True


# Step 2: Exclude Keywords
def should_exclude(message: str, exclusion_keywords: List[str]) -> bool:
    """
    Check if message should be excluded based on keywords

    Args:
        message: The message to check
        exclusion_keywords: List of keywords that trigger exclusion

    Returns:
        True if message should be excluded
    """
    text_lower = message.lower()

    for keyword in exclusion_keywords:
        if re.search(r'\b' + keyword + r'\b', text_lower):
            logger.debug(f"Excluding message due to keyword: {keyword}")
            return True

    return False


# Step 3: Validate signal
def validate_signal(signal) -> bool:
    """
    Validate a complete parsed signal

    Args:
        signal: ParsedSignal object to validate

    Returns:
        True if signal is valid
    """
    if not signal:
        logger.debug("No signal to validate")
        return False

    # Must have all required fields
    if not all([signal.instrument, signal.direction, signal.limits,
                signal.stop_loss is not None]):
        logger.debug("Missing required fields in signal")
        return False

    # Instrument must be valid
    if not validate_instrument(signal.instrument):
        return False

    return True


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def validate_instrument(instrument: str, forbidden_instruments: set = None) -> bool:
    """
    Validate an instrument

    Args:
        instrument: The instrument to validate
        forbidden_instruments: Set of forbidden instruments (optional)

    Returns:
        True if instrument is valid
    """
    if not instrument:
        return False

    forbidden = forbidden_instruments or {'DXY', 'NQ', 'ES', 'YM', 'RTY', 'VIX'}

    if instrument.upper() in forbidden:
        logger.debug(f"Rejecting forbidden instrument: {instrument}")
        return False

    return True


def detect_channel_type(channel_name: str) -> str:
    """
    Detect parser type based on channel name

    Args:
        channel_name: Discord channel name

    Returns:
        'stock', 'crypto', or 'core' (default)
    """
    if not channel_name:
        return 'core'

    channel_lower = channel_name.lower()

    if 'stock' in channel_lower or 'equity' in channel_lower or 'shares' in channel_lower:
        return 'stock'

    if 'crypto' in channel_lower:
        return 'crypto'

    return 'core'


def is_stock_channel(channel_name: str) -> bool:
    """Check if channel is for stocks"""
    return detect_channel_type(channel_name) == 'stock'


def is_crypto_channel(channel_name: str) -> bool:
    """Check if channel is for crypto"""
    return detect_channel_type(channel_name) == 'crypto'


def _remove_index_symbols(text: str) -> str:
    """Remove index symbols to prevent number extraction from them"""
    blacklist = [
        "spx500usd", "nas100usd", "us30usd", "us2000usd",
        "jp225", "nas100", "us30", "spx500", "sp500", "us2000",
        "de30", "dax30", "ger30", "china50", "russel2000",
        "aus200", "f40", "cac40", "ftse100", "hk50", "asx200"
    ]

    for symbol in blacklist:
        text = re.sub(re.escape(symbol), "", text, flags=re.IGNORECASE)

    return text


def _extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text"""
    try:
        numbers_str = re.findall(r'\d+\.?\d*', text)
        return [float(n) for n in numbers_str]
    except ValueError:
        return []


def _extract_direction_quick(text_lower: str) -> Optional[str]:
    """Quick direction extraction for validation"""
    if re.search(r'\b(long|buy)\b', text_lower):
        return 'long'
    if re.search(r'\b(short|sell)\b', text_lower):
        return 'short'
    return None


def _separate_limits_and_stop(numbers: List[float], direction: str) -> Tuple[List[float], Optional[float]]:
    """
    Separate limit prices from stop loss

    For long: stop is lowest number (limits > stop)
    For short: stop is highest number (limits < stop)

    Args:
        numbers: All extracted numbers
        direction: Trade direction

    Returns:
        Tuple of (limits, stop_loss)
    """
    if not numbers or len(numbers) < 2:
        return numbers, None

    stop_loss = numbers[-1]
    limits = numbers[:-1]

    return limits, stop_loss