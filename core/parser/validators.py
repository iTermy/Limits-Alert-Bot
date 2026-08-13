"""
validators.py
Signal validation and channel detection for the trading signal parser
"""

import re
from typing import Optional

from utils.logger import get_logger

logger = get_logger("parser.validators")

# ============================================================================
# CONSTANTS
# ============================================================================

# Channels that treat every number as a limit and derive the stop loss from the
# gold-tolls offset (default $5 from the last limit) instead of reading an
# explicit SL from the message. The toll channels (except general-tolls, which
# has its own per-instrument auto-SL) match by substring; anything else is
# listed here explicitly.
_GOLD_TOLLS_SL_CHANNELS = {"risky-gold"}

# Channels whose signals execute at the current market price instead of waiting
# for a limit. The message carries an explicit stop loss and take profit; the
# entry is resolved from the live feed when the signal is saved.
_INSTANT_ENTRY_CHANNELS = {"semi-swing-pa-signals"}


def uses_gold_tolls_sl(channel_name: Optional[str]) -> bool:
    """True for channels that auto-derive SL from the gold-tolls offset."""
    if not channel_name:
        return False
    name = channel_name.lower()
    if name in _GOLD_TOLLS_SL_CHANNELS:
        return True
    return "toll" in name and name != "general-tolls"


def uses_instant_entry(channel_name: Optional[str]) -> bool:
    """True for channels that enter at market with an explicit SL and TP."""
    return bool(channel_name) and channel_name.lower() in _INSTANT_ENTRY_CHANNELS

# Index tickers whose digits would otherwise be read as price levels. Sorted
# longest-first so a shorter entry can never eat part of a longer one (stripping
# "us2000" before "aus2000" would leave a stray "a0").
INDEX_SYMBOL_BLACKLIST = sorted(
    [
        "spx500usd",
        "nas100usd",
        "us30usd",
        "us2000usd",
        "jp225",
        "nas100",
        "us30",
        "spx500",
        "sp500",
        "us2000",
        "de30",
        "dax30",
        "ger30",
        "china50",
        "cn50",
        "russel2000",
        "aus2000",
        "aus200",
        "f40",
        "fr40",
        "cac40",
        "ftse100",
        "hk50",
        "hk33",
        "asx200",
        "gcq26",
    ],
    key=len,
    reverse=True,
)

# ============================================================================
# MAIN VALIDATION FUNCTIONS (for __init__.py)
# ============================================================================


# Step 1: Check if message is a signal
def is_potential_signal(
    message: str, trading_keywords: list[str], instrument_mappings: dict, channel_name: Optional[str] = None
) -> bool:
    """
    Check if message could be a trading signal

    Args:
        message: The message to check
        trading_keywords: List of trading-related keywords
        instrument_mappings: Dictionary of instrument mappings
        channel_name: Discord channel name (for tolls channel detection)

    Returns:
        True if message appears to be a signal
    """
    # Remove index symbols before number extraction
    # US30, SPX500, etc could interfere with numbers
    text = _remove_index_symbols(message)

    numbers = _extract_numbers(text)

    # Tolls-style channels (including risky-gold) auto-derive the stop loss, so a
    # single number is a valid signal — just a limit. Regular channels need at
    # least 2 numbers (limits + stop).
    min_numbers = 1 if uses_gold_tolls_sl(channel_name) else 2

    if len(numbers) < min_numbers:
        return False

    # Check for trading-related keywords
    text_lower = text.lower()
    all_keywords = trading_keywords + list(instrument_mappings.keys())
    return any(keyword in text_lower for keyword in all_keywords)


# Step 2: Exclude Keywords
def should_exclude(message: str, exclusion_keywords: list[str]) -> bool:
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
        if re.search(r"\b" + keyword + r"\b", text_lower):
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

    # Must have all required fields. Instant-entry signals carry no limits —
    # the entry is the market price at save time — but must name a take profit.
    if signal.instant_entry:
        required = [
            signal.instrument,
            signal.direction,
            signal.stop_loss is not None,
            signal.take_profit is not None,
        ]
    else:
        required = [
            signal.instrument,
            signal.direction,
            signal.limits,
            signal.stop_loss is not None,
        ]

    if not all(required):
        logger.debug("Missing required fields in signal")
        return False

    # Instrument must be valid
    return validate_instrument(signal.instrument)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def validate_instrument(instrument: str, forbidden_instruments: Optional[set] = None) -> bool:
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

    forbidden = forbidden_instruments or {"DXY", "NQ", "ES", "YM", "RTY", "VIX"}

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
        return "core"

    channel_lower = channel_name.lower()

    if "stock" in channel_lower or "equity" in channel_lower or "shares" in channel_lower:
        return "stock"

    if "crypto" in channel_lower:
        return "crypto"

    return "core"


def _remove_index_symbols(text: str) -> str:
    """Remove index symbols to prevent number extraction from them"""
    for symbol in INDEX_SYMBOL_BLACKLIST:
        text = re.sub(re.escape(symbol), "", text, flags=re.IGNORECASE)
    return text


def _extract_numbers(text: str) -> list[float]:
    """Extract all numbers from text"""
    try:
        numbers_str = re.findall(r"\d+\.?\d*", text)
        return [float(n) for n in numbers_str]
    except ValueError:
        return []
