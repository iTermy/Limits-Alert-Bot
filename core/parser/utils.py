"""
Utility functions for the signal parser
"""
import re
from typing import List, Optional
from utils.logger import get_logger

logger = get_logger("parser.utils")


def clean_message(message: str) -> str:
    """Clean and normalize message text"""
    # Convert to lowercase
    cleaned = message.lower()

    # Replace separators with spaces
    cleaned = re.sub(r'[-—–]+', ' ', cleaned)
    cleaned = re.sub(r'[,/|]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)

    return cleaned.strip()


import re
from typing import List


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text, excluding numbers inside blacklisted terms."""
    blacklist = ["spx500usd", "jp225", "nas100usd", "china50", "russel2000", "us30usd", "de30",
                 "dax30", "ger30", "spx500", "sp500", "nas100", "us2000", "us2000usd"]

    # Remove any blacklisted term (case-insensitive) before extracting
    for word in blacklist:
        text = re.sub(re.escape(word), "", text, flags=re.IGNORECASE)

    # Extract numbers
    numbers = re.findall(r"\d+\.?\d*", text)
    try:
        return [float(n) for n in numbers]
    except ValueError:
        return []


def scale_forex_numbers(numbers: List[float], instrument: str,
                        forex_pairs: set, high_value_instruments: set) -> List[float]:
    """
    Scale down large forex numbers if needed

    Args:
        numbers: List of numbers to potentially scale
        instrument: The trading instrument
        forex_pairs: Set of forex pair instruments
        high_value_instruments: Set of instruments that shouldn't be scaled

    Returns:
        Scaled numbers list
    """
    # Check if scaling is needed
    if instrument not in forex_pairs or instrument in high_value_instruments:
        return numbers

    # Only scale if numbers are large (> 10000)
    if any(n > 10000 for n in numbers):
        scaled = [n / 100000 for n in numbers]
        logger.debug(f"Scaled down large forex numbers for {instrument}")
        return scaled

    return numbers


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
    blacklist = ["spx500usd", "jp225", "nas100usd", "china50", "russel2000", "us30usd", "de30",
                 "dax30", "ger30", "spx500", "sp500", "nas100", "us30"]

    # Remove any blacklisted term (case-insensitive) before extracting
    for word in blacklist:
        text = re.sub(re.escape(word), "", message, flags=re.IGNORECASE)

    # Should have multiple numbers for limits and stop
    numbers = re.findall(r'\d+\.?\d*', text)
    if len(numbers) < 2:
        return False

    # Check for trading-related keywords
    text_lower = text.lower()
    all_keywords = trading_keywords + list(instrument_mappings.keys())

    return any(keyword in text_lower for keyword in all_keywords)


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

    for exclude_word in exclusion_keywords:
        # Use word boundaries to avoid false positives
        if re.search(r'\b' + exclude_word + r'\b', text_lower):
            logger.debug(f"Excluding message due to keyword: {exclude_word}")
            return True

    return False


def validate_limits_and_stop(limits: List[float], stop_loss: float, direction: str) -> bool:
    """
    Validate that limits and stop loss make sense for the direction

    Args:
        limits: Entry limit prices
        stop_loss: Stop loss price
        direction: Trade direction (long/short)

    Returns:
        True if the configuration is valid
    """
    if not limits:
        return False

    # For long: limits should be above stop
    if direction == 'long':
        return all(limit > stop_loss for limit in limits)
    # For short: limits should be below stop
    else:
        return all(limit < stop_loss for limit in limits)


def validate_price_distance(limits: List[float], stop_loss: float) -> bool:
    """
    Validate minimum distance between limits and stop loss

    Args:
        limits: Entry limit prices
        stop_loss: Stop loss price

    Returns:
        True if distances are valid
    """
    if not limits:
        return False

    # Check minimum distance between limits and stop
    min_distance = min(abs(limit - stop_loss) for limit in limits)

    # For forex (prices < 10), minimum 5 pips
    if stop_loss < 10:
        if min_distance < 0.0005:
            logger.debug("Limits too close to stop for forex")
            return False
    # For larger prices, proportional distance
    else:
        if min_distance < stop_loss * 0.0005:
            logger.debug("Limits too close to stop")
            return False

    return True


def extract_words_with_boundaries(text: str) -> List[str]:
    """
    Extract words from text including alphanumeric patterns

    Args:
        text: Input text

    Returns:
        List of words/tokens
    """
    # Include alphanumeric patterns to catch things like "jp225", "us30", etc.
    return re.findall(r'\b[a-z0-9]+\b', text.lower())