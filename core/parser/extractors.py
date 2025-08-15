"""
Extraction methods for parsing trading signals
"""
import re
from typing import List, Optional
from utils.logger import get_logger
from .constants import (
    INSTRUMENT_MAPPINGS, LONG_KEYWORDS, SHORT_KEYWORDS,
    EXPIRY_PATTERNS, SPECIAL_KEYWORDS
)
from .utils import extract_words_with_boundaries

logger = get_logger("parser.extractors")


class InstrumentExtractor:
    """Extracts trading instruments from text"""

    def __init__(self, channel_config: dict = None):
        self.channel_config = channel_config or {}

    def extract(self, text: str, channel_name: str = None) -> Optional[str]:
        """
        Extract trading instrument with channel awareness

        Args:
            text: The text to parse
            channel_name: The channel name for context

        Returns:
            The extracted instrument or None
        """
        text_lower = text.lower()

        # Check if this is a stock channel
        is_stock_channel = self._is_stock_channel(channel_name)
        if is_stock_channel:
            # Stock extraction is handled by a separate strategy
            return None

        # Check channel configuration for default instrument
        if channel_name and channel_name in self.channel_config:
            channel_settings = self.channel_config[channel_name]
            default_instrument = channel_settings.get("default_instrument")

            if default_instrument:
                # Check if another instrument is explicitly mentioned
                other_instrument = self._find_explicit_instrument(text_lower)
                if not other_instrument:
                    logger.debug(f"Using default instrument {default_instrument} for channel {channel_name}")
                    return default_instrument
                else:
                    logger.debug(f"Found explicit instrument {other_instrument}, overriding channel default")
                    return other_instrument

        # Fallback to channel name detection if no config
        if channel_name:
            channel_based = self._extract_from_channel_name(text_lower, channel_name)
            if channel_based:
                return channel_based

        # Look for explicit instrument
        return self._find_explicit_instrument(text_lower)

    def _is_stock_channel(self, channel_name: str) -> bool:
        """Check if this is a stock channel"""
        if not channel_name:
            return False

        channel_lower = channel_name.lower()
        return 'stock' in channel_lower or 'equity' in channel_lower or 'shares' in channel_lower

    def _extract_from_channel_name(self, text_lower: str, channel_name: str) -> Optional[str]:
        """Extract instrument based on channel name"""
        channel_lower = channel_name.lower()

        # Gold channel - default to XAUUSD if no other instrument found
        if 'gold' in channel_lower:
            other_instrument = self._find_explicit_instrument(text_lower)
            if not other_instrument:
                logger.debug(f"Gold channel detected, defaulting to XAUUSD")
                return 'XAUUSD'
            return other_instrument

        # Oil channel - default to USOILSPOT unless IC mentioned
        elif 'oil' in channel_lower:
            if 'ic' in text_lower or 'xti' in text_lower:
                logger.debug(f"IC oil detected, using XTIUSD")
                return 'XTIUSD'
            other_instrument = self._find_explicit_instrument(text_lower)
            if not other_instrument:
                logger.debug(f"Oil channel detected, defaulting to USOILSPOT")
                return 'USOILSPOT'
            return other_instrument

        return None

    def _find_explicit_instrument(self, text_lower: str) -> Optional[str]:
        """Find explicitly mentioned instrument in text"""
        # Check for crypto first
        crypto_found = self._find_crypto_symbol(text_lower)
        if crypto_found:
            return crypto_found

        # Check exact word matches for abbreviations
        words = extract_words_with_boundaries(text_lower)

        for word in words:
            if word in INSTRUMENT_MAPPINGS:
                # Make sure it's not part of a longer symbol
                pattern = r'\b' + re.escape(word) + r'\b'
                if re.search(pattern, text_lower):
                    logger.debug(f"Found instrument mapping: {word} -> {INSTRUMENT_MAPPINGS[word]}")
                    return INSTRUMENT_MAPPINGS[word]

        # Check for full instrument names (6+ characters like 'eurusd')
        for pattern, instrument in INSTRUMENT_MAPPINGS.items():
            if len(pattern) >= 6:  # Full names like 'eurusd'
                if pattern in text_lower:
                    logger.debug(f"Found full instrument name: {pattern} -> {instrument}")
                    return instrument

        return None

    def _find_crypto_symbol(self, text_lower: str) -> Optional[str]:
        """Find crypto symbols in text"""
        crypto_keys = ['btc', 'eth', 'sol', 'bnb', 'ada', 'xrp', 'dot', 'doge']
        for crypto_key in crypto_keys:
            if re.search(r'\b' + crypto_key + r'\b', text_lower):
                return INSTRUMENT_MAPPINGS.get(crypto_key, crypto_key.upper() + 'USDT')
        return None


class DirectionExtractor:
    """Extracts trade direction from text"""

    @staticmethod
    def extract(text: str) -> Optional[str]:
        """
        Extract trade direction from text

        Args:
            text: The text to parse

        Returns:
            'long', 'short', or None
        """
        text_lower = text.lower()

        for keyword in LONG_KEYWORDS:
            if re.search(r'\b' + keyword + r'\b', text_lower):
                return 'long'

        for keyword in SHORT_KEYWORDS:
            if re.search(r'\b' + keyword + r'\b', text_lower):
                return 'short'

        return None


class ExpiryExtractor:
    """Extracts expiry type from text"""

    def __init__(self, channel_config: dict = None):
        self.channel_config = channel_config or {}

    def extract(self, text: str, channel_name: str = None) -> str:
        """
        Extract expiry type with channel defaults

        Args:
            text: The text to parse
            channel_name: The channel name for context

        Returns:
            The expiry type
        """
        text_lower = text.lower()

        # First check for explicit expiry patterns in the text
        for pattern, expiry_type in EXPIRY_PATTERNS.items():
            if pattern in text_lower:
                return expiry_type

        # If no explicit expiry, use channel default from config
        if channel_name and channel_name in self.channel_config:
            channel_settings = self.channel_config[channel_name]
            default_expiry = channel_settings.get("default_expiry", "day_end")
            logger.debug(f"Using default expiry {default_expiry} for channel {channel_name}")
            return default_expiry

        # Default expiry
        return 'day_end'


class KeywordExtractor:
    """Extracts special keywords from text"""

    @staticmethod
    def extract(text: str) -> List[str]:
        """
        Extract special keywords from text

        Args:
            text: The text to parse

        Returns:
            List of keywords found
        """
        text_lower = text.lower()
        keywords = []

        # Check for compound keywords first (order matters - check longer phrases first)
        compound_keywords = ['semi-swing', 'day-trade', 'swing-trade', 'position-trade']
        for keyword in compound_keywords:
            # Check with hyphen
            if keyword in text_lower:
                keywords.append(keyword)
            # Also check without hyphen
            elif keyword.replace('-', ' ') in text_lower:
                keywords.append(keyword)

        # Then check single keywords (but avoid duplicates)
        for keyword in SPECIAL_KEYWORDS:
            if keyword in text_lower and keyword not in keywords:
                # Don't add 'swing' if 'semi-swing' is already added
                if keyword == 'swing' and 'semi-swing' in keywords:
                    continue
                keywords.append(keyword)

        return keywords