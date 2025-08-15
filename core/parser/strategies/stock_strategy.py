"""
Stock-specific parsing strategy with MT5 integration
"""
import re
import MetaTrader5 as mt5
from typing import Optional, List, Dict, Set
from .high_confidence import HighConfidenceStrategy
from ..base import ParsedSignal
from ..constants import STOCK_SKIP_WORDS, FOREX_PAIRS, HIGH_VALUE_INSTRUMENTS
from ..utils import clean_message, extract_numbers, scale_forex_numbers
from utils.logger import get_logger

logger = get_logger("parser.strategies.stock")


class StockParsingStrategy(HighConfidenceStrategy):
    """Strategy for parsing stock trading signals"""

    def __init__(self, channel_config: dict = None):
        super().__init__(channel_config)
        self.mt5_initialized = False
        self.available_symbols: Set[str] = set()
        self._initialize_mt5()

    @property
    def name(self) -> str:
        return "stock"

    def _initialize_mt5(self):
        """Initialize MT5 connection for symbol checking"""
        try:
            if not mt5.initialize():
                logger.warning("MT5 initialization failed, stock symbol checking disabled")
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

    def can_parse(self, message: str, channel_name: str = None) -> bool:
        """
        Check if this is a stock signal that we can parse

        Args:
            message: The message to check
            channel_name: The channel name

        Returns:
            True if this is a stock channel and we can parse it
        """
        if not self.mt5_initialized:
            return False

        if not channel_name:
            logger.error(f'No channel_name found')
            return False

        # Check if this is a stock channel
        if not self._is_stock_channel(channel_name):
            return False

        # For stock channels, check basic requirements
        # Don't use cleaned message for stock extraction as we need original case
        cleaned = clean_message(message)

        # Must have numbers
        numbers = re.findall(r'(\d+\.?\d*)', cleaned)
        if len(numbers) < 2:
            return False

        # Must have direction
        if not self.extract_direction(cleaned):
            return False

        # Check if we can extract a stock symbol (use original message, not cleaned)
        stock_symbol = self._extract_stock_symbol(message)
        if not stock_symbol:
            logger.error(f'Could not extract stock symbol with high-confidence... using AI...')
            return False

        return True

    def _is_stock_channel(self, channel_name: str) -> bool:
        """Check if this is a stock channel"""
        channel_lower = channel_name.lower()
        return 'stock' in channel_lower or 'equity' in channel_lower or 'shares' in channel_lower

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse a stock trading signal

        Args:
            message: The message to parse (preserves case for stock symbols)
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
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
                logger.debug(f"No stock symbol found in message")
                return None

            # Don't scale stock prices (they're not forex)
            # Stock prices are already in the correct format

            # Extract direction from cleaned message
            direction = self.extract_direction(cleaned)
            if not direction:
                return None

            # Determine limits and stop loss
            limits, stop_loss = self._determine_limits_and_stop(numbers, direction)

            if not limits or stop_loss is None:
                return None

            # Extract expiry (stocks often use day_end by default)
            expiry_type = self.extract_expiry(cleaned, channel_name)

            # Extract keywords
            keywords = self.extract_keywords(cleaned)

            # Create signal
            signal = ParsedSignal(
                instrument=instrument,
                direction=direction,
                limits=sorted(limits, reverse=(direction == 'long')),
                stop_loss=stop_loss,
                expiry_type=expiry_type,
                raw_text=message,
                parse_method=self.name,
                keywords=keywords,
                channel_name=channel_name
            )

            # Validate before returning
            if self.validate_signal(signal):
                logger.info(f"Stock parse success: {signal.instrument} {signal.direction}")
                return signal

            return None

        except Exception as e:
            logger.error(f"Stock parsing failed: {e}")
            return None
        """
        Extract stock symbol from text
        
        Args:
            text: The text to parse
            channel_name: Channel name (unused but kept for interface)
            
        Returns:
            Stock symbol or None
        """
        return self._extract_stock_symbol(text)

    def _extract_stock_symbol(self, text: str) -> Optional[str]:
        """
        Extract stock symbol using MT5 integration

        Args:
            text: The text to parse

        Returns:
            Stock symbol with exchange suffix or None
        """
        if not self.mt5_initialized:
            return None

        # Get words from text
        words_original = text.split()
        words_upper = [w.upper() for w in words_original]

        # Get only stock symbols from available symbols
        stock_symbols = [s for s in self.available_symbols
                        if s.endswith(('.NYSE', '.NAS', '.NASDAQ'))]

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

    def _find_by_description(self, text: str, stock_symbols: List[str]) -> List[Dict]:
        """
        Find stocks by description matching

        Args:
            text: The text to search in
            stock_symbols: List of available stock symbols

        Returns:
            List of matching stocks with metadata
        """
        # Get meaningful words for search
        words_lower = [w.lower() for w in text.split()
                      if len(w) >= 3 and not w.replace('.', '').isdigit()
                      and w.upper() not in STOCK_SKIP_WORDS]

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
        """
        Select the best match from multiple candidates

        Args:
            matches: List of match dictionaries

        Returns:
            Best match or None
        """
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