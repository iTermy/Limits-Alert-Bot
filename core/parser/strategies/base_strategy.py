"""
Base strategy class for parsing strategies
"""
from abc import ABC, abstractmethod
from typing import Optional, List
from ..base import ParsedSignal, ParsingStrategy
from ..extractors import (
    InstrumentExtractor, DirectionExtractor,
    ExpiryExtractor, KeywordExtractor
)
from ..validators import SignalValidator
from utils.logger import get_logger

logger = get_logger("parser.strategies.base")


class BaseParsingStrategy(ParsingStrategy):
    """Base implementation of parsing strategy with common functionality"""

    def __init__(self, channel_config: dict = None):
        """
        Initialize the strategy

        Args:
            channel_config: Channel configuration dictionary
        """
        self.channel_config = channel_config or {}
        self.instrument_extractor = InstrumentExtractor(channel_config)
        self.direction_extractor = DirectionExtractor()
        self.expiry_extractor = ExpiryExtractor(channel_config)
        self.keyword_extractor = KeywordExtractor()
        self.validator = SignalValidator()

    @abstractmethod
    def can_parse(self, message: str, channel_name: str = None) -> bool:
        """Check if this strategy can parse the message"""
        pass

    @abstractmethod
    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """Parse the message and return a ParsedSignal or None"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this parsing strategy"""
        pass

    def validate_signal(self, signal: Optional[ParsedSignal]) -> bool:
        """
        Validate a parsed signal

        Args:
            signal: The signal to validate

        Returns:
            True if signal is valid
        """
        return self.validator.validate(signal)

    def extract_instrument(self, text: str, channel_name: str = None) -> Optional[str]:
        """
        Extract instrument from text

        Args:
            text: The text to parse
            channel_name: Channel name for context

        Returns:
            Extracted instrument or None
        """
        return self.instrument_extractor.extract(text, channel_name)

    def extract_direction(self, text: str) -> Optional[str]:
        """
        Extract direction from text

        Args:
            text: The text to parse

        Returns:
            'long', 'short', or None
        """
        return self.direction_extractor.extract(text)

    def extract_expiry(self, text: str, channel_name: str = None) -> str:
        """
        Extract expiry type from text

        Args:
            text: The text to parse
            channel_name: Channel name for context

        Returns:
            Expiry type string
        """
        return self.expiry_extractor.extract(text, channel_name)

    def extract_keywords(self, text: str) -> List[str]:
        """
        Extract special keywords from text

        Args:
            text: The text to parse

        Returns:
            List of keywords
        """
        return self.keyword_extractor.extract(text)