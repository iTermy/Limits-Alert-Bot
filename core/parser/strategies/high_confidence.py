"""
High-confidence pattern-based parsing strategy
"""
import re
from typing import Optional, List
from .base_strategy import BaseParsingStrategy
from ..base import ParsedSignal
from ..constants import FOREX_PAIRS, HIGH_VALUE_INSTRUMENTS
from ..utils import (
    clean_message, extract_numbers, scale_forex_numbers,
    validate_limits_and_stop
)
from utils.logger import get_logger

logger = get_logger("parser.strategies.high_confidence")


class HighConfidenceStrategy(BaseParsingStrategy):
    """Pattern-based high-confidence parsing strategy"""

    @property
    def name(self) -> str:
        return "high_confidence"

    def can_parse(self, message: str, channel_name: str = None) -> bool:
        """
        Check if this strategy can parse the message

        High confidence requires:
        - At least 2 numbers
        - Clear direction keywords
        - Recognizable instrument or channel default
        """
        if not message or len(message) < 5:
            return False

        # Clean and extract numbers
        cleaned = clean_message(message)
        numbers = extract_numbers(cleaned)

        if len(numbers) < 2:
            return False

        # Must have direction
        if not self.extract_direction(cleaned):
            return False

        # Must have or infer instrument
        if not self.extract_instrument(cleaned, channel_name):
            return False

        return True

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse using high-confidence pattern matching

        Args:
            message: The message to parse
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
        try:
            # Clean the message
            cleaned = clean_message(message)

            # Extract numbers
            numbers = extract_numbers(cleaned)
            if len(numbers) < 2:
                return None

            # Extract instrument
            instrument = self.extract_instrument(cleaned, channel_name)
            if not instrument:
                logger.debug(f"No instrument found for channel {channel_name}")
                return None
            if instrument:
                logger.info(f"instrument is {instrument}")

            # Scale numbers if needed for forex
            numbers = scale_forex_numbers(
                numbers, instrument, FOREX_PAIRS, HIGH_VALUE_INSTRUMENTS
            )
            if numbers:
                logger.info(f"numbers is {numbers}")
            if not numbers:
                logger.warning(f"No numbers found")

            # Extract direction
            direction = self.extract_direction(cleaned)
            if not direction:
                return None
            logger.info(f"Direction is {direction}")

            # Determine limits and stop loss
            limits, stop_loss = self._determine_limits_and_stop(numbers, direction)
            logger.info(f'limits is {limits} and stop_loss is {stop_loss}')

            if not limits or stop_loss is None:
                logger.info(f"Stop loss is {stop_loss}" and limits is {limits})
                return None

            # Extract expiry
            expiry_type = self.extract_expiry(cleaned, channel_name)
            logger.info(f"expiry_type is {expiry_type}")

            # Extract keywords
            keywords = self.extract_keywords(cleaned)
            logger.info(f"keywords is {keywords}")

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
                logger.info(f"High-confidence parse success: {signal.instrument} {signal.direction}")
                return signal

            if not self.validate_signal(signal):
                logger.info(f'Could not validate {signal}')

        except Exception as e:
            return None

    def _determine_limits_and_stop(self, numbers: List[float], direction: str) -> tuple:
        """
        Determine which numbers are limits and which is stop loss

        Args:
            numbers: List of price numbers
            direction: Trade direction

        Returns:
            Tuple of (limits, stop_loss) or (None, None)
        """
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
        # For long: stop should be lowest number
        # For short: stop should be highest number
        if direction == 'long':
            stop_loss = min(numbers)
            limits = [n for n in numbers if n != stop_loss]
        else:
            stop_loss = max(numbers)
            limits = [n for n in numbers if n != stop_loss]

        if limits and validate_limits_and_stop(limits, stop_loss, direction):
            return limits, stop_loss

        return None, None