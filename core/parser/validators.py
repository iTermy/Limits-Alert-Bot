"""
Validation logic for parsed signals
"""
from typing import Optional
from utils.logger import get_logger
from .base import ParsedSignal
from .utils import validate_limits_and_stop, validate_price_distance

logger = get_logger("parser.validators")


class SignalValidator:
    """Validates parsed trading signals"""

    @staticmethod
    def validate(signal: Optional[ParsedSignal]) -> bool:
        """
        Validate a parsed signal

        Args:
            signal: The signal to validate

        Returns:
            True if signal is valid
        """
        if not signal:
            return False

        # Must have all required fields
        if not all([signal.instrument, signal.direction, signal.limits, signal.stop_loss]):
            logger.debug("Missing required fields in signal")
            return False

        # Instrument must be valid (not DXY, not futures unless we support it)
        if signal.instrument == 'DXY':
            logger.debug("Rejecting DXY signal")
            return False

        # Validate limit/stop relationship
        if not validate_limits_and_stop(signal.limits, signal.stop_loss, signal.direction):
            logger.debug(f"Invalid limit/stop relationship for {signal.direction}")
            return False

        # Validate price distances
        if not validate_price_distance(signal.limits, signal.stop_loss):
            logger.debug("Invalid price distances")
            return False

        return True

    @staticmethod
    def validate_instrument(instrument: str, forbidden_instruments: set = None) -> bool:
        """
        Validate an instrument

        Args:
            instrument: The instrument to validate
            forbidden_instruments: Set of forbidden instruments

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

    @staticmethod
    def validate_numbers(numbers: list, min_count: int = 2) -> bool:
        """
        Validate a list of numbers

        Args:
            numbers: List of numbers to validate
            min_count: Minimum required count

        Returns:
            True if numbers are valid
        """
        if not numbers or len(numbers) < min_count:
            return False

        # Check for reasonable values (not negative, not too extreme)
        for num in numbers:
            if num < 0:
                logger.debug(f"Negative number found: {num}")
                return False
            if num > 1000000:  # Arbitrary max to catch parsing errors
                logger.debug(f"Extremely large number found: {num}")
                return False

        return True