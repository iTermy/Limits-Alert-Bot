"""
Enhanced modular signal parser for trading signals
"""
from typing import Optional, List
from .base import ParsedSignal
from .strategies.high_confidence import HighConfidenceStrategy
from .strategies.ai_strategy import AIParsingStrategy
from .strategies.stock_strategy import StockParsingStrategy
from .utils import is_potential_signal, should_exclude
from .constants import TRADING_KEYWORDS, INSTRUMENT_MAPPINGS, EXCLUSION_KEYWORDS
from utils.logger import get_logger

logger = get_logger("parser")


class EnhancedSignalParser:
    """
    Main parser that orchestrates different parsing strategies
    """

    def __init__(self, config_loader=None):
        """
        Initialize the parser with configuration

        Args:
            config_loader: Configuration loader instance
        """
        # Load channel configuration
        self.channel_config = self._load_channel_config(config_loader)

        # Initialize parsing strategies in priority order
        self.strategies: List = [
            StockParsingStrategy(self.channel_config),  # Stock-specific first
            HighConfidenceStrategy(self.channel_config),  # Pattern-based
            AIParsingStrategy(self.channel_config)  # AI fallback
        ]

        logger.info(f"Initialized parser with {len(self.strategies)} strategies")

    def _load_channel_config(self, config_loader) -> dict:
        """Load channel configuration"""
        channel_config = {}

        if config_loader:
            try:
                channels_data = config_loader.load("channels.json")
                channel_config = channels_data.get("channel_settings", {})
                logger.info(f"Loaded channel configuration for {len(channel_config)} channels")
            except Exception as e:
                logger.warning(f"Could not load channel configuration: {e}")
        else:
            # Try to load directly
            try:
                from utils.config_loader import config
                channels_data = config.load("channels.json")
                channel_config = channels_data.get("channel_settings", {})
                logger.info(f"Loaded channel configuration for {len(channel_config)} channels")
            except Exception as e:
                logger.warning(f"Could not load channel configuration: {e}")

        return channel_config

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse a trading signal using multi-tier approach

        Args:
            message: Raw message text
            channel_name: Name of the Discord channel

        Returns:
            ParsedSignal object or None if parsing fails
        """
        if not message or len(message) < 5:
            return None

        # Step 1: Pre-validation - is this a potential signal?
        if not is_potential_signal(message, TRADING_KEYWORDS, INSTRUMENT_MAPPINGS):
            logger.debug(f"Message doesn't appear to be a signal: {message[:50]}...")
            return None

        # Step 2: Check for exclusions
        if should_exclude(message, EXCLUSION_KEYWORDS):
            logger.debug(f"Message contains excluded content: {message[:50]}...")
            return None

        # Step 3: Try each strategy in order
        for strategy in self.strategies:
            try:
                if strategy.can_parse(message, channel_name):
                    result = strategy.parse(message, channel_name)
                    if result:
                        logger.info(
                            f"Parse success using {strategy.name}: "
                            f"{result.instrument} {result.direction}"
                        )
                        return result
            except Exception as e:
                logger.error(f"Error in {strategy.name} strategy: {e}")
                continue

        logger.debug(f"Failed to parse signal: {message[:100]}...")
        return None

    def cleanup(self):
        """Cleanup resources (e.g., MT5 connections)"""
        for strategy in self.strategies:
            if hasattr(strategy, 'cleanup'):
                try:
                    strategy.cleanup()
                except Exception as e:
                    logger.error(f"Error cleaning up {strategy.name}: {e}")


# Global parser instance
_parser_instance: Optional[EnhancedSignalParser] = None


def get_parser() -> EnhancedSignalParser:
    """Get or create the global parser instance"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = initialize_parser()
    return _parser_instance


def initialize_parser(config_loader=None) -> EnhancedSignalParser:
    """
    Initialize the parser with configuration

    Args:
        config_loader: Optional configuration loader

    Returns:
        Initialized parser instance
    """
    global _parser_instance

    if config_loader:
        _parser_instance = EnhancedSignalParser(config_loader)
    else:
        try:
            from utils.config_loader import config
            _parser_instance = EnhancedSignalParser(config)
        except:
            _parser_instance = EnhancedSignalParser()

    return _parser_instance


def parse_signal(message: str, channel_name: str = None) -> Optional[ParsedSignal]:
    """
    Parse a trading signal with channel awareness

    This is the main entry point for signal parsing.

    Args:
        message: Raw message text
        channel_name: Discord channel name

    Returns:
        ParsedSignal object or None
    """
    parser = get_parser()
    return parser.parse(message, channel_name)


def cleanup_parser():
    """Cleanup parser resources"""
    global _parser_instance
    if _parser_instance:
        _parser_instance.cleanup()
        _parser_instance = None


# Export main components
__all__ = [
    'ParsedSignal',
    'EnhancedSignalParser',
    'parse_signal',
    'initialize_parser',
    'cleanup_parser',
    'get_parser'
]