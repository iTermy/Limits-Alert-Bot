"""
__init__.py
Main entry point for the signal parser with channel-aware routing
"""

from dataclasses import dataclass, field
from typing import Optional

from utils.logger import get_logger

# Import validation functions
from .validators import detect_channel_type, is_potential_signal, should_exclude, validate_signal

logger = get_logger("parser")


# ============================================================================
# DATA STRUCTURES
# ============================================================================


@dataclass
class ParsedSignal:
    """Represents a parsed trading signal"""

    instrument: str
    direction: str  # long/short
    limits: list[float]
    stop_loss: float
    expiry_type: str  # week_end, no_expiry, day_end, month_end
    raw_text: str
    parse_method: str  # high_confidence/stock/ai
    keywords: list[str] = field(default_factory=list)
    channel_name: str = None
    type: str = "standard"  # standard, scalp, swing, toll, pa, 1-1


class RejectedSignal:
    """
    Sentinel returned when a message looks like a signal but is definitively
    malformed (e.g. limits are out of order, indicating a typo).

    Distinct from None (which means "not a signal at all") so that callers
    can react with ❌ to prompt the user to fix and re-edit the message.
    """

    def __init__(self, reason: str = ""):
        self.reason = reason

    def __bool__(self):
        return False  # still falsy so existing `if result:` guards stay safe


class LimitsOrderError(Exception):
    """Raised by pattern parsers when limit prices are out of the expected order."""


# ============================================================================
# CONSTANTS
# ============================================================================

# Trading keywords for signal detection
TRADING_KEYWORDS = ["stop", "sl", "long", "short", "buy", "sell", "stops"]

# Exclusion keywords
EXCLUSION_KEYWORDS = [
    "dxy",
    "nq",
    "es",
    "ym",
    "rty",
    "vix",
]

# Instrument mappings
INSTRUMENT_MAPPINGS = {
    # Forex abbreviations
    "eu": "EURUSD",
    "gu": "GBPUSD",
    "uj": "USDJPY",
    "uchf": "USDCHF",
    "au": "AUDUSD",
    "ucad": "USDCAD",
    "nu": "NZDUSD",
    "nzd": "NZDUSD",
    "eg": "EURGBP",
    "ej": "EURJPY",
    "gj": "GBPJPY",
    "aj": "AUDJPY",
    "nj": "NZDJPY",
    "ea": "EURAUD",
    # Full pairs
    "eurusd": "EURUSD",
    "gbpusd": "GBPUSD",
    "usdjpy": "USDJPY",
    "usdchf": "USDCHF",
    "audusd": "AUDUSD",
    "usdcad": "USDCAD",
    "nzdusd": "NZDUSD",
    "eurgbp": "EURGBP",
    "eurjpy": "EURJPY",
    "gbpjpy": "GBPJPY",
    "audjpy": "AUDJPY",
    "nzdjpy": "NZDJPY",
    "euraud": "EURAUD",
    "eurnzd": "EURNZD",
    "gbpaud": "GBPAUD",
    "gbpnzd": "GBPNZD",
    "eurchf": "EURCHF",
    "audcad": "AUDCAD",
    "audnzd": "AUDNZD",
    "cadchf": "CADCHF",
    "cadjpy": "CADJPY",
    "chfjpy": "CHFJPY",
    "eurcad": "EURCAD",
    "gbpcad": "GBPCAD",
    "gbpchf": "GBPCHF",
    "nzdcad": "NZDCAD",
    "nzdchf": "NZDCHF",
    "audchf": "AUDCHF",
    # Commodities
    "gold": "XAUUSD",
    "xauusd": "XAUUSD",
    "xau": "XAUUSD",
    "gc": "GCQ26",
    "gcq26": "GCQ26",
    "silver": "XAGUSD",
    "xagusd": "XAGUSD",
    "xag": "XAGUSD",
    "oil": "USOILSPOT",
    "wti": "USOILSPOT",
    "crude": "USOILSPOT",
    "usoil": "USOILSPOT",
    "brent": "UKOIL",
    "ukoil": "UKOIL",
    # Indices
    "spx": "SPX500USD",
    "sp500": "SPX500USD",
    "s&p": "SPX500USD",
    "spx500": "SPX500USD",
    "nas": "NAS100USD",
    "nasdaq": "NAS100USD",
    "nas100": "NAS100USD",
    "ndx": "NAS100USD",
    "dow": "US30USD",
    "us30": "US30USD",
    "djia": "US30USD",
    "jp225": "JP225",
    "nikkei": "JP225",
    "dax": "DE30EUR",
    "dax30": "DE30EUR",
    "de30": "DE30EUR",
    "russell": "US2000USD",
    "us2000": "US2000USD",
    "rut": "US2000USD",
    "aus200": "AUS2000",
    "asx": "AUS2000",
    "f40": "F40",
    "cac": "F40",
    # Crypto (keep main ones, alt coins handled by auto-append)
    "btc": "BTCUSDT",
    "bitcoin": "BTCUSDT",
    "btcusdt": "BTCUSDT",
    "eth": "ETHUSDT",
    "ethereum": "ETHUSDT",
    "ethusdt": "ETHUSDT",
    "sol": "SOLUSDT",
    "solana": "SOLUSDT",
    "bnb": "BNBUSDT",
    "ada": "ADAUSDT",
    "xrp": "XRPUSDT",
    "dot": "DOTUSDT",
    "doge": "DOGEUSDT",
}


# ============================================================================
# MAIN PARSER CLASS
# ============================================================================


class SignalParser:
    """
    Main parser that orchestrates channel-aware parsing

    Flow:
    1. Pre-validation (is_potential_signal, should_exclude)
    2. Detect channel type (Core/Stock/Crypto)
    3. Route to appropriate parser
    4. AI fallback if pattern parsing fails
    """

    def __init__(self, config_loader=None):
        self.channel_config = self._load_channel_config(config_loader)

        # Lazy-load parsers (imported when needed)
        self._core_parser = None
        self._stock_parser = None
        self._ai_parser = None

        logger.info("Initialized SignalParser")

    def _load_channel_config(self, config_loader) -> dict:
        """Load channel configuration from JSON"""
        if not config_loader:
            try:
                from utils.config_loader import config

                config_loader = config
            except Exception as e:
                logger.warning(f"Could not load channel configuration: {e}")
                return {}
        try:
            channels_data = config_loader.load("channels.json")
            channel_config = channels_data.get("channel_settings", {})
            logger.info(f"Loaded channel config for {len(channel_config)} channels")
            return channel_config
        except Exception as e:
            logger.warning(f"Could not load channel configuration: {e}")
            return {}

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse a trading signal with channel-aware routing

        Args:
            message: Raw message text
            channel_name: Discord channel name

        Returns:
            ParsedSignal object or None
        """
        if not message or len(message) < 5:
            return None

        logger.debug(f"=== Parsing message in channel: {channel_name} ===")
        logger.debug(f"Message: {message[:100]}...")

        # Step 1: Pre-validation - is this a potential signal?
        if not is_potential_signal(message, TRADING_KEYWORDS, INSTRUMENT_MAPPINGS, channel_name):
            logger.debug(f"Not a potential signal: {message[:50]}...")
            return None

        logger.debug("✓ Pre-validation passed")

        # Step 2: Check for exclusions
        if should_exclude(message, EXCLUSION_KEYWORDS):
            logger.debug(f"Excluded: {message[:50]}...")
            return None

        logger.debug("✓ Not excluded")

        # Step 3: Detect channel type and route to appropriate parser
        channel_type = detect_channel_type(channel_name)
        logger.debug(f"✓ Channel type detected: '{channel_type}' for '{channel_name}'")

        result = None

        # Try channel-specific parser first
        try:
            if channel_type == "stock":
                logger.debug("→ Routing to StockPatternParser")
                result = self._parse_with_stock_parser(message, channel_name)
            elif channel_type == "crypto":
                logger.debug("→ Routing to crypto parser (CorePatternParser)")
                result = self._parse_with_crypto_parser(message, channel_name)
            else:  # core
                logger.debug("→ Routing to CorePatternParser")
                result = self._parse_with_core_parser(message, channel_name)
        except LimitsOrderError as e:
            logger.info(f"Signal rejected — limits out of order (likely typo): {e}")
            return RejectedSignal(reason=str(e))

        # Step 4: AI fallback if pattern parsing failed
        if not result:
            logger.debug("Pattern parsing failed, trying AI fallback")
            result = self._parse_with_ai(message, channel_name)

        if result:
            logger.info(
                f"Parse success ({result.parse_method}): {result.instrument} {result.direction}"
            )
        else:
            logger.debug(f"Failed to parse: {message[:100]}...")

        return result

    def _parse_with_core_parser(self, message: str, channel_name: str) -> Optional[ParsedSignal]:
        """Parse using core pattern parser (forex, gold, indices)"""
        if self._core_parser is None:
            from .pattern_parsers import CorePatternParser

            self._core_parser = CorePatternParser(self.channel_config)

        return self._core_parser.parse(message, channel_name)

    def _parse_with_stock_parser(self, message: str, channel_name: str) -> Optional[ParsedSignal]:
        """Parse using stock-specific parser"""
        if self._stock_parser is None:
            from .pattern_parsers import StockPatternParser

            self._stock_parser = StockPatternParser(self.channel_config)

        return self._stock_parser.parse(message, channel_name)

    def _parse_with_crypto_parser(self, message: str, channel_name: str) -> Optional[ParsedSignal]:
        """Parse crypto signals using the core parser with crypto parse_method label"""
        if self._core_parser is None:
            from .pattern_parsers import CorePatternParser

            self._core_parser = CorePatternParser(self.channel_config)

        result = self._core_parser.parse(message, channel_name)
        if result:
            result.parse_method = "crypto"
        return result

    def _parse_with_ai(self, message: str, channel_name: str) -> Optional[ParsedSignal]:
        """Parse using AI fallback"""
        if self._ai_parser is None:
            from .ai_fallback import AIFallbackParser

            self._ai_parser = AIFallbackParser(self.channel_config)

        return self._ai_parser.parse(message, channel_name)

    def cleanup(self):
        """Cleanup resources (e.g., MT5 connections)"""
        if self._stock_parser and hasattr(self._stock_parser, "cleanup"):
            try:
                self._stock_parser.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up stock parser: {e}")


# ============================================================================
# GLOBAL PARSER INSTANCE
# ============================================================================

_parser_instance: Optional[SignalParser] = None


def get_parser() -> SignalParser:
    """Get or create the global parser instance"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = SignalParser()
    return _parser_instance


def parse_signal(message: str, channel_name: str = None) -> Optional[ParsedSignal]:
    """
    Main entry point for signal parsing.

    Args:
        message: Raw message text
        channel_name: Discord channel name

    Returns:
        ParsedSignal object or None
    """
    return get_parser().parse(message, channel_name)


# Export main components
__all__ = [
    "LimitsOrderError",
    "ParsedSignal",
    "RejectedSignal",
    "get_parser",
    "parse_signal",
]
