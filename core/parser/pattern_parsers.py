"""
pattern_parsers.py
Channel-specific pattern-based parsers for trading signals
"""

import re
import time
from typing import Optional

from utils.logger import get_logger

from . import INSTRUMENT_MAPPINGS, ParsedSignal
from .validators import (
    INDEX_SYMBOL_BLACKLIST,
    uses_gold_tolls_sl,
    validate_signal,
)

logger = get_logger("parser.pattern_parsers")

# ---------------------------------------------------------------------------
# Gold Tolls SL offset — configurable via !goldtollssl, stored in settings.json
# ---------------------------------------------------------------------------

_GOLD_TOLLS_SL_OFFSET_DEFAULT = 5.0
_gold_tolls_sl_cache: float = _GOLD_TOLLS_SL_OFFSET_DEFAULT
_gold_tolls_sl_cache_ts: float = 0.0
_GOLD_TOLLS_SL_CACHE_TTL: float = 30.0  # seconds


def get_gold_tolls_sl_offset() -> float:
    """
    Return the current gold-tolls SL offset (dollars from the last limit).

    Value is read from settings.json → ``gold_tolls_sl_offset`` and cached
    for 30 s so we're not hitting disk on every parse.  The command handler
    calls ``invalidate_gold_tolls_sl_cache()`` after writing a new value so
    the change takes effect immediately.
    """
    global _gold_tolls_sl_cache, _gold_tolls_sl_cache_ts
    if time.monotonic() - _gold_tolls_sl_cache_ts > _GOLD_TOLLS_SL_CACHE_TTL:
        try:
            from utils.config_loader import load_settings

            settings = load_settings()
            _gold_tolls_sl_cache = float(settings.gold_tolls_sl_offset)
        except Exception as e:
            logger.warning(
                f"Could not load gold_tolls_sl_offset from settings: {e}. Using {_gold_tolls_sl_cache}."
            )
        _gold_tolls_sl_cache_ts = time.monotonic()
    return _gold_tolls_sl_cache


def invalidate_gold_tolls_sl_cache() -> None:
    """Force the next call to get_gold_tolls_sl_offset() to re-read from disk."""
    global _gold_tolls_sl_cache_ts
    _gold_tolls_sl_cache_ts = 0.0


# ---------------------------------------------------------------------------
# Risky-Gold SL offset — configurable independently of gold tolls via
# !riskygoldsl, stored in settings.json → risky_gold_sl_offset
# ---------------------------------------------------------------------------

_RISKY_GOLD_SL_OFFSET_DEFAULT = 10.0
_risky_gold_sl_cache: float = _RISKY_GOLD_SL_OFFSET_DEFAULT
_risky_gold_sl_cache_ts: float = 0.0


def get_risky_gold_sl_offset() -> float:
    """
    Return the current risky-gold SL offset (dollars from the nearest limit).

    Read from settings.json → ``risky_gold_sl_offset`` and cached for 30 s.
    Independent of the gold-tolls offset so the two can be tuned separately.
    """
    global _risky_gold_sl_cache, _risky_gold_sl_cache_ts
    if time.monotonic() - _risky_gold_sl_cache_ts > _GOLD_TOLLS_SL_CACHE_TTL:
        try:
            from utils.config_loader import load_settings

            settings = load_settings()
            _risky_gold_sl_cache = float(settings.risky_gold_sl_offset)
        except Exception as e:
            logger.warning(
                f"Could not load risky_gold_sl_offset from settings: {e}. Using {_risky_gold_sl_cache}."
            )
        _risky_gold_sl_cache_ts = time.monotonic()
    return _risky_gold_sl_cache


def invalidate_risky_gold_sl_cache() -> None:
    """Force the next call to get_risky_gold_sl_offset() to re-read from disk."""
    global _risky_gold_sl_cache_ts
    _risky_gold_sl_cache_ts = 0.0


# Optional import for stock parsing
try:
    import MetaTrader5 as mt5

    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    logger.warning("MetaTrader5 not available - stock parsing will be disabled")

# ============================================================================
# CONSTANTS
# ============================================================================

FOREX_PAIRS = {
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "AUDUSD",
    "USDCAD",
    "NZDUSD",
    "EURGBP",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",
    "NZDJPY",
    "EURAUD",
    "EURNZD",
    "GBPAUD",
    "GBPNZD",
    "EURCHF",
    "AUDCAD",
    "AUDNZD",
    "CADCHF",
    "CADJPY",
    "CHFJPY",
    "EURCAD",
    "GBPCAD",
    "GBPCHF",
    "NZDCAD",
    "NZDCHF",
    "AUDCHF",
    "EURSGD",
    "EURTRY",
    "GBPSGD",
    "USDMXN",
    "USDNOK",
    "USDSEK",
    "USDSGD",
    "USDTRY",
    "USDZAR",
    "ZARJPY",
}

# High-value instruments
HIGH_VALUE_INSTRUMENTS = {
    "BTCUSDT",
    "BTCUSD",
    "ETHUSDT",
    "JP225",
    "US30USD",
    "SPX500USD",
    "NAS100USD",
    "US2000USD",
    "DE30EUR",
    "AUS2000",
    "F40",
}

LONG_KEYWORDS = ["long", "buy"]
SHORT_KEYWORDS = ["short", "sell"]

# Channel-name → signal type mapping. Channels not listed default to "standard"
# unless the message body itself contains a "swing" or "scalp" keyword.
CHANNEL_TYPE_MAP = {
    "scalps": "scalp",
    "swing-trades": "swing",
    "gold-swings": "swing",
    "gold-tolls-map": "toll",
    "general-tolls": "toll",
    "oil-tolls": "toll",
    "gold-pa-signals": "pa",
    "price-action-trades": "pa",
    "gold-1-1-rr": "1-1",
    "risky-gold": "risky",
    "semi-swing-pa-signals": "pa",
}

# Expiry patterns
EXPIRY_PATTERNS = {
    "vth": "week_end",
    "vtai": "no_expiry",
    "alien": "no_expiry",
    "vtd": "day_end",
    "vtw": "week_end",
    "vtwe": "week_end",
    "vtm": "month_end",
    "vtme": "month_end",
    "valid till hit": "no_expiry",
    "valid till week": "week_end",
    "valid till day": "day_end",
    "valid till month": "month_end",
    "swing": "week_end",
    "no expiry": "no_expiry",
}

SPECIAL_KEYWORDS = ["hot", "semi-swing", "swing", "scalp", "swing-trade", "intraday", "position"]

_SL_KEYWORD_RE = re.compile(r"\b(sl|stop|stops)\b", re.IGNORECASE)
_FUTURES_RE = re.compile(r"\b(?:future|futures)\b", re.IGNORECASE)
_GOLD_KW_RE = re.compile(r"\b(?:gold|xau|xauusd)\b", re.IGNORECASE)


def _general_tolls_auto_sl(channel_name: str, text: str) -> bool:
    """Returns True when this is a general-tolls channel with no explicit SL keyword (auto-SL mode)."""
    return bool(
        channel_name and channel_name.lower() == "general-tolls" and not _SL_KEYWORD_RE.search(text)
    )


STOCK_SKIP_WORDS = {
    "LONG",
    "SHORT",
    "BUY",
    "SELL",
    "VTH",
    "VTAI",
    "VTWE",
    "VTD",
    "VTME",
    "HOT",
    "STOPS",
    "SL",
    "STOP",
    "ALIEN",
    "SCALP",
    "SWING",
    "INTRADAY",
    "POSITION",
    "SEMI-SWING",
    "DAY-TRADE",
    "SWING-TRADE",
}


# Ticker aliases for names that would otherwise match multiple symbols by
# description (e.g. "GOOGLE" matches both GOOG.NAS and GOOGL.NAS). Maps an
# upper-cased word to the canonical ticker to prefer.
STOCK_TICKER_ALIASES = {
    "GOOGLE": "GOOG",
    "GOOGL": "GOOG",
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def clean_message(message: str) -> str:
    """Clean and normalize message text"""
    cleaned = message.lower()
    cleaned = re.sub(r"[-—–]+", " ", cleaned)
    cleaned = re.sub(r"[,/|]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def extract_numbers(text: str) -> list[float]:
    """Extract all numbers from text, excluding numbers inside blacklisted terms"""
    for word in INDEX_SYMBOL_BLACKLIST:
        text = re.sub(re.escape(word), "", text, flags=re.IGNORECASE)
    numbers = re.findall(r"\d+\.?\d*", text)
    try:
        return [float(n) for n in numbers]
    except ValueError:
        return []


def scale_forex_numbers(numbers: list[float], instrument: str) -> list[float]:
    """Scale down large forex numbers if needed"""
    if instrument not in FOREX_PAIRS or instrument in HIGH_VALUE_INSTRUMENTS:
        return numbers

    # Only scale if numbers are large (> 10000)
    if any(n > 10000 for n in numbers):
        scaled = [n / 100000 for n in numbers]
        logger.debug(f"Scaled down large forex numbers for {instrument}")
        return scaled

    return numbers


def extract_words_with_boundaries(text: str) -> list[str]:
    """Extract words from text including alphanumeric patterns"""
    return re.findall(r"\b[a-z0-9]+\b", text.lower())


def validate_limits_and_stop(limits: list[float], stop_loss: float, direction: str) -> bool:
    """Validate that limits and stop loss make sense for the direction"""
    if not limits:
        return False

    # For long: limits should be above stop
    if direction == "long":
        return all(limit > stop_loss for limit in limits)
    # For short: limits should be below stop
    return all(limit < stop_loss for limit in limits)


# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================


def extract_instrument(text: str, channel_name: str, channel_config: dict) -> Optional[str]:
    """Extract trading instrument with channel awareness"""
    text_lower = text.lower()

    # Gold futures: gold keyword + "future"/"futures", or gold channel + "future"/"futures"
    if _FUTURES_RE.search(text_lower):
        is_gold_channel = channel_name and "gold" in channel_name.lower()
        if is_gold_channel or _GOLD_KW_RE.search(text_lower):
            return "GCZ26_CFD"

    # Check if this is a crypto-alt channel (has both "crypto" and "alt")
    is_crypto_alt = False
    if channel_name:
        channel_lower = channel_name.lower()
        is_crypto_alt = "crypto" in channel_lower and "alt" in channel_lower

    # Check channel configuration for default instrument
    if channel_name and channel_name in channel_config:
        channel_settings = channel_config[channel_name]
        default_instrument = channel_settings.get("default_instrument")

        if default_instrument:
            # Check if another instrument is explicitly mentioned
            other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
            if not other_instrument:
                logger.debug(f"Using default instrument {default_instrument}")
                return default_instrument
            logger.debug(f"Found explicit instrument {other_instrument}")
            return other_instrument

    # Fallback to channel name detection
    if channel_name:
        channel_based = _extract_from_channel_name(text_lower, channel_name, is_crypto_alt)
        if channel_based:
            return channel_based

    # Look for explicit instrument
    return _find_explicit_instrument(text_lower, is_crypto_alt)


def _extract_from_channel_name(
    text_lower: str, channel_name: str, is_crypto_alt: bool = False
) -> Optional[str]:
    """Extract instrument based on channel name"""
    channel_lower = channel_name.lower()

    # Crypto-alt channel - try to extract any word and append USDT
    if is_crypto_alt:
        alt_symbol = _extract_crypto_alt_symbol(text_lower)
        if alt_symbol:
            logger.debug(f"Crypto-alt channel: {alt_symbol} maps to {alt_symbol}USDT")
            return f"{alt_symbol}USDT"

    # Gold channel - default to XAUUSD if no other instrument found
    if "gold" in channel_lower:
        other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
        if not other_instrument:
            logger.debug("Gold channel detected, defaulting to XAUUSD")
            return "XAUUSD"
        return other_instrument

    # Oil channel - default to USOILSPOT unless IC mentioned
    if "oil" in channel_lower:
        if "ic" in text_lower or "xti" in text_lower:
            logger.debug("IC oil detected, using XTIUSD")
            return "XTIUSD"
        other_instrument = _find_explicit_instrument(text_lower, is_crypto_alt)
        if not other_instrument:
            logger.debug("Oil channel detected, defaulting to USOILSPOT")
            return "USOILSPOT"
        return other_instrument

    return None


def _find_explicit_instrument(text_lower: str, is_crypto_alt: bool = False) -> Optional[str]:
    """Find explicitly mentioned instrument in text"""
    # For crypto-alt channels, try to find any potential symbol
    if is_crypto_alt:
        alt_symbol = _extract_crypto_alt_symbol(text_lower)
        if alt_symbol:
            logger.debug(f"Crypto-alt auto-append: {alt_symbol} maps to {alt_symbol}USDT")
            return f"{alt_symbol}USDT"

    # Check for crypto first (standard mappings for BTC, ETH, etc.)
    crypto_found = _find_crypto_symbol(text_lower)
    if crypto_found:
        return crypto_found

    # Check exact word matches for abbreviations
    words = extract_words_with_boundaries(text_lower)

    for word in words:
        if word in INSTRUMENT_MAPPINGS:
            # Make sure it's not part of a longer symbol
            pattern = r"\b" + re.escape(word) + r"\b"
            if re.search(pattern, text_lower):
                logger.debug(f"Found instrument: {word} -> {INSTRUMENT_MAPPINGS[word]}")
                return INSTRUMENT_MAPPINGS[word]

    # Check for full instrument names (6+ characters like 'eurusd')
    for pattern, instrument in INSTRUMENT_MAPPINGS.items():
        if len(pattern) >= 6 and pattern in text_lower:  # Full names
            logger.debug(f"Found full instrument: {pattern} -> {instrument}")
            return instrument

    return None


def _find_crypto_symbol(text_lower: str) -> Optional[str]:
    """Find crypto symbols in text"""
    crypto_keys = ["btc", "eth", "sol", "bnb", "ada", "xrp", "dot", "doge"]
    for crypto_key in crypto_keys:
        if re.search(r"\b" + crypto_key + r"\b", text_lower):
            return INSTRUMENT_MAPPINGS.get(crypto_key, crypto_key.upper() + "USDT")
    return None


def _extract_crypto_alt_symbol(text_lower: str) -> Optional[str]:
    """
    Extract a potential crypto alt symbol from text for auto-USDT appending

    Used in crypto-alt channels to detect any ticker-like word and append USDT
    Example: "dash short" → extracts "dash" → becomes "DASHUSDT"
    """
    # Skip words that are clearly not crypto symbols
    skip_words = {
        "long",
        "short",
        "buy",
        "sell",
        "stop",
        "stops",
        "sl",
        "vth",
        "vtai",
        "vtwe",
        "vtd",
        "vtme",
        "alien",
        "hot",
        "scalp",
        "swing",
        "intraday",
        "position",
        "semi",
        "day",
        "week",
        "month",
        "trade",
        "limit",
        "entry",
        "take",
        "profit",
        "loss",
        "price",
        "usdt",
        "usd",
    }

    # Get all words from the text
    words = extract_words_with_boundaries(text_lower)

    # Look for a word that could be a crypto symbol
    for word in words:
        # Skip numbers
        if word.replace(".", "").isdigit():
            continue

        # Skip common trading terms
        if word in skip_words:
            continue

        # Skip very short words (< 2 chars) or very long (> 10 chars)
        if len(word) < 2 or len(word) > 10:
            continue

        # If word is all letters and 2-10 chars, it's probably a ticker
        if word.isalpha():
            logger.debug(f"Crypto-alt symbol detected: {word}")
            return word.upper()

    return None


def extract_direction(text: str) -> Optional[str]:
    """Extract trade direction from text"""
    text_lower = text.lower()

    for keyword in LONG_KEYWORDS:
        if re.search(r"\b" + keyword + r"\b", text_lower):
            return "long"

    for keyword in SHORT_KEYWORDS:
        if re.search(r"\b" + keyword + r"\b", text_lower):
            return "short"

    return None


def extract_expiry(text: str, channel_name: str, channel_config: dict) -> str:
    """Extract expiry type with channel defaults"""
    text_lower = text.lower()

    # First check for explicit expiry patterns in the text
    for pattern, expiry_type in EXPIRY_PATTERNS.items():
        if pattern in text_lower:
            return expiry_type

    # If no explicit expiry, use channel default from config
    if channel_name and channel_name in channel_config:
        channel_settings = channel_config[channel_name]
        default_expiry = channel_settings.get("default_expiry", "day_end")
        logger.debug(f"Using default expiry {default_expiry} for {channel_name}")
        return default_expiry

    # Default expiry
    return "day_end"


def extract_keywords(text: str) -> list[str]:
    """Extract special keywords from text"""
    text_lower = text.lower()
    keywords = []

    # Check for compound keywords first
    compound_keywords = ["semi-swing", "day-trade", "swing-trade", "position-trade"]
    for keyword in compound_keywords:
        if keyword in text_lower or keyword.replace("-", " ") in text_lower:
            keywords.append(keyword)

    # Then check single keywords
    for keyword in SPECIAL_KEYWORDS:
        if keyword in text_lower and keyword not in keywords:
            # Don't add 'swing' if 'semi-swing' is already added
            if keyword == "swing" and "semi-swing" in keywords:
                continue
            keywords.append(keyword)

    return keywords


def validate_limits_order(limits: list[float], direction: str) -> bool:
    """
    Validate that limits are in the expected order for the given direction.

    For SHORT: limits must be in ascending order (lowest → highest).
               Traders list entry levels from lower to higher as price rises.
    For LONG:  limits must be in descending order (highest → lowest).
               Traders list entry levels from higher to lower as price falls.

    A single limit always passes (nothing to order-check).

    Returns:
        True if the order is correct (or only one limit provided)
    """
    if len(limits) <= 1:
        return True

    if direction == "short":
        # Ascending: each limit must be >= the previous
        return all(limits[i] <= limits[i + 1] for i in range(len(limits) - 1))
    # long
    # Descending: each limit must be <= the previous
    return all(limits[i] >= limits[i + 1] for i in range(len(limits) - 1))


def _reject_out_of_order(limits: list[float], direction: str, label: str) -> None:
    """Raise LimitsOrderError when multi-limit prices break the direction's
    expected ordering (short ascending, long descending) — almost always a typo."""
    if len(limits) > 1 and not validate_limits_order(limits, direction):
        from . import LimitsOrderError

        prefix = f"{direction} {label}".strip()
        raise LimitsOrderError(
            f"{prefix} limits not "
            f"{'ascending' if direction == 'short' else 'descending'}: {limits}"
        )


def _split_explicit_stop(numbers: list[float], direction: str) -> tuple:
    """Split numbers into (limits, stop_loss) trying the last number as the stop
    first, then the first (alternative convention). Returns (None, None) when
    neither placement validates against the direction."""
    stop_loss = numbers[-1]
    limits = numbers[:-1]
    if validate_limits_and_stop(limits, stop_loss, direction):
        return limits, stop_loss

    stop_loss = numbers[0]
    limits = numbers[1:]
    if validate_limits_and_stop(limits, stop_loss, direction):
        return limits, stop_loss

    return None, None


def _auto_stop(limits: list[float], direction: str, offset: float) -> float:
    """Derive the stop loss from the outermost limit: offset below the lowest
    limit for longs, offset above the highest for shorts."""
    if direction == "long":
        return min(limits) - offset
    return max(limits) + offset


def _general_tolls_limits_and_stop(
    numbers: list[float], direction: str, raw_text: str, instrument: str
) -> tuple:
    """general-tolls: explicit SL keyword uses the standard last/first-number
    convention; otherwise every number is a limit and the SL is derived from a
    per-instrument dollar offset (single-number messages are valid)."""
    auto_sl_offsets = {"SPX500USD": 10.0, "NAS100USD": 30.0}
    auto_sl_default = 10.0

    if raw_text and _SL_KEYWORD_RE.search(raw_text):
        if len(numbers) < 2:
            return None, None
        limits, stop_loss = _split_explicit_stop(numbers, direction)
        if limits is None:
            logger.debug(
                f"General tolls stop loss validation failed for {direction} with numbers {numbers}"
            )
            return None, None
        _reject_out_of_order(limits, direction, "general-tolls")
        logger.debug(
            f"General tolls (explicit SL): {len(limits)} limit(s), stop={stop_loss} ({direction})"
        )
        return limits, stop_loss

    if not numbers:
        return None, None

    limits = numbers
    instr_upper = (instrument or "").upper()
    sl_offset = auto_sl_offsets.get(instr_upper, auto_sl_default)
    stop_loss = _auto_stop(limits, direction, sl_offset)
    _reject_out_of_order(limits, direction, "general-tolls")
    logger.debug(
        f"General tolls (auto SL, offset={sl_offset}): {len(limits)} limit(s), "
        f"stop={stop_loss} ({direction}, instrument={instr_upper})"
    )
    return limits, stop_loss


def _tolls_limits_and_stop(numbers: list[float], direction: str, raw_text: str, channel_name: str) -> tuple:
    """Gold-tolls-style channels: an explicit SL keyword (with 2+ numbers) makes
    the last number the stop; otherwise every number is a limit and the SL is the
    configured offset beyond the outermost limit (risky-gold has its own offset)."""
    if not numbers:
        return None, None

    if raw_text and _SL_KEYWORD_RE.search(raw_text) and len(numbers) >= 2:
        limits, stop_loss = _split_explicit_stop(numbers, direction)
        if limits is None:
            logger.debug(
                f"Tolls explicit stop loss validation failed for {direction} with numbers {numbers}"
            )
            return None, None
        _reject_out_of_order(limits, direction, "tolls")
        logger.debug(
            f"Tolls channel (explicit SL): {len(limits)} limit(s), stop={stop_loss} ({direction})"
        )
        return limits, stop_loss

    limits = numbers
    if channel_name and channel_name.lower() == "risky-gold":
        sl_offset = get_risky_gold_sl_offset()
    else:
        sl_offset = get_gold_tolls_sl_offset()
    stop_loss = _auto_stop(limits, direction, sl_offset)
    _reject_out_of_order(limits, direction, "tolls")
    logger.debug(
        f"Tolls channel: Using all {len(limits)} number(s) as limits, "
        f"auto-setting stop to {stop_loss} (offset={sl_offset}, {direction})"
    )
    return limits, stop_loss


def _standard_limits_and_stop(numbers: list[float], direction: str) -> tuple:
    """Regular channels: at least two numbers; the last (or first) is the stop,
    the rest are limits, and multi-limit ordering must match the direction."""
    if len(numbers) < 2:
        return None, None

    limits, stop_loss = _split_explicit_stop(numbers, direction)
    if limits is None:
        logger.debug(f"Stop loss validation failed for {direction} with numbers {numbers}")
        return None, None

    _reject_out_of_order(limits, direction, "")
    return limits, stop_loss


def determine_limits_and_stop(
    numbers: list[float],
    direction: str,
    channel_name: Optional[str] = None,
    raw_text: Optional[str] = None,
    instrument: Optional[str] = None,
) -> tuple:
    """
    Determine which numbers are limits and which is the stop loss.

    Numbers are taken in the order provided — no reordering is performed. If the
    limits are not in the expected order for the direction (short ascending,
    long descending), LimitsOrderError is raised to surface typos such as a
    misplaced decimal point.

    Channel routing:
      - general-tolls: per-instrument auto-SL unless an SL keyword is present
      - gold-tolls-style channels (incl. risky-gold): offset-derived auto-SL
        unless an SL keyword provides one explicitly
      - everything else: last (or first) number is the stop loss
    """
    if channel_name and channel_name.lower() == "general-tolls":
        return _general_tolls_limits_and_stop(numbers, direction, raw_text, instrument)

    if uses_gold_tolls_sl(channel_name):
        return _tolls_limits_and_stop(numbers, direction, raw_text, channel_name)

    return _standard_limits_and_stop(numbers, direction)


def get_signal_type(text: str, channel_name: Optional[str] = None) -> str:
    """
    Determine the signal type from the channel and message body.

    Channel takes priority — a channel listed in CHANNEL_TYPE_MAP always
    yields that type regardless of message content. Otherwise we look for
    'swing' or 'scalp' keywords in the text, then fall back to 'standard'.
    """
    if channel_name and channel_name.lower() in CHANNEL_TYPE_MAP:
        return CHANNEL_TYPE_MAP[channel_name.lower()]
    if re.search(r"\bswing\b", text, re.IGNORECASE):
        return "swing"
    if re.search(r"\bscalp\b", text, re.IGNORECASE):
        return "scalp"
    return "standard"


# ============================================================================
# INSTANT ENTRY PARSER
# ============================================================================

# Labelled price capture for instant-entry signals ("short gold sl 5001 tp 4080").
# Both labels are mandatory and may appear in either order.
_INSTANT_SL_RE = re.compile(r"\b(?:sl|stops?|stop\s*loss)\b\s*[:=@]?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_INSTANT_TP_RE = re.compile(
    r"\b(?:tp|target|take\s*profit)\b\s*[:=@]?\s*(\d+(?:\.\d+)?)", re.IGNORECASE
)


def _instant_price(pattern: re.Pattern, text: str) -> Optional[float]:
    """First labelled price matched by `pattern`, or None when the label is absent."""
    match = pattern.search(text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_instant_signal(
    message: str, channel_name: Optional[str], channel_config: Optional[dict] = None
) -> Optional[ParsedSignal]:
    """
    Parse a market-entry signal: an instrument, a direction, and labelled stop
    loss and take profit prices ("short gold sl 5001 tp 4080").

    No limits are produced — the entry is the live market price, resolved by the
    save path. Raises LimitsOrderError when the take profit sits on the losing
    side of the stop loss, which is always a typo.
    """
    from . import LimitsOrderError

    cleaned = clean_message(message)

    instrument = extract_instrument(cleaned, channel_name, channel_config or {})
    if not instrument:
        logger.debug(f"Instant parse: no instrument found for channel {channel_name}")
        return None

    direction = extract_direction(cleaned)
    if not direction:
        logger.debug("Instant parse: no direction found")
        return None

    stop_loss = _instant_price(_INSTANT_SL_RE, cleaned)
    take_profit = _instant_price(_INSTANT_TP_RE, cleaned)
    if stop_loss is None or take_profit is None:
        logger.debug(
            f"Instant parse: needs both SL and TP (got sl={stop_loss}, tp={take_profit})"
        )
        return None

    stop_loss, take_profit = scale_forex_numbers([stop_loss, take_profit], instrument)

    tp_is_profitable = take_profit > stop_loss if direction == "long" else take_profit < stop_loss
    if not tp_is_profitable:
        raise LimitsOrderError(
            f"{direction} take profit {take_profit} is on the wrong side of stop loss {stop_loss}"
        )

    signal = ParsedSignal(
        instrument=instrument,
        direction=direction,
        limits=[],
        stop_loss=stop_loss,
        take_profit=take_profit,
        instant_entry=True,
        expiry_type=extract_expiry(cleaned, channel_name, channel_config or {}),
        raw_text=message,
        parse_method="instant",
        keywords=extract_keywords(cleaned),
        channel_name=channel_name,
        type=get_signal_type(message, channel_name),
    )

    if not validate_signal(signal):
        logger.debug("Instant signal validation failed")
        return None

    logger.info(
        f"Instant parse success: {instrument} {direction} sl={stop_loss} tp={take_profit}"
    )
    return signal


# ============================================================================
# CORE PATTERN PARSER
# ============================================================================


class CorePatternParser:
    """
    Pattern-based parser for forex, gold, indices, and other core instruments

    This is the main parser used for most channels.
    """

    def __init__(self, channel_config: Optional[dict] = None):
        self.channel_config = channel_config or {}
        logger.info("Initialized CorePatternParser")

    def parse(self, message: str, channel_name: Optional[str] = None) -> Optional[ParsedSignal]:
        """Parse using pattern matching for core instruments"""
        try:
            cleaned = clean_message(message)
            numbers = extract_numbers(cleaned)

            min_numbers = (
                1
                if (uses_gold_tolls_sl(channel_name) or _general_tolls_auto_sl(channel_name, cleaned))
                else 2
            )

            if len(numbers) < min_numbers:
                logger.debug(f"Not enough numbers found (need {min_numbers}, got {len(numbers)})")
                return None

            instrument = extract_instrument(cleaned, channel_name, self.channel_config)
            if not instrument:
                logger.debug(f"No instrument found for channel {channel_name}")
                return None

            numbers = scale_forex_numbers(numbers, instrument)
            if not numbers:
                logger.warning("No numbers after scaling")
                return None

            direction = extract_direction(cleaned)
            if not direction:
                logger.debug("No direction found")
                return None

            limits, stop_loss = determine_limits_and_stop(
                numbers,
                direction,
                channel_name,
                raw_text=cleaned,
                instrument=instrument,
            )
            if not limits or stop_loss is None:
                logger.debug("Could not determine limits and stop loss")
                return None

            expiry_type = extract_expiry(cleaned, channel_name, self.channel_config)
            keywords = extract_keywords(cleaned)
            signal_type = get_signal_type(message, channel_name)

            signal = ParsedSignal(
                instrument=instrument,
                direction=direction,
                limits=limits,
                stop_loss=stop_loss,
                expiry_type=expiry_type,
                raw_text=message,
                parse_method="core",
                keywords=keywords,
                channel_name=channel_name,
                type=signal_type,
            )

            if validate_signal(signal):
                logger.info(f"Core parse success: {signal.instrument} {signal.direction}")
                return signal

            logger.debug("Signal validation failed")
            return None

        except Exception as e:
            from . import LimitsOrderError

            if isinstance(e, LimitsOrderError):
                raise
            logger.error(f"Core parsing error: {e}")
            return None


# ============================================================================
# STOCK PATTERN PARSER
# ============================================================================


class StockPatternParser:
    """
    Stock-specific parser with MT5 integration for symbol lookup
    """

    # Minimum seconds between symbol-cache refreshes when stock symbols are missing.
    # Prevents every miss from paying the ~24 ms symbols_get() cost if the broker
    # genuinely has no stocks.
    _SYMBOL_REFRESH_MIN_INTERVAL = 60.0

    def __init__(self, channel_config: Optional[dict] = None):
        self.channel_config = channel_config or {}
        self.mt5_initialized = False
        self.available_symbols: set[str] = set()
        self._last_symbol_refresh: float = 0.0
        self._initialize_mt5()
        logger.info("Initialized StockPatternParser")

    def _initialize_mt5(self):
        """Initialize MT5 connection for symbol checking"""
        if not MT5_AVAILABLE:
            logger.warning("MT5 module not available, stock parsing disabled")
            return

        try:
            if not mt5.initialize():
                logger.warning("MT5 initialization failed, stock parsing disabled")
                return

            # Get all available symbols
            symbols = mt5.symbols_get()
            if symbols:
                self.available_symbols = {s.name for s in symbols}
                self.mt5_initialized = True
                self._last_symbol_refresh = time.monotonic()
                logger.info(f"MT5 initialized with {len(self.available_symbols)} symbols")
            else:
                logger.warning("No symbols retrieved from MT5")

        except Exception as e:
            logger.error(f"MT5 initialization error: {e}")
            self.mt5_initialized = False

    def _refresh_symbols(self) -> None:
        """Re-fetch MT5's symbol set. Rate-limited so misses on a stock-less broker don't stall the event loop."""
        now = time.monotonic()
        if now - self._last_symbol_refresh < self._SYMBOL_REFRESH_MIN_INTERVAL:
            return
        self._last_symbol_refresh = now
        try:
            symbols = mt5.symbols_get()
            if symbols:
                self.available_symbols = {s.name for s in symbols}
                logger.debug(f"Refreshed MT5 symbol set: {len(self.available_symbols)} symbols")
        except Exception as e:
            logger.debug(f"Symbol refresh failed: {e}")

    def parse(self, message: str, channel_name: Optional[str] = None) -> Optional[ParsedSignal]:
        """
        Parse a stock trading signal

        Args:
            message: The message to parse (preserves case for stock symbols)
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
        if not self.mt5_initialized:
            logger.warning("MT5 not initialized, cannot parse stocks")
            return None

        try:
            # Clean message for everything except stock extraction
            cleaned = clean_message(message)

            numbers = extract_numbers(cleaned)
            if len(numbers) < (1 if _general_tolls_auto_sl(channel_name, cleaned) else 2):
                return None

            # Extract stock symbol from ORIGINAL message (preserves case)
            instrument = self._extract_stock_symbol(message)
            if not instrument:
                logger.debug("No stock symbol found")
                return None

            # Don't scale stock prices (they're already correct)

            # Extract direction from cleaned message
            direction = extract_direction(cleaned)
            if not direction:
                return None

            # Determine limits and stop loss (pass channel_name for tolls handling)
            limits, stop_loss = determine_limits_and_stop(
                numbers,
                direction,
                channel_name,
                raw_text=cleaned,
                instrument=instrument,
            )
            if not limits or stop_loss is None:
                logger.debug("Could not determine limits and stop loss")
                return None

            # Extract expiry
            expiry_type = extract_expiry(cleaned, channel_name, self.channel_config)

            # Extract keywords
            keywords = extract_keywords(cleaned)

            signal_type = get_signal_type(message, channel_name)

            signal = ParsedSignal(
                instrument=instrument,
                direction=direction,
                limits=limits,
                stop_loss=stop_loss,
                expiry_type=expiry_type,
                raw_text=message,
                parse_method="stock",
                keywords=keywords,
                channel_name=channel_name,
                type=signal_type,
            )

            # Validate before returning
            if validate_signal(signal):
                logger.info(f"Stock parse success: {signal.instrument} {signal.direction}")
                return signal

            return None

        except Exception as e:
            from . import LimitsOrderError

            if isinstance(e, LimitsOrderError):
                raise
            logger.error(f"Stock parsing error: {e}")
            return None

    def _extract_stock_symbol(self, text: str) -> Optional[str]:
        """Extract stock symbol using MT5 integration"""
        if not self.mt5_initialized:
            return None

        # Get words from text
        words_original = text.split()
        words_upper = [w.upper() for w in words_original]

        # Get only stock symbols from available symbols
        stock_symbols = [
            s for s in self.available_symbols if s.endswith((".NYSE", ".NAS", ".NASDAQ"))
        ]

        # Cache was empty of stock symbols — MT5 may have loaded them after init.
        # Refresh once and retry (rate-limited inside _refresh_symbols).
        if not stock_symbols:
            self._refresh_symbols()
            stock_symbols = [
                s for s in self.available_symbols if s.endswith((".NYSE", ".NAS", ".NASDAQ"))
            ]

        if not stock_symbols:
            logger.warning("No stock symbols found in MT5")
            return None

        tickers_to_symbol = {symbol.split(".")[0]: symbol for symbol in stock_symbols}

        # Step 0: Alias match — resolve ambiguous names to a canonical ticker
        for word in words_upper:
            alias = STOCK_TICKER_ALIASES.get(word)
            if alias and alias in tickers_to_symbol:
                symbol = tickers_to_symbol[alias]
                logger.info(f"Found ticker alias match: {word} -> {symbol}")
                return symbol

        # Step 1: Direct ticker match
        for word in words_upper:
            if word in STOCK_SKIP_WORDS:
                continue

            if word in tickers_to_symbol:
                symbol = tickers_to_symbol[word]
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
            return match["symbol"]
        if len(matches) > 1:
            # Try to find best match
            best = self._select_best_match(matches)
            if best:
                logger.info(f"Selected best match: {best['symbol']}")
                return best["symbol"]

        return None

    def _find_by_description(self, text: str, stock_symbols: list[str]) -> list[dict]:
        """Find stocks by description matching"""
        # Get meaningful words for search
        words_lower = [
            w.lower()
            for w in text.split()
            if len(w) >= 3
            and not w.replace(".", "").isdigit()
            and w.upper() not in STOCK_SKIP_WORDS
        ]

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
                        matches.append(
                            {
                                "symbol": symbol,
                                "description": symbol_info.description,
                                "matched_word": word,
                            }
                        )
                        break

            except Exception as e:
                logger.debug(f"Error getting info for {symbol}: {e}")
                continue

        return matches

    def _select_best_match(self, matches: list[dict]) -> Optional[dict]:
        """Select the best match from multiple candidates"""
        if not matches:
            return None

        best_match = None
        best_score = 0

        for match in matches:
            # Score based on word length and exact matches
            score = len(match["matched_word"])

            # Bonus for exact word match in description
            description_words = match["description"].lower().split()
            if match["matched_word"] in description_words:
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
