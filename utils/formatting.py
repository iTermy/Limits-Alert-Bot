"""
Formatting utilities for trading signals
"""


def format_price(price: float, symbol: str = None) -> str:
    """Format price with appropriate decimal places based on magnitude"""
    if price is None:
        return "N/A"

    if price < 0.0001:
        formatted = f"{price:.8f}"
    elif price < 0.01:
        formatted = f"{price:.5f}"
    elif price < 10:
        formatted = f"{price:.5f}"
    elif price < 100:
        formatted = f"{price:.3f}"
    else:
        formatted = f"{price:.2f}"

    # Remove trailing zeros but keep at least one decimal
    if '.' in formatted:
        formatted = formatted.rstrip('0')
        if formatted.endswith('.'):
            formatted += '0'

    return formatted


def format_distance_display(symbol: str, distance_value: float, is_crypto: bool = False) -> str:
    """Format distance for display (dollars for crypto/indices, pips for forex)"""
    if is_crypto:
        return f"${abs(distance_value):.2f}"
    else:
        if abs(distance_value) < 1:
            return f"{abs(distance_value):.1f} pip"
        return f"{abs(distance_value):.1f} pips"


def is_crypto_symbol(symbol: str) -> bool:
    """Check if symbol is a cryptocurrency"""
    symbol_upper = symbol.upper()
    crypto_indicators = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'DOT', 'USDT']
    return any(crypto in symbol_upper for crypto in crypto_indicators)


def is_index_symbol(symbol: str) -> bool:
    """Check if symbol is an index"""
    symbol_upper = symbol.upper()
    index_indicators = ['SPX', 'NAS', 'DOW', 'DAX', 'CHINA50', 'US500', 'USTEC',
                       'US30', 'US2000', 'RUSSEL', 'GER30', 'DE30', 'JP225', 'NIKKEI']
    return any(idx in symbol_upper for idx in index_indicators)


def get_status_emoji(status: str) -> str:
    """Get emoji for signal status"""
    status_lower = status.lower()
    if status_lower == 'active':
        return '🟢'
    elif status_lower == 'hit':
        return '🎯'
    elif status_lower == 'profit':
        return '💰'
    elif status_lower == 'stoploss':
        return '🛑'
    elif status_lower == 'expired':
        return '⏰'
    elif status_lower == 'cancelled':
        return '❌'
    return '⚪'