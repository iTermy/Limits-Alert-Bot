from models.enums import SignalStatus


def format_price(price: float, symbol: str = None) -> str:
    """Format price with appropriate decimal places based on magnitude"""
    if price is None:
        return "N/A"

    if price < 0.0001:
        formatted = f"{price:.8f}"
    elif price < 0.01 or price < 10:
        formatted = f"{price:.5f}"
    elif price < 100:
        formatted = f"{price:.3f}"
    else:
        formatted = f"{price:.2f}"

    # Remove trailing zeros but keep at least one decimal
    if "." in formatted:
        formatted = formatted.rstrip("0")
        if formatted.endswith("."):
            formatted += "0"

    return formatted


def get_channel_name(channels_config: dict, channel_id: int) -> str | None:
    """Look up a monitored channel's name by its Discord ID."""
    for name, ch_id in channels_config.get("monitored_channels", {}).items():
        if int(ch_id) == channel_id:
            return name
    return None


def get_status_emoji(status: str) -> str:
    emoji_map = {
        SignalStatus.ACTIVE: "🟢",
        SignalStatus.HIT: "🎯",
        SignalStatus.PROFIT: "💰",
        SignalStatus.BREAKEVEN: "➖",
        SignalStatus.STOP_LOSS: "🛑",
        SignalStatus.CANCELLED: "❌",
    }
    return emoji_map.get(status, "❓")
