"""
Embed builders for the alert system.

Pure functions that construct Discord embeds for signal alerts and archives.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

import discord

from models.signal import LimitData, SignalData
from price_feeds.tp_config import TPConfig
from utils.logger import get_logger

logger = get_logger("embed_builders")

try:
    _tp_config = TPConfig()
except Exception:
    _tp_config = None


def _set_archive_footer(embed: discord.Embed, label: str = "📁 Archived") -> None:
    """Append the archive label to an embed footer, stripping any prior timer suffixes."""
    old = embed.footer.text or ""
    clean = old.split(" • ⏳")[0].split(" • 🗑️")[0]
    embed.set_footer(text=f"{clean} • {label}")


def _fmt(price: float) -> str:
    """Format price with appropriate decimal places."""
    if price == 0:
        return "0"
    s = f"{price:.5f}".rstrip("0").rstrip(".")
    if "." not in s:
        s += ".00"
    elif len(s.split(".")[1]) < 2:
        s += "0"
    return s


def _build_signal_embed(
    signal: SignalData,
    limits: List[LimitData],
    current_price: Optional[float] = None,
    distance_formatted: Optional[str] = None,
    spread: Optional[float] = None,
    spread_buffer_enabled: bool = False,
    event: str = "approaching",
    guild_id: Optional[int] = None,
    bot=None,
    hit_limit_ids: Optional[set] = None,
    force_hit_up_to_seq: int = 0,
    limit_pnl_map: Optional[Dict] = None,
    delete_after_minutes: Optional[int] = None,
) -> discord.Embed:
    """
    Build (or rebuild) the single persistent embed for a signal.
    event: "approaching" | "hit" | "stop_loss" | "auto_tp"
           | "profit" | "breakeven" | "cancelled" | "reactivated"

    hit_limit_ids: optional set of limit_id values that are confirmed hit.
                   Used when limits come from the hit-limits DB query, which
                   returns rows without a 'status' key.
    force_hit_up_to_seq: treat all limits with sequence_number <= this value
                   as hit, regardless of DB status. Used when the alert fires
                   before the DB write has committed.
    limit_pnl_map: sequence_number -> formatted pnl string, shown per-limit
                   on auto_tp embeds only (e.g. {1: "+3 pips", 2: "+4 pips"}).
    """
    instrument = signal.instrument
    direction = signal.direction.upper()
    signal_id = signal.signal_id
    total = len(limits) or signal.total_limits

    def _is_hit(lim: LimitData) -> bool:
        if force_hit_up_to_seq and lim.sequence_number <= force_hit_up_to_seq:
            return True
        if hit_limit_ids is not None and lim.id in hit_limit_ids:
            return True
        return bool(lim.hit_alert_sent or lim.status == "hit")

    hit_count = sum(1 for l in limits if _is_hit(l))

    status_map = {
        "approaching": (0xFFA500, "🟡 Approaching"),
        "hit": (0x00FF00, "🎯 Limit Hit"),
        "stop_loss": (0xFF0000, "🛑 Stop Loss"),
        "auto_tp": (0x00FF00, "💰 Auto Take-Profit"),
        "profit": (0x00FF00, "💰 Profit"),
        "breakeven": (0x808080, "➖ Breakeven"),
        "cancelled": (0x808080, "❌ Cancelled"),
        "expired": (0x808080, "⌛ Expired"),
        "spread_hour_cancelled": (0xFFA500, "🕔 Spread Hour — Cancelled"),
        "late_market_cancelled": (0xFFA500, "🕓 Late Market Hours — Cancelled"),
        "risky_window_cancelled": (0xE74C3C, "🚫 Risky Disabled — Cancelled"),
        "near_miss_cancelled": (0x808080, "❌ Near-Miss — Cancelled"),
        "reactivated": (0x3498DB, "♻️ Reactivated"),
        "edited": (0x3498DB, "📝 Updated"),
    }
    color, status_label = status_map.get(event, (0xFFA500, "🟡 Active"))

    embed = discord.Embed(
        title=f"{status_label} — {instrument} {direction}",
        color=color,
        timestamp=datetime.now(timezone.utc),
    )

    # ── Limits section ───────────────────────────────────────────────────────
    direction = (signal.direction or "long").lower()
    signal_type = signal.type

    sorted_limits = sorted(limits, key=lambda l: l.sequence_number)
    limit_lines = []
    for lim in sorted_limits:
        seq = lim.sequence_number
        price = _fmt(lim.price_level)
        if _is_hit(lim):
            per_limit_pnl = limit_pnl_map.get(seq) if limit_pnl_map else None
            if per_limit_pnl:
                limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅  +{per_limit_pnl}")
            elif current_price is not None and _tp_config is not None and lim.price_level:
                try:
                    pnl_val = _tp_config.calculate_pnl(
                        instrument, direction, lim.price_level, current_price,
                        signal_type=signal_type,
                    )
                    pnl_str = _tp_config.format_value(instrument, abs(pnl_val))
                    sign = "+" if pnl_val >= 0 else "-"
                    limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅  {sign}{pnl_str}")
                except Exception:
                    limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅")
            else:
                limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅")
        elif lim.status == "cancelled":
            limit_lines.append(f"Limit #{seq}: {price} ❌")
        else:
            limit_lines.append(f"Limit #{seq}: {price}")

    embed.add_field(
        name=f"Limits ({hit_count}/{total} hit)",
        value="\n".join(limit_lines) if limit_lines else "—",
        inline=False,
    )

    # ── TP price (auto_tp event only) ────────────────────────────────────────
    tp_price = signal.tp_price
    if event == "auto_tp" and tp_price is not None:
        embed.add_field(name="TP Price", value=f"**{_fmt(float(tp_price))}**", inline=True)

    # ── Stop loss ────────────────────────────────────────────────────────────
    sl = signal.stop_loss
    if sl:
        sl_label = f"~~{_fmt(sl)}~~ 🛑" if event == "stop_loss" else _fmt(sl)
        embed.add_field(name="Stop Loss", value=sl_label, inline=True)

    # ── Current price ────────────────────────────────────────────────────────
    if current_price is not None:
        if spread_buffer_enabled and spread and spread > 0 and event != "stop_loss":
            display_price = _fmt(current_price + spread)
        else:
            display_price = _fmt(current_price)
        embed.add_field(name="Current Price", value=display_price, inline=True)

    # ── Distance ─────────────────────────────────────────────────────────────
    if distance_formatted and event == "approaching":
        embed.add_field(name="Distance", value=distance_formatted, inline=True)
    elif distance_formatted and event == "hit":
        embed.add_field(name="Next Limit Distance", value=distance_formatted, inline=True)

    # ── Cancelled/expired reason notice ─────────────────────────────────────
    cancel_type = signal.closed_reason or ""
    is_expired = event == "expired" or cancel_type == "expiry"

    if event in (
        "cancelled",
        "expired",
        "near_miss_cancelled",
        "spread_hour_cancelled",
        "late_market_cancelled",
        "risky_window_cancelled",
    ):
        if event == "near_miss_cancelled" or cancel_type == "near_miss":
            reason_text = "Auto near-miss"
        elif event == "spread_hour_cancelled" or cancel_type == "spread_hour":
            reason_text = "Auto spread hour"
        elif event == "late_market_cancelled" or cancel_type == "late_market":
            reason_text = "Auto late market hours"
        elif event == "risky_window_cancelled" or cancel_type == "risky_window":
            reason_text = "Auto risky disabled window"
        elif is_expired:
            reason_text = "Auto expiry"
        elif cancel_type.startswith("news"):
            currency = cancel_type.split(":")[-1] if ":" in cancel_type else ""
            reason_text = "Auto news" + (f" ({currency})" if currency else "")
        elif cancel_type == "manual":
            reason_text = "Manual"
        elif cancel_type == "automatic":
            reason_text = "Auto expiry"
        else:
            reason_text = "Cancelled"

        embed.add_field(name="Reason", value=reason_text, inline=True)

    # ── Source link ──────────────────────────────────────────────────────────
    msg_id = signal.message_id
    ch_id = signal.channel_id
    if msg_id and ch_id and not str(msg_id).startswith("manual_"):
        if not guild_id and bot and bot.guilds:
            guild_id = bot.guilds[0].id
        if guild_id:
            url = f"https://discord.com/channels/{guild_id}/{ch_id}/{msg_id}"
            embed.add_field(name="Source", value=url, inline=False)

    _deletion_suffix = (
        f" • ⏳ Moving to archive in {delete_after_minutes} min" if delete_after_minutes else ""
    )

    if event == "expired" or (event == "cancelled" and cancel_type == "expiry"):
        embed.set_footer(text=f"Signal #{signal_id} • Auto-expired{_deletion_suffix}")
    elif event == "spread_hour_cancelled" or cancel_type == "spread_hour":
        embed.set_footer(
            text=f"Signal #{signal_id} • Auto-cancelled (spread hour){_deletion_suffix}"
        )
    elif event == "late_market_cancelled" or cancel_type == "late_market":
        embed.set_footer(
            text=f"Signal #{signal_id} • Auto-cancelled (late market hours){_deletion_suffix}"
        )
    elif event == "risky_window_cancelled" or cancel_type == "risky_window":
        embed.set_footer(
            text=f"Signal #{signal_id} • Auto-cancelled (risky disabled){_deletion_suffix}"
        )
    elif event == "near_miss_cancelled" or cancel_type == "near_miss":
        embed.set_footer(text=f"Signal #{signal_id} • Auto-cancelled (near-miss){_deletion_suffix}")
    elif event == "cancelled" and cancel_type.startswith("news"):
        embed.set_footer(text=f"Signal #{signal_id} • Auto-cancelled (news){_deletion_suffix}")
    elif event == "cancelled" and cancel_type == "automatic":
        embed.set_footer(text=f"Signal #{signal_id} • Auto-expired{_deletion_suffix}")
    elif _deletion_suffix:
        embed.set_footer(text=f"Signal #{signal_id}{_deletion_suffix}")
    else:
        embed.set_footer(text=f"Signal #{signal_id} • Reply to this message to manage")
    return embed


def _build_profit_archive_embed(
    sig_data: Optional[SignalData], signal_id: int, bot=None
) -> discord.Embed:
    """
    Build the dedicated profit embed posted to the profit channel when a signal
    is archived (after the END_STATE_DELETE_MINUTES window).
    """
    if not sig_data:
        return discord.Embed(
            title="💰 PROFIT",
            description=f"Signal #{signal_id} closed as profit.",
            color=0x00FF00,
            timestamp=datetime.now(timezone.utc),
        )

    instrument = sig_data.instrument or "?"
    direction = sig_data.direction.upper()
    sid = sig_data.signal_id or signal_id
    is_auto_tp = (sig_data.closed_reason or "") == "automatic"
    # Effective close price: a retrospective manual override wins over the
    # close recorded at profit time.
    tp_price = sig_data.manual_tp_price if sig_data.manual_tp_price is not None else sig_data.tp_price

    all_limits = sorted(sig_data.limits, key=lambda l: l.sequence_number)
    hit_limits = [l for l in all_limits if l.status == "hit" or l.hit_alert_sent]
    total = len(all_limits) or len(hit_limits)
    num_hit = len(hit_limits)

    method = "Auto Take-Profit" if is_auto_tp else "Manual Profit"

    embed = discord.Embed(
        title=f"💰 PROFIT — {instrument} {direction}",
        color=0x00FF00,
        timestamp=datetime.now(timezone.utc),
    )

    embed.add_field(name="Symbol", value=instrument, inline=True)
    embed.add_field(name="Position", value=direction, inline=True)
    embed.add_field(name="Method", value=method, inline=True)

    if hit_limits:
        dir_lc = (sig_data.direction or "long").lower()
        signal_type = sig_data.type.lower()
        close_price = float(tp_price) if tp_price is not None else None
        lines = []
        for l in hit_limits:
            seq = l.sequence_number
            price = _fmt(l.price_level)
            if close_price is not None and l.price_level and _tp_config is not None:
                try:
                    pnl_val = _tp_config.calculate_pnl(
                        instrument, dir_lc, l.price_level, close_price, signal_type=signal_type
                    )
                    pnl_str = _tp_config.format_value(instrument, abs(pnl_val))
                    sign = "+" if pnl_val >= 0 else "-"
                    lines.append(f"~~Limit #{seq}: {price}~~ ✅  {sign}{pnl_str}")
                except Exception:
                    lines.append(f"~~Limit #{seq}: {price}~~ ✅")
            else:
                lines.append(f"~~Limit #{seq}: {price}~~ ✅")
        embed.add_field(
            name=f"Limits Hit ({num_hit}/{total})",
            value="\n".join(lines),
            inline=False,
        )

    if sig_data.stop_loss:
        embed.add_field(name="Stop Loss", value=_fmt(sig_data.stop_loss), inline=True)

    if tp_price is not None:
        embed.add_field(name="TP Price", value=f"**{_fmt(float(tp_price))}**", inline=True)

    msg_id = sig_data.message_id
    ch_id = sig_data.channel_id
    if msg_id and ch_id and not str(msg_id).startswith("manual_"):
        guild_id = sig_data.guild_id
        if not guild_id and bot and bot.guilds:
            guild_id = bot.guilds[0].id
        if guild_id:
            url = f"https://discord.com/channels/{guild_id}/{ch_id}/{msg_id}"
            embed.add_field(name="Source", value=url, inline=False)

    embed.set_footer(text=f"Signal #{sid} • 📁 Profit Archived")
    return embed
