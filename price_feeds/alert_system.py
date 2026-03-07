"""
Alert System - Handles all alert generation and sending for the price monitor
REDESIGNED: Single persistent message per signal, edited in-place for all events.
Separate short ping messages are sent for each event so role pings still fire.
"""

import asyncio
import logging
from typing import Dict, Optional, List
from datetime import datetime, timezone
from enum import Enum
import discord

from utils.embed_factory import EmbedFactory
from utils.logger import get_logger

logger = get_logger('alert_system')


class AlertType(Enum):
    """Types of alerts"""
    APPROACHING = "approaching"
    HIT = "hit"
    STOP_LOSS = "stop_loss"


# ──────────────────────────────────────────────────────────────────────────────
# Embed builder helpers
# ──────────────────────────────────────────────────────────────────────────────

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
    signal: Dict,
    limits: List[Dict],
    current_price: Optional[float] = None,
    distance_formatted: Optional[str] = None,
    spread: Optional[float] = None,
    spread_buffer_enabled: bool = False,
    event: str = "approaching",
    guild_id: Optional[int] = None,
    bot=None,
    hit_limit_ids: Optional[set] = None,
    pnl_display: Optional[str] = None,
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
    pnl_display:   formatted profit string (e.g. "+12.3 pips") shown for
                   auto_tp and profit events only.
    force_hit_up_to_seq: treat all limits with sequence_number <= this value
                   as hit, regardless of DB status. Used when the alert fires
                   before the DB write has committed.
    limit_pnl_map: sequence_number -> formatted pnl string, shown per-limit
                   on auto_tp embeds only (e.g. {1: "+3 pips", 2: "+4 pips"}).
    """
    instrument = signal["instrument"]
    direction = signal["direction"].upper()
    signal_id = signal["signal_id"]
    total = len(limits) or signal.get("total_limits", 0)

    def _is_hit(lim: Dict) -> bool:
        if force_hit_up_to_seq and isinstance(lim.get("sequence_number"), int):
            if lim["sequence_number"] <= force_hit_up_to_seq:
                return True
        if hit_limit_ids is not None:
            lid = lim.get("limit_id") or lim.get("id")
            if lid is not None and lid in hit_limit_ids:
                return True
        return bool(lim.get("hit_alert_sent") or lim.get("status") == "hit")

    hit_count = sum(1 for l in limits if _is_hit(l))

    status_map = {
        "approaching":  (0xFFA500, "🟡 Approaching"),
        "hit":          (0x00FF00, "🎯 Limit Hit"),
        "stop_loss":    (0xFF0000, "🛑 Stop Loss"),
        "auto_tp":      (0x00FF00, "💰 Auto Take-Profit"),
        "profit":       (0x00FF00, "💰 Profit"),
        "breakeven":    (0x808080, "➖ Breakeven"),
        "cancelled":    (0x808080, "❌ Cancelled"),
        "expired":      (0x808080, "⌛ Expired"),
        "spread_hour_cancelled": (0xFFA500, "🕔 Spread Hour — Cancelled"),
        "near_miss_cancelled":   (0x808080, "❌ Near-Miss — Cancelled"),
        "reactivated":  (0x3498DB, "♻️ Reactivated"),
        "edited":       (0x3498DB, "📝 Updated"),
    }
    color, status_label = status_map.get(event, (0xFFA500, "🟡 Active"))

    embed = discord.Embed(
        title=f"{status_label} — {instrument} {direction}",
        color=color,
        timestamp=datetime.now(timezone.utc),
    )

    # ── Limits section ───────────────────────────────────────────────────────
    # Load tp_config once for live pnl calculations on hit limits
    _tp_config = None
    try:
        from price_feeds.tp_config import TPConfig
        _tp_config = TPConfig()
    except Exception:
        pass

    direction = signal.get("direction", "long").lower()
    is_scalp = bool(signal.get("scalp", False))

    sorted_limits = sorted(limits, key=lambda l: l.get("sequence_number", 0))
    limit_lines = []
    for lim in sorted_limits:
        seq = lim.get("sequence_number", "?")
        price = _fmt(lim["price_level"])
        if _is_hit(lim):
            # Priority 1: explicit pnl_map (e.g. auto_tp final values)
            per_limit_pnl = limit_pnl_map.get(seq) if limit_pnl_map else None
            if per_limit_pnl:
                limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅  +{per_limit_pnl}")
            elif current_price is not None and _tp_config is not None:
                # Live pnl: use hit_price if available, otherwise fall back to price_level
                entry = lim.get("hit_price") or lim.get("price_level")
                if entry:
                    try:
                        pnl_val = _tp_config.calculate_pnl(
                            instrument, direction, entry, current_price, scalp=is_scalp
                        )
                        pnl_str = _tp_config.format_value(instrument, abs(pnl_val))
                        sign = "+" if pnl_val >= 0 else "-"
                        limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅  {sign}{pnl_str}")
                    except Exception:
                        limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅")
                else:
                    limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅")
            else:
                limit_lines.append(f"~~Limit #{seq}: {price}~~ ✅")
        else:
            limit_lines.append(f"Limit #{seq}: {price}")

    embed.add_field(
        name=f"Limits ({hit_count}/{total} hit)",
        value="\n".join(limit_lines) if limit_lines else "—",
        inline=False,
    )

    # ── Profit display (auto_tp / profit events only) ────────────────────────
    if pnl_display and event in ("auto_tp", "profit"):
        embed.add_field(name="Profit", value=f"**+{pnl_display}**", inline=True)

    # ── Stop loss ────────────────────────────────────────────────────────────
    sl = signal.get("stop_loss")
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
    cancel_type = signal.get("cancel_type") or signal.get("closed_reason") or ""
    is_expired = event == "expired" or cancel_type == "expiry"

    if event in ("cancelled", "expired", "near_miss_cancelled", "spread_hour_cancelled"):
        if event == "near_miss_cancelled" or cancel_type == "near_miss":
            reason_text = "Auto near-miss"
        elif event == "spread_hour_cancelled" or cancel_type == "spread_hour":
            reason_text = "Auto spread hour"
        elif is_expired:
            reason_text = "Auto expiry"
        elif cancel_type.startswith("news"):
            currency = cancel_type.split(":")[-1] if ":" in cancel_type else ""
            reason_text = f"Auto news" + (f" ({currency})" if currency else "")
        elif cancel_type == "manual":
            reason_text = "Manual"
        elif cancel_type == "automatic":
            reason_text = "Auto expiry"
        else:
            reason_text = "Cancelled"

        embed.add_field(name="Reason", value=reason_text, inline=True)

    # ── Source link ──────────────────────────────────────────────────────────
    msg_id = signal.get("message_id")
    ch_id = signal.get("channel_id")
    if msg_id and ch_id and not str(msg_id).startswith("manual_"):
        if not guild_id and bot and bot.guilds:
            guild_id = bot.guilds[0].id
        if guild_id:
            url = f"https://discord.com/channels/{guild_id}/{ch_id}/{msg_id}"
            embed.add_field(name="Source", value=url, inline=False)

    _deletion_suffix = f" • ⏳ Moving to archive in {delete_after_minutes} min" if delete_after_minutes else ""

    if event == "expired" or (event == "cancelled" and cancel_type == "expiry"):
        embed.set_footer(text=f"Signal #{signal_id} • Auto-expired{_deletion_suffix}")
    elif event == "spread_hour_cancelled" or cancel_type == "spread_hour":
        embed.set_footer(text=f"Signal #{signal_id} • Auto-cancelled (spread hour){_deletion_suffix}")
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


def _build_profit_archive_embed(sig_data: Optional[Dict], signal_id: int, bot=None) -> discord.Embed:
    """
    Build the dedicated profit embed posted to the profit channel when a signal
    is archived (after the END_STATE_DELETE_MINUTES window).

    Shows: instrument, direction, hit limits, stop loss, P&L (if available),
    source link, and whether it was auto-TP or manual profit.
    """
    if not sig_data:
        return discord.Embed(
            title="💰 PROFIT",
            description=f"Signal #{signal_id} closed as profit.",
            color=0x00FF00,
            timestamp=datetime.now(timezone.utc),
        )

    instrument = sig_data.get("instrument", "?")
    direction = (sig_data.get("direction") or "").upper()
    sid = sig_data.get("signal_id") or sig_data.get("id", signal_id)
    db_status = sig_data.get("status", "")
    closed_reason = sig_data.get("closed_reason") or ""
    is_auto_tp = closed_reason == "automatic"
    result_pips = sig_data.get("result_pips")

    all_limits = sorted(sig_data.get("limits", []), key=lambda l: l.get("sequence_number", 0))
    hit_limits = [l for l in all_limits if l.get("status") == "hit" or l.get("hit_alert_sent")]
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
        lines = [
            f"Limit #{l.get('sequence_number', '?')}: {_fmt(l.get('price_level', 0))} ✅"
            for l in hit_limits
        ]
        embed.add_field(
            name=f"Limits Hit ({num_hit}/{total})",
            value="\n".join(lines),
            inline=False,
        )

    if sig_data.get("stop_loss"):
        embed.add_field(name="Stop Loss", value=_fmt(sig_data["stop_loss"]), inline=True)

    if result_pips is not None:
        try:
            from price_feeds.tp_config import TPConfig
            _tp = TPConfig()
            pnl_str = _tp.format_value(instrument, abs(float(result_pips)))
            sign = "+" if float(result_pips) >= 0 else "-"
            embed.add_field(name="P&L", value=f"**{sign}{pnl_str}**", inline=True)
        except Exception:
            embed.add_field(name="P&L", value=f"**+{result_pips:.1f}**", inline=True)

    # Source link
    msg_id = sig_data.get("message_id")
    ch_id = sig_data.get("channel_id")
    if msg_id and ch_id and not str(msg_id).startswith("manual_"):
        guild_id = sig_data.get("guild_id")
        if not guild_id and bot and bot.guilds:
            guild_id = bot.guilds[0].id
        if guild_id:
            url = f"https://discord.com/channels/{guild_id}/{ch_id}/{msg_id}"
            embed.add_field(name="Source", value=url, inline=False)

    embed.set_footer(text=f"Signal #{sid} • 📁 Profit Archived")
    return embed


# ──────────────────────────────────────────────────────────────────────────────
# AlertSystem
# ──────────────────────────────────────────────────────────────────────────────

class AlertSystem:
    """
    Handles all alert generation and sending for trading signals.

    NEW BEHAVIOUR
    ─────────────
    One persistent Discord message (embed) is created per signal when the first
    approaching / hit alert fires.  All subsequent events (more limits hit,
    stop loss, auto-TP, manual overrides) EDIT that same message instead of
    posting new ones.

    Pinging
    ───────
    Because editing a message does NOT ping anyone, a short plain-text ping
    message is sent as a REPLY to the persistent embed for each event.
    Previous ping messages are deleted before sending a new one, keeping
    the channel tidy.

    Manual overrides
    ────────────────
    Call update_signal_message(signal, event, ...) from message_handler.py after
    processing reply commands (profit, sl, cancel, etc.) to update the embed.

    Live Price Updates
    ──────────────────
    Active approaching/hit embeds are refreshed every LIVE_UPDATE_INTERVAL seconds
    with the latest price and distance. Only embeds in "live" states (approaching,
    hit) are updated; terminal states (profit, cancelled, etc.) are left static.
    Updates are staggered 1 second apart to stay well within Discord's 5 edits /
    5 seconds per-channel rate limit.
    """

    # How often (seconds) to refresh live embeds with the latest price/distance
    LIVE_UPDATE_INTERVAL = 15

    def __init__(self, alert_channel: Optional[discord.TextChannel] = None, bot=None):
        self.alert_channel = alert_channel
        self.pa_alert_channel = None
        self.toll_alert_channel = None
        self.general_toll_alert_channel = None
        self._load_pa_channels()
        self._load_toll_channels()
        self.bot = bot

        # signal_id → discord.Message  (the one persistent embed per signal)
        self.signal_messages: Dict[int, discord.Message] = {}

        # signal_id → discord.Message  (the most recent ping message per signal)
        self.signal_ping_messages: Dict[int, discord.Message] = {}

        # signal_id → discord.Message  (embed in finished-signals channel after move)
        self.signal_finished_messages: Dict[int, discord.Message] = {}

        # BACKWARDS COMPAT: alert message ID (str) → signal_id
        self.alert_messages: Dict[str, int] = {}

        # signal_id → {"signal": dict, "event": str, "spread_buffer_enabled": bool}
        # Tracks which signals have "live" embeds that should be refreshed with prices
        self._live_embeds: Dict[int, Dict] = {}

        # Background task handle
        self._live_update_task: Optional[asyncio.Task] = None

        # signal_id → asyncio.Task  (pending auto-delete tasks for end-state embeds)
        self._deletion_tasks: Dict[int, asyncio.Task] = {}

        self.stats = {
            "approaching_sent": 0,
            "hit_sent": 0,
            "stop_loss_sent": 0,
            "auto_tp_sent": 0,
            "spread_hour_cancelled": 0,
            "total_alerts": 0,
            "errors": 0,
        }

    # ── Live update loop ─────────────────────────────────────────────────────

    def start_live_updates(self):
        """Start the background task that refreshes live embeds. Call after bot is ready."""
        if self._live_update_task and not self._live_update_task.done():
            return
        self._live_update_task = asyncio.create_task(self._live_update_loop())
        logger.info("Live embed update loop started")

    def stop_live_updates(self):
        """Cancel the live update loop and any pending archive-move tasks."""
        if self._live_update_task and not self._live_update_task.done():
            self._live_update_task.cancel()
            logger.info("Live embed update loop stopped")
        for signal_id, task in list(self._deletion_tasks.items()):
            if not task.done():
                task.cancel()
        self._deletion_tasks.clear()
        logger.info(f"Cancelled all pending end-state archive-move tasks")

    async def _live_update_loop(self):
        """
        Periodically refreshes all active approaching/hit embeds with the latest
        price and distance. Staggered 1 second per embed to respect Discord rate limits.
        """
        await asyncio.sleep(5)  # Brief startup delay
        while True:
            try:
                await asyncio.sleep(self.LIVE_UPDATE_INTERVAL)
                await self._refresh_live_embeds()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Live update loop error: {e}", exc_info=True)

    async def _refresh_live_embeds(self):
        """Refresh each live embed with the latest price, staggered 1s apart."""
        if not self._live_embeds:
            return
        if not (self.bot and hasattr(self.bot, "monitor") and self.bot.monitor):
            return

        monitor = self.bot.monitor
        stream_manager = getattr(monitor, "stream_manager", None)
        if not stream_manager:
            return

        signal_ids = list(self._live_embeds.keys())
        logger.debug(f"Refreshing {len(signal_ids)} live embed(s)")

        for i, signal_id in enumerate(signal_ids):
            # Stagger updates: 1 second apart to stay well under rate limits
            if i > 0:
                await asyncio.sleep(1)

            entry = self._live_embeds.get(signal_id)
            if not entry:
                continue

            signal = entry["signal"]
            event = entry["event"]
            spread_buffer_enabled = entry.get("spread_buffer_enabled", False)

            try:
                # Get latest price from the stream manager
                instrument = signal["instrument"]
                price_data = await stream_manager.get_latest_price(instrument)
                if not price_data:
                    continue

                direction = signal.get("direction", "long").lower()
                current_price = price_data["ask"] if direction == "long" else price_data["bid"]
                spread = price_data.get("spread", 0.0)

                # Fetch fresh limits from DB
                limits = await self._fetch_limits(signal)

                # Calculate distance for approaching embeds (to nearest pending limit)
                # and for hit embeds (to the next pending limit)
                distance_formatted = None
                if event in ("approaching", "hit"):
                    pending_limits = [
                        l for l in limits
                        if l.get("status") != "hit" and not l.get("hit_alert_sent")
                    ]
                    if pending_limits:
                        nearest = min(
                            pending_limits,
                            key=lambda l: abs(current_price - l["price_level"])
                        )
                        distance = abs(current_price - nearest["price_level"])
                        if hasattr(monitor, "alert_config") and monitor.alert_config:
                            distance_formatted = monitor.alert_config.format_distance_for_display(
                                instrument, distance, current_price
                            )
                        else:
                            distance_formatted = f"{distance:.5f}".rstrip("0").rstrip(".")

                # Re-check that the signal is still live before editing — a manual
                # status change (profit, sl, cancel) may have called _unregister_live_embed
                # while we were awaiting price data or limits above.
                if signal_id not in self._live_embeds:
                    logger.debug(f"Live update: signal {signal_id} was unregistered mid-cycle, skipping")
                    continue

                # Rebuild and edit the embed (no ping — this is a silent live update)
                existing_msg = self.signal_messages.get(signal_id)
                if not existing_msg:
                    continue

                guild_id = signal.get("guild_id")
                if not guild_id and self.bot and self.bot.guilds:
                    guild_id = self.bot.guilds[0].id

                embed = _build_signal_embed(
                    signal=signal,
                    limits=limits,
                    current_price=current_price,
                    distance_formatted=distance_formatted,
                    spread=spread,
                    spread_buffer_enabled=spread_buffer_enabled,
                    event=event,
                    guild_id=guild_id,
                    bot=self.bot,
                )

                try:
                    await existing_msg.edit(embed=embed)
                    logger.debug(f"Live-updated embed for signal {signal_id} @ {current_price}")
                except discord.NotFound:
                    logger.warning(f"Live update: embed for signal {signal_id} not found, removing")
                    self._live_embeds.pop(signal_id, None)
                    self.signal_messages.pop(signal_id, None)
                except discord.HTTPException as e:
                    if e.status == 429:
                        logger.warning(f"Live update rate-limited for signal {signal_id}, skipping this cycle")
                    else:
                        logger.warning(f"Live update HTTP error for signal {signal_id}: {e}")

            except Exception as e:
                logger.error(f"Live update failed for signal {signal_id}: {e}", exc_info=True)

    def _register_live_embed(self, signal: Dict, event: str, spread_buffer_enabled: bool = False):
        """Register or update a signal embed as 'live' so it gets periodic price updates."""
        signal_id = signal["signal_id"]
        self._live_embeds[signal_id] = {
            "signal": signal,
            "event": event,
            "spread_buffer_enabled": spread_buffer_enabled,
        }

    def _unregister_live_embed(self, signal_id: int):
        """Remove a signal from live tracking (called when it reaches a terminal state)."""
        self._live_embeds.pop(signal_id, None)

    # ── End-state auto-deletion ──────────────────────────────────────────────

    # End states that trigger the 15-minute deletion countdown.
    # Every state where the signal will not be re-traded belongs here.
    _END_STATES = {
        "profit", "auto_tp",           # take-profit outcomes
        "stop_loss",                    # stop loss hit
        "cancelled", "near_miss_cancelled",  # manual or auto cancel
        "spread_hour_cancelled",        # auto-cancel during spread hour
        "expired",                      # auto-expired by expiry manager
        "breakeven",                    # closed at breakeven
    }
    # How long (minutes) to wait before deleting end-state embeds
    END_STATE_DELETE_MINUTES = 15

    def _cancel_deletion_task(self, signal_id: int):
        """Cancel any pending move-to-finished task for a signal (e.g. on reactivation)."""
        task = self._deletion_tasks.pop(signal_id, None)
        if task and not task.done():
            task.cancel()
            logger.debug(f"Cancelled pending move-to-finished task for signal {signal_id}")

    def _get_finished_channel(self) -> Optional[discord.TextChannel]:
        """
        Return the finished-signals Discord channel, or None if not configured.
        Reads `finished_signals` from channels.json each time so hot-reloads work.
        """
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).resolve().parent.parent / "config" / "channels.json"
            with open(config_path) as f:
                cfg = json.load(f)
            finished_id = cfg.get("finished_signals")
            if not finished_id:
                return None
            channel = self.bot.get_channel(int(finished_id)) if self.bot else None
            return channel
        except Exception as e:
            logger.warning(f"Could not load finished_signals channel: {e}")
            return None

    def _get_profit_channel_sync(self) -> Optional[discord.TextChannel]:
        """
        Return the profit Discord channel from channels.json synchronously,
        or None if not configured. Used inside the archive-move task.
        """
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).resolve().parent.parent / "config" / "channels.json"
            with open(config_path) as f:
                cfg = json.load(f)
            profit_id = cfg.get("profit_channel")
            if not profit_id:
                return None
            channel = self.bot.get_channel(int(profit_id)) if self.bot else None
            return channel
        except Exception as e:
            logger.warning(f"Could not load profit_channel: {e}")
            return None

    # Profit events — moved to profit_channel instead of finished_signals channel
    _PROFIT_EVENTS = {"profit", "auto_tp"}

    def _schedule_end_state_deletion(self, signal_id: int, event: str = ""):
        """
        Schedule the persistent embed to be moved out of the alert channel after
        END_STATE_DELETE_MINUTES minutes.

        Routing:
          • profit / auto_tp  → profit_channel  (so wins land in the profit log)
          • everything else   → finished_signals channel  (so members can see why
                                a signal was cancelled / stopped out)

        If the target channel is not configured, the embed is simply deleted.

        Steps after the delay:
          1. Delete the ping reply.
          2. Re-send the embed (updated footer) to the target channel.
          3. Track the new message so reply commands still work.
          4. Delete the original embed from the alert channel.
        """
        self._cancel_deletion_task(signal_id)
        is_profit = event in self._PROFIT_EVENTS

        async def _move_after_delay():
            try:
                await asyncio.sleep(self.END_STATE_DELETE_MINUTES * 60)
            except asyncio.CancelledError:
                return

            # ── 1. Delete the ping reply ──────────────────────────────────────
            ping_msg = self.signal_ping_messages.pop(signal_id, None)
            if ping_msg:
                try:
                    await ping_msg.delete()
                    logger.debug(f"Deleted ping for signal {signal_id} before archive move")
                except Exception:
                    pass

            # ── 2. Grab the original embed message ───────────────────────────
            embed_msg = self.signal_messages.get(signal_id)
            if not embed_msg:
                self._deletion_tasks.pop(signal_id, None)
                return

            # ── 3. Pick the destination channel ──────────────────────────────
            if is_profit:
                dest_channel = self._get_profit_channel_sync()
                archive_label = "📁 Profit Archived"
                dest_name = "profit channel"
            else:
                dest_channel = self._get_finished_channel()
                archive_label = "📁 Archived"
                dest_name = "finished-signals channel"

            if dest_channel:
                try:
                    # Fetch fresh signal data from DB once — used for both paths
                    sig_data = None
                    if self.bot and hasattr(self.bot, "signal_db") and self.bot.signal_db:
                        try:
                            sig_data = await self.bot.signal_db.get_signal_with_limits(signal_id)
                            if sig_data and "signal_id" not in sig_data:
                                sig_data = dict(sig_data)
                                sig_data["signal_id"] = sig_data.get("id", signal_id)
                        except Exception as _fetch_err:
                            logger.warning(f"Could not fetch signal {signal_id} from DB for archive: {_fetch_err}")

                    if is_profit:
                        # ── Profit channel: send a dedicated profit summary embed ────
                        new_embed = _build_profit_archive_embed(sig_data, signal_id, self.bot)
                    else:
                        # ── Finished channel: rebuild the signal embed with correct event ──
                        new_embed = None
                        if sig_data:
                            try:
                                db_status = sig_data.get("status", "")
                                cancel_type_db = sig_data.get("closed_reason") or ""
                                _status_to_event = {
                                    "profit": "profit",
                                    "auto_tp": "auto_tp",
                                    "stop_loss": "stop_loss",
                                    "cancelled": "cancelled",
                                    "expired": "expired",
                                    "breakeven": "breakeven",
                                }
                                rebuild_event = _status_to_event.get(db_status, event)
                                if rebuild_event == "cancelled":
                                    if cancel_type_db == "near_miss":
                                        rebuild_event = "near_miss_cancelled"
                                    elif cancel_type_db == "spread_hour":
                                        rebuild_event = "spread_hour_cancelled"

                                guild_id_val = sig_data.get("guild_id")
                                if not guild_id_val and self.bot and self.bot.guilds:
                                    guild_id_val = self.bot.guilds[0].id

                                new_embed = _build_signal_embed(
                                    signal=sig_data,
                                    limits=sig_data.get("limits", []),
                                    event=rebuild_event,
                                    guild_id=guild_id_val,
                                    bot=self.bot,
                                )
                                old_footer = new_embed.footer.text or ""
                                clean_footer = old_footer.split(" • ⏳")[0].split(" • 🗑️")[0]
                                new_embed.set_footer(text=f"{clean_footer} • 📁 Archived")
                            except Exception as _rebuild_err:
                                logger.warning(f"Could not rebuild embed for signal {signal_id} from DB: {_rebuild_err}")

                        # Fallback: copy the existing embed if DB rebuild failed
                        if new_embed is None:
                            existing_embed = embed_msg.embeds[0] if embed_msg.embeds else None
                            if existing_embed:
                                new_embed = existing_embed.copy()
                                old_footer = existing_embed.footer.text or ""
                                clean_footer = old_footer.split(" • ⏳")[0].split(" • 🗑️")[0]
                                new_embed.set_footer(text=f"{clean_footer} • 📁 Archived")
                            else:
                                new_embed = discord.Embed(
                                    description="Signal reached a final state.",
                                    color=0x808080,
                                )

                    finished_msg = await dest_channel.send(embed=new_embed)
                    self.signal_finished_messages[signal_id] = finished_msg
                    self.track_alert_message(finished_msg.id, signal_id)
                    logger.info(
                        f"Moved signal {signal_id} embed to {dest_name} (msg {finished_msg.id})"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to send embed to {dest_name} for signal {signal_id}: {e}"
                    )

                # ── 4. Delete the original from the alert channel ─────────────
                try:
                    await embed_msg.delete()
                    logger.info(
                        f"Deleted alert-channel embed for signal {signal_id} after move to {dest_name}"
                    )
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(
                        f"Failed to delete alert embed for signal {signal_id}: {e}"
                    )
            else:
                # Target channel not configured — fall back to plain delete
                try:
                    await embed_msg.delete()
                    logger.info(
                        f"Deleted end-state embed for signal {signal_id} "
                        f"(no {dest_name} configured)"
                    )
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(f"Failed to delete embed for signal {signal_id}: {e}")

            # ── 5. Clean up in-memory tracking ───────────────────────────────
            self.signal_messages.pop(signal_id, None)
            if embed_msg:
                self.alert_messages.pop(str(embed_msg.id), None)
            self._deletion_tasks.pop(signal_id, None)

            # ── 6. Gold-tolls only: delete the original signal message ────────
            # For signals originating in the gold-tolls-map channel, also remove
            # the sender's original message so the channel stays clean.
            try:
                if self.bot and hasattr(self.bot, "signal_db") and self.bot.signal_db:
                    sig_data = await self.bot.signal_db.get_signal_with_limits(signal_id)
                    if sig_data:
                        await self._maybe_delete_toll_original(sig_data, signal_id)
            except Exception as e:
                logger.warning(f"Gold-tolls original message cleanup failed for signal {signal_id}: {e}")

        task = asyncio.ensure_future(_move_after_delay())
        self._deletion_tasks[signal_id] = task
        logger.info(
            f"Scheduled archive move for signal {signal_id} (event='{event}') "
            f"in {self.END_STATE_DELETE_MINUTES} minutes → "
            f"{'profit channel' if is_profit else 'finished-signals channel'}"
        )

    # ── Channel helpers ──────────────────────────────────────────────────────

    def set_channel(self, channel: discord.TextChannel):
        self.alert_channel = channel
        logger.info(f"Alert channel set to #{channel.name} ({channel.id})")

    def set_pa_channel(self, channel: discord.TextChannel):
        self.pa_alert_channel = channel
        logger.info(f"PA alert channel set: #{channel.name} ({channel.id})")

    def set_toll_channel(self, channel: discord.TextChannel):
        self.toll_alert_channel = channel
        logger.info(f"Toll alert channel set: #{channel.name} ({channel.id})")

    def set_general_toll_channel(self, channel: discord.TextChannel):
        self.general_toll_alert_channel = channel
        logger.info(f"General-toll alert channel set: #{channel.name} ({channel.id})")

    def _load_pa_channels(self):
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).parent.parent / "config" / "channels.json"
            with open(config_path) as f:
                cfg = json.load(f)
            monitored = cfg.get("monitored_channels", {})
            self.pa_channel_ids = {
                str(v) for k, v in monitored.items()
                if "pa" in k.lower() or "price-action" in k.lower()
            }
            logger.info(f"Loaded {len(self.pa_channel_ids)} PA channel IDs")
        except Exception as e:
            logger.error(f"Failed to load PA channels: {e}")
            self.pa_channel_ids = set()

    def _load_toll_channels(self):
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).parent.parent / "config" / "channels.json"
            with open(config_path) as f:
                cfg = json.load(f)
            monitored = cfg.get("monitored_channels", {})
            self.toll_channel_ids = set()
            self.general_toll_channel_ids = set()
            for channel_name, channel_id in monitored.items():
                if not channel_id:
                    continue
                if channel_name.lower() == "general-tolls":
                    self.general_toll_channel_ids.add(str(channel_id))
                elif "toll" in channel_name.lower():
                    self.toll_channel_ids.add(str(channel_id))
            logger.info(f"Loaded {len(self.toll_channel_ids)} toll channel IDs")
            logger.info(f"Loaded {len(self.general_toll_channel_ids)} general-toll channel IDs")
        except Exception as e:
            logger.error(f"Failed to load toll channels: {e}")
            self.toll_channel_ids = set()
            self.general_toll_channel_ids = set()

    def is_pa_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.pa_channel_ids

    def is_toll_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.toll_channel_ids

    def is_general_toll_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.general_toll_channel_ids

    async def _maybe_delete_toll_original(self, signal: Dict, signal_id: int) -> None:
        """
        Delete the original signal message for gold-toll signals (channel is in toll_channel_ids).
        Safe to call on any signal — silently skips non-toll and manual signals.
        """
        src_channel_id = str(signal.get("channel_id", ""))
        src_message_id = str(signal.get("message_id", ""))
        if (
            src_channel_id not in self.toll_channel_ids
            or not src_message_id
            or src_message_id.startswith("manual_")
        ):
            return
        try:
            src_channel = self.bot.get_channel(int(src_channel_id)) if self.bot else None
            if not src_channel:
                return
            try:
                src_msg = await src_channel.fetch_message(int(src_message_id))
                await src_msg.delete()
                logger.info(
                    f"Deleted gold-tolls original message {src_message_id} for signal {signal_id}"
                )
            except discord.NotFound:
                pass  # Already deleted
            except discord.Forbidden:
                logger.warning(
                    f"No permission to delete gold-tolls message {src_message_id} for signal {signal_id}"
                )
            except Exception as e:
                logger.warning(f"Could not delete gold-tolls message {src_message_id}: {e}")
        except Exception as e:
            logger.warning(f"Gold-toll original message cleanup failed for signal {signal_id}: {e}")

    def _get_alert_channel(self, signal: Dict) -> Optional[discord.TextChannel]:
        # Most-specific routing first
        if self.is_general_toll_signal(signal):
            if self.general_toll_alert_channel:
                return self.general_toll_alert_channel
            logger.warning("General-toll signal but no general-toll alert channel; falling back")
        if self.is_toll_signal(signal):
            if self.toll_alert_channel:
                return self.toll_alert_channel
            logger.warning("Toll signal but no toll alert channel; falling back")
        if self.is_pa_signal(signal):
            if self.pa_alert_channel:
                return self.pa_alert_channel
            logger.warning("PA signal but no PA alert channel; falling back")
        return self.alert_channel

    # ── Backwards-compat tracking ────────────────────────────────────────────

    def track_alert_message(self, message_id: int, signal_id: int):
        """Register a message_id → signal_id mapping (used by reply handler)."""
        self.alert_messages[str(message_id)] = signal_id
        if len(self.alert_messages) > 1000:
            for k in list(self.alert_messages)[:len(self.alert_messages) - 1000]:
                del self.alert_messages[k]

    def get_signal_from_alert(self, message_id: str) -> Optional[int]:
        return self.alert_messages.get(str(message_id))

    # ── Limit fetcher ────────────────────────────────────────────────────────

    async def _fetch_limits(self, signal: Dict) -> List[Dict]:
        """
        Get ALL limits for a signal (hit + pending) from the DB.
        The DB is always the source of truth — the signal dict may only have
        pending_limits (unhit), which would cause the embed to show wrong data.
        Falls back to signal dict only if the DB call fails or bot is unavailable.
        """
        if self.bot and hasattr(self.bot, "signal_db") and self.bot.signal_db:
            try:
                full = await self.bot.signal_db.get_signal_with_limits(signal["signal_id"])
                if full:
                    return full.get("limits", [])
            except Exception as e:
                logger.warning(f"Could not fetch limits from DB for signal {signal['signal_id']}: {e}")
        # Fallback: prefer 'limits' (all) over 'pending_limits' (subset)
        return signal.get("limits") or signal.get("pending_limits") or []

    # ── Core: get/create/edit the persistent message ─────────────────────────

    async def _upsert_signal_message(
        self,
        signal: Dict,
        limits: List[Dict],
        event: str,
        current_price: Optional[float] = None,
        distance_formatted: Optional[str] = None,
        spread: Optional[float] = None,
        spread_buffer_enabled: bool = False,
        ping_text: Optional[str] = None,
        hit_limit_ids: Optional[set] = None,
        pnl_display: Optional[str] = None,
        force_hit_up_to_seq: int = 0,
        limit_pnl_map: Optional[Dict] = None,
        delete_after_minutes: Optional[int] = None,
    ) -> Optional[discord.Message]:
        """
        Send a ping then create or edit the persistent embed for this signal.

        The ping message is always a NEW reply (so the role gets notified).
        The embed is edited if one already exists, created otherwise.
        """
        signal_id = signal["signal_id"]
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            logger.error("No alert channel configured")
            return None

        guild_id = signal.get("guild_id")
        if not guild_id and self.bot and self.bot.guilds:
            guild_id = self.bot.guilds[0].id

        embed = _build_signal_embed(
            signal=signal,
            limits=limits,
            current_price=current_price,
            distance_formatted=distance_formatted,
            spread=spread,
            spread_buffer_enabled=spread_buffer_enabled,
            event=event,
            guild_id=guild_id,
            bot=self.bot,
            hit_limit_ids=hit_limit_ids,
            pnl_display=pnl_display,
            force_hit_up_to_seq=force_hit_up_to_seq,
            limit_pnl_map=limit_pnl_map,
            delete_after_minutes=delete_after_minutes,
        )

        # ── Edit or create the persistent embed ───────────────────────────────
        existing_msg = self.signal_messages.get(signal_id)
        embed_msg = None

        if existing_msg:
            try:
                await existing_msg.edit(embed=embed)
                logger.info(f"Edited persistent message for signal {signal_id} (event={event})")
                embed_msg = existing_msg
            except discord.NotFound:
                logger.warning(f"Persistent message for signal {signal_id} deleted — recreating")
                del self.signal_messages[signal_id]
                existing_msg = None
            except Exception as e:
                logger.error(f"Failed to edit persistent message for signal {signal_id}: {e}")
                return None

        if not existing_msg:
            try:
                # On the initial send, include the role mention as content so users
                # are notified even though there's no separate ping reply yet.
                role_mention = "<@&1334203997107650662>"
                embed_msg = await target_channel.send(content=role_mention, embed=embed)
                self.signal_messages[signal_id] = embed_msg
                self.track_alert_message(embed_msg.id, signal_id)
                logger.info(f"Created persistent message for signal {signal_id} (event={event})")
            except Exception as e:
                logger.error(f"Failed to send new persistent message for signal {signal_id}: {e}")
                return None

        # ── Delete old ping, send new one as a reply to the embed ─────────────
        if ping_text and embed_msg:
            old_ping = self.signal_ping_messages.get(signal_id)
            if old_ping:
                try:
                    await old_ping.delete()
                    logger.debug(f"Deleted old ping for signal {signal_id}")
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(f"Could not delete old ping for signal {signal_id}: {e}")

            try:
                role_mention = "<@&1334203997107650662>"
                new_ping = await embed_msg.reply(f"{role_mention} {ping_text}")
                self.signal_ping_messages[signal_id] = new_ping
                logger.debug(f"Sent new ping for signal {signal_id} (event={event})")
            except Exception as e:
                logger.error(f"Failed to send ping for signal {signal_id}: {e}")

        return embed_msg

    # ── Public alert API ─────────────────────────────────────────────────────

    async def send_approaching_alert(
        self,
        signal: Dict,
        limit: Dict,
        current_price: float,
        distance_formatted: str,
        spread: float = None,
        spread_buffer_enabled: bool = False,
    ) -> bool:
        if limit.get("sequence_number", 0) != 1:
            logger.debug(f"Skipping approaching alert for limit #{limit['sequence_number']} (not first)")
            return False
        try:
            limits = await self._fetch_limits(signal)
            if not limits:
                limits = [limit]
            # No ping for approaching — the embed itself is the first notification.
            # Pings are only sent when the embed is *updated* (hit, SL, profit, etc.)
            msg = await self._upsert_signal_message(
                signal=signal, limits=limits, event="approaching",
                current_price=current_price, distance_formatted=distance_formatted,
                spread=spread, spread_buffer_enabled=spread_buffer_enabled,
                ping_text=None,
            )
            if msg:
                self._register_live_embed(signal, "approaching", spread_buffer_enabled)
                self.stats["approaching_sent"] += 1
                self.stats["total_alerts"] += 1
                return True
        except Exception as e:
            logger.error(f"Failed to send approaching alert: {e}", exc_info=True)
            self.stats["errors"] += 1
        return False

    async def send_limit_hit_alert(
        self,
        signal: Dict,
        limit: Dict,
        current_price: float,
        spread: float = None,
        spread_buffer_enabled: bool = False,
    ) -> bool:
        try:
            limits = await self._fetch_limits(signal)
            if not limits:
                limits = [limit]
            seq = limit.get("sequence_number", "?")
            total = len(limits)

            # The DB status may not be committed yet at the moment this alert fires,
            # so we force-mark the current limit as hit by its sequence_number.
            # Any limits with seq <= current seq are treated as hit for display purposes.
            force_hit_up_to_seq = seq if isinstance(seq, int) else 0
            hit_count = sum(
                1 for l in limits
                if l.get("status") == "hit"
                or l.get("hit_alert_sent")
                or (isinstance(l.get("sequence_number"), int) and l["sequence_number"] <= force_hit_up_to_seq)
            )

            suffix = "🎯🎯 **FINAL**" if seq == total else "🎯"
            ping = (
                f"{suffix} **{signal['instrument']}** {signal['direction'].upper()} — "
                f"limit #{seq} hit @ {_fmt(limit['price_level'])} "
                f"({hit_count}/{total} done)"
            )

            # Calculate distance to next pending limit for the embed
            distance_formatted = None
            pending_limits = [
                l for l in limits
                if l.get("status") != "hit"
                and not l.get("hit_alert_sent")
                and (not isinstance(l.get("sequence_number"), int) or l["sequence_number"] > force_hit_up_to_seq)
            ]
            if pending_limits and current_price is not None:
                nearest = min(pending_limits, key=lambda l: abs(current_price - l["price_level"]))
                distance = abs(current_price - nearest["price_level"])
                monitor = getattr(self.bot, "monitor", None) if self.bot else None
                alert_config = getattr(monitor, "alert_config", None) if monitor else None
                if alert_config:
                    distance_formatted = alert_config.format_distance_for_display(
                        signal["instrument"], distance, current_price
                    )
                else:
                    distance_formatted = f"{distance:.5f}".rstrip("0").rstrip(".")

            msg = await self._upsert_signal_message(
                signal=signal, limits=limits, event="hit",
                current_price=current_price,
                distance_formatted=distance_formatted,
                spread=spread, spread_buffer_enabled=spread_buffer_enabled,
                ping_text=ping,
                force_hit_up_to_seq=force_hit_up_to_seq,
            )
            if msg:
                self._register_live_embed(signal, "hit", spread_buffer_enabled)
                self.stats["hit_sent"] += 1
                self.stats["total_alerts"] += 1
                return True
        except Exception as e:
            logger.error(f"Failed to send limit hit alert: {e}", exc_info=True)
            self.stats["errors"] += 1
        return False

    async def send_stop_loss_alert(self, signal: Dict, current_price: float) -> bool:
        try:
            signal_id = signal["signal_id"]
            self._unregister_live_embed(signal_id)
            limits = await self._fetch_limits(signal)
            ping = (
                f"🛑 **{signal['instrument']}** {signal['direction'].upper()} — "
                f"stop loss hit @ {_fmt(current_price)} (SL: {_fmt(signal.get('stop_loss', 0))})"
            )
            msg = await self._upsert_signal_message(
                signal=signal, limits=limits, event="stop_loss",
                current_price=current_price,
                ping_text=ping,
                delete_after_minutes=self.END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._schedule_end_state_deletion(signal_id, event="stop_loss")
                self.stats["stop_loss_sent"] += 1
                self.stats["total_alerts"] += 1
                return True
        except Exception as e:
            logger.error(f"Failed to send stop loss alert: {e}", exc_info=True)
            self.stats["errors"] += 1
        return False

    async def send_auto_tp_alert(
        self, signal: Dict, hit_limits: list, last_pnl: float, tp_config,
        cumulative_pnl: Optional[float] = None,
        limit_pnl_map: Optional[Dict] = None,
    ) -> bool:
        """Edit the persistent embed to show auto take-profit. Also posts to profit channel.

        cumulative_pnl: total P&L across all hit limits (if provided, shown instead of last_pnl).
        limit_pnl_map:  sequence_number -> formatted pnl string for per-limit display.
        """
        instrument = signal["instrument"]
        direction = signal["direction"].upper()
        # Unregister from live updates — signal is now in a terminal state
        self._unregister_live_embed(signal["signal_id"])
        # Use cumulative P&L for display if available, otherwise fall back to last limit pnl
        display_pnl = cumulative_pnl if cumulative_pnl is not None else last_pnl
        pnl_display = tp_config.format_value(instrument, display_pnl)
        num_hit = len(hit_limits)

        # Build a set of hit limit IDs so the embed can correctly show struck-through limits
        hit_limit_ids = {lim.get("limit_id") or lim.get("id") for lim in hit_limits}
        hit_limit_ids.discard(None)

        # Fetch all limits (hit + pending) for the full limits display
        limits = await self._fetch_limits(signal)
        if not limits:
            limits = hit_limits

        total = len(limits) or num_hit

        ping = (
            f"💰 **{instrument}** {direction} — "
            f"Auto Take-Profit triggered! {num_hit}/{total} limits hit (+{pnl_display})"
        )

        try:
            msg = await self._upsert_signal_message(
                signal=signal, limits=limits, event="auto_tp", ping_text=ping,
                hit_limit_ids=hit_limit_ids, pnl_display=pnl_display,
                limit_pnl_map=limit_pnl_map,
                delete_after_minutes=self.END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._schedule_end_state_deletion(signal["signal_id"], event="auto_tp")
                self.stats["auto_tp_sent"] += 1
                self.stats["total_alerts"] += 1
        except Exception as e:
            logger.error(f"Failed to update embed for auto-TP signal {signal['signal_id']}: {e}")
            self.stats["errors"] += 1
            return False

        return True

    async def reactivate_embed(
        self,
        signal: Dict,
        ping_text: Optional[str] = None,
    ) -> bool:
        """
        After a signal is reactivated from cancelled state, rebuild its embed to reflect
        the correct live state (approaching or hit) with current price and distance.
        Re-registers the embed for live price updates.

        If the persistent embed was previously auto-deleted (end-state deletion), a new
        message is sent to the alert channel — the signal is effectively re-announced.
        """
        signal_id = signal.get("signal_id") or signal.get("id")
        if signal_id is None:
            return False

        # If there's a pending move-to-finished task for this signal, cancel it
        self._cancel_deletion_task(signal_id)

        # If the embed was already moved to the finished channel, remove it from there
        # so the signal re-appears only in the live alert channel.
        finished_msg = self.signal_finished_messages.pop(signal_id, None)
        if finished_msg:
            # Untrack it so reply commands no longer hit the finished-channel message
            self.alert_messages.pop(str(finished_msg.id), None)
            try:
                await finished_msg.delete()
                logger.info(f"Deleted finished-channel embed for signal {signal_id} on reactivation")
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"Could not delete finished-channel embed for signal {signal_id}: {e}")

        # Normalise signal dict so it always has signal_id
        signal = dict(signal)
        signal["signal_id"] = signal_id

        # Fetch fresh limits from DB
        limits = await self._fetch_limits(signal)

        # Determine event based on whether any limits were already hit
        hit_count = sum(1 for l in limits if l.get("status") == "hit" or l.get("hit_alert_sent"))
        event = "hit" if hit_count > 0 else "approaching"

        # Try to get current live price
        current_price = None
        distance_formatted = None
        spread = 0.0
        spread_buffer_enabled = False

        monitor = getattr(self.bot, "monitor", None) if self.bot else None
        stream_manager = getattr(monitor, "stream_manager", None) if monitor else None

        if stream_manager:
            try:
                price_data = await stream_manager.get_latest_price(signal["instrument"])
                if price_data:
                    direction = signal.get("direction", "long").lower()
                    current_price = price_data["ask"] if direction == "long" else price_data["bid"]
                    spread = price_data.get("spread", 0.0)

                    # Reload spread buffer setting
                    if hasattr(monitor, "_reload_spread_buffer_setting"):
                        monitor._reload_spread_buffer_setting()
                    spread_buffer_enabled = getattr(monitor, "spread_buffer_enabled", False)

                    # Calculate distance to nearest pending limit (for approaching state)
                    if event == "approaching" and limits:
                        pending = [
                            l for l in limits
                            if l.get("status") != "hit" and not l.get("hit_alert_sent")
                        ]
                        if pending:
                            nearest = min(pending, key=lambda l: abs(current_price - l["price_level"]))
                            distance = abs(current_price - nearest["price_level"])
                            alert_config = getattr(monitor, "alert_config", None)
                            if alert_config:
                                distance_formatted = alert_config.format_distance_for_display(
                                    signal["instrument"], distance, current_price
                                )
                            else:
                                distance_formatted = f"{distance:.5f}".rstrip("0").rstrip(".")
            except Exception as e:
                logger.warning(f"reactivate_embed: could not fetch live price for signal {signal_id}: {e}")

        # If the embed was previously auto-deleted, we need to drop the stale reference
        # so _upsert_signal_message will create a fresh one in the channel.
        if signal_id not in self.signal_messages:
            logger.info(
                f"reactivate_embed: no existing embed for signal {signal_id} "
                f"(likely auto-deleted) — will send fresh embed to channel"
            )
            # Fall through to _upsert_signal_message which will create a new message

        try:
            msg = await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event=event,
                current_price=current_price,
                distance_formatted=distance_formatted,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled,
                ping_text=ping_text,
            )
            if msg:
                # Re-register for live updates since the signal is active again
                self._register_live_embed(signal, event, spread_buffer_enabled)
                logger.info(f"Reactivated embed for signal {signal_id} as event='{event}'")
                return True
        except Exception as e:
            logger.error(f"reactivate_embed failed for signal {signal_id}: {e}", exc_info=True)
        return False

    async def update_signal_message(
        self,
        signal: Dict,
        event: str,
        limits: Optional[List[Dict]] = None,
        current_price: Optional[float] = None,
        ping_text: Optional[str] = None,
    ) -> bool:
        """
        Update the persistent embed after a manual command (profit, sl, cancel, etc.).
        If no persistent message exists yet, this is a no-op for most events.
        Exception: 'reactivated' always calls reactivate_embed (which creates a fresh
        embed if the old one was auto-deleted).
        """
        signal_id = signal["signal_id"]

        # Reactivation: always delegate to reactivate_embed regardless of embed state
        if event == "reactivated":
            return await self.reactivate_embed(signal=signal, ping_text=ping_text)

        if signal_id not in self.signal_messages:
            logger.debug(
                f"update_signal_message: signal {signal_id} has no persistent message yet — skipping"
            )
            return False

        # Terminal events: stop live price updates
        _TERMINAL_EVENTS = {"stop_loss", "auto_tp", "profit", "breakeven", "cancelled", "expired",
                            "spread_hour_cancelled", "near_miss_cancelled"}
        if event in _TERMINAL_EVENTS:
            self._unregister_live_embed(signal_id)

        # End states: schedule auto-deletion after 15 minutes
        is_end_state = event in self._END_STATES

        try:
            if limits is None:
                limits = await self._fetch_limits(signal)
            await self._upsert_signal_message(
                signal=signal, limits=limits, event=event,
                current_price=current_price, ping_text=ping_text,
                delete_after_minutes=self.END_STATE_DELETE_MINUTES if is_end_state else None,
            )
            if is_end_state:
                self._schedule_end_state_deletion(signal_id, event=event)
            return True
        except Exception as e:
            logger.error(f"Failed to update signal message for {signal_id}: {e}", exc_info=True)
            return False

    async def update_embed_for_signal_id(
        self,
        signal_id: int,
        event: str,
        ping_text: Optional[str] = None,
    ) -> bool:
        """
        Fetch the signal from the DB by ID and update its persistent embed.
        Safe to call from anywhere (commands, expiry, message handler, etc.).

        For 'reactivated' events this always runs (even if the embed was deleted),
        so a fresh embed is sent to the channel.  For all other events this is a
        no-op when no persistent embed exists yet.
        """
        # Always proceed for reactivation — the embed may have been auto-deleted
        if event != "reactivated" and signal_id not in self.signal_messages:
            logger.debug(f"update_embed_for_signal_id: signal {signal_id} has no embed yet — skipping")
            return False
        if not (self.bot and hasattr(self.bot, "signal_db") and self.bot.signal_db):
            logger.warning("update_embed_for_signal_id: bot/signal_db not available")
            return False
        try:
            signal = await self.bot.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.warning(f"update_embed_for_signal_id: signal {signal_id} not found in DB")
                return False
            # Normalise key: update_signal_message expects signal["signal_id"]
            if "signal_id" not in signal:
                signal = dict(signal)
                signal["signal_id"] = signal.get("id", signal_id)
            # Reactivation needs special handling to show live state + current price
            if event == "reactivated":
                return await self.reactivate_embed(signal=signal, ping_text=ping_text)
            return await self.update_signal_message(
                signal=signal,
                event=event,
                ping_text=ping_text,
            )
        except Exception as e:
            logger.error(f"update_embed_for_signal_id failed for {signal_id}: {e}", exc_info=True)
            return False

    # ── Spread hour / news cancel (standalone new messages) ──────────────────

    async def send_spread_hour_cancel_alert(self, signal: Dict, current_price: float) -> bool:
        """
        If an approaching alert embed already exists for this signal, update it to show
        the spread-hour cancellation and ping members.  If no embed exists yet (no
        approaching alert was sent), do nothing — the cancel is handled silently in the DB.
        """
        signal_id = signal.get("signal_id")
        if signal_id not in self.signal_messages:
            # No approaching alert was sent — silent backend cancel, no Discord message needed.
            # Still clean up gold-toll original message if applicable.
            logger.debug(f"Spread hour cancel for signal {signal_id}: no persistent embed, skipping alert")
            await self._maybe_delete_toll_original(signal, signal_id)
            return True
        try:
            await self.update_signal_message(
                signal=signal,
                event="spread_hour_cancelled",
                current_price=current_price,
                ping_text="Signal cancelled — spread hour.",
            )
            self.stats["spread_hour_cancelled"] += 1
            self.stats["total_alerts"] += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send spread hour cancel alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_news_cancel_alert(self, signal: Dict, current_price: float, news_event) -> bool:
        """
        If an approaching/hit embed already exists for this signal, update it to show
        the news cancellation.  If no embed exists, send a standalone message.
        Either way, schedule auto-deletion after END_STATE_DELETE_MINUTES minutes.
        """
        signal_id = signal.get("signal_id")
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            return False

        instrument = signal.get("instrument", "?")
        direction = signal.get("direction", "").upper()

        # If there's a persistent embed, edit it (keeps the channel clean)
        if signal_id in self.signal_messages:
            try:
                await self.update_signal_message(
                    signal=signal,
                    event="cancelled",
                    current_price=current_price,
                    ping_text=f"📰 **{instrument}** {direction} — cancelled (news: {news_event.category.upper()})",
                )
                self.stats["news_cancelled"] = self.stats.get("news_cancelled", 0) + 1
                self.stats["total_alerts"] += 1
                # Deletion is scheduled inside update_signal_message for _END_STATES
                return True
            except Exception as e:
                logger.error(f"Failed to update embed for news cancel (signal {signal_id}): {e}")
                self.stats["errors"] += 1
                return False

        # No persistent embed — send a standalone message and schedule its deletion
        try:
            news_ts = int(news_event.news_time.timestamp())
            all_limits = signal.get("limits", signal.get("pending_limits", []))
            if all_limits:
                limit_prices = "  |  ".join(
                    _fmt(l["price_level"] if isinstance(l, dict) else l)
                    for l in sorted(all_limits, key=lambda x: x["sequence_number"] if isinstance(x, dict) else 0)
                )
            else:
                limit_prices = "—"
            signal_summary = (
                f"**{instrument}** {direction}\n"
                f"Limits: {limit_prices}\n"
                f"SL: {_fmt(signal.get('stop_loss', 0))}"
            )
            embed = discord.Embed(
                title="📰 Signal Cancelled — News",
                description=(
                    f"The following signal was cancelled due to news "
                    f"({news_event.category.upper()} @ "
                    f"<t:{news_ts}:t>):\n\n"
                    f"{signal_summary}"
                ),
                color=0x5865F2,
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(
                text=(
                    f"Signal #{signal_id} • Auto-cancelled (news mode) "
                    f"• 🗑️ Deletes in {self.END_STATE_DELETE_MINUTES} min"
                )
            )
            if signal.get("message_id") and signal.get("channel_id"):
                if not str(signal["message_id"]).startswith("manual_"):
                    guild_id = signal.get("guild_id")
                    if not guild_id and self.bot and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id
                    url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    embed.add_field(name="Source", value=url, inline=False)
            await target_channel.send("<@&1334203997107650662>")
            message = await target_channel.send(embed=embed)
            self.track_alert_message(message.id, signal_id)
            self.stats["news_cancelled"] = self.stats.get("news_cancelled", 0) + 1
            self.stats["total_alerts"] += 1

            # Schedule move to finished-signals channel (and gold-toll source cleanup)
            # after END_STATE_DELETE_MINUTES minutes, mirroring the embed path.
            async def _move_standalone_after_delay():
                try:
                    await asyncio.sleep(self.END_STATE_DELETE_MINUTES * 60)
                except asyncio.CancelledError:
                    return

                # Move to finished-signals channel
                finished_channel = self._get_finished_channel()
                if finished_channel:
                    try:
                        # Rebuild embed with archive footer
                        archived_embed = embed.copy()
                        old_footer = embed.footer.text or ""
                        clean_footer = old_footer.split(" • ⏳")[0].split(" • 🗑️")[0]
                        archived_embed.set_footer(text=f"{clean_footer} • 📁 Archived")
                        await finished_channel.send(embed=archived_embed)
                        logger.info(f"Moved standalone news-cancel embed for signal {signal_id} to finished-signals")
                    except Exception as _mv:
                        logger.warning(f"Could not move standalone news-cancel embed for signal {signal_id}: {_mv}")

                # Delete the original alert channel message
                try:
                    await message.delete()
                    logger.info(f"Deleted standalone news-cancel message for signal {signal_id}")
                except Exception:
                    pass

                # Gold-toll: delete the original signal source message
                await self._maybe_delete_toll_original(signal, signal_id)

            asyncio.ensure_future(_move_standalone_after_delay())
            return True
        except Exception as e:
            logger.error(f"Failed to send news cancel alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_near_miss_cancel_alert(self, signal: Dict, nm_state=None) -> bool:
        """
        Update the persistent embed to show near-miss cancellation and send a role ping.

        This mirrors send_auto_tp_alert in structure: edits the existing embed
        (so the channel history stays clean) and sends a fresh role ping.
        """
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]
        direction = signal["direction"].upper()

        # Build description of the near-miss
        closest_str = "N/A"
        bounce_str = "N/A"
        if nm_state is not None:
            try:
                # Import here to avoid circular imports
                from price_feeds.nm_config import NMConfig
                _cfg = NMConfig()
                closest_str = _cfg.format_value(instrument, nm_state.closest_distance)
                required_bounce = _cfg.get_required_bounce(instrument, nm_state.closest_distance)
                bounce_str = _cfg.format_value(instrument, required_bounce)
            except Exception:
                pass

        # Unregister from live updates — signal is now in a terminal state
        self._unregister_live_embed(signal_id)

        ping = (
            f"❌ **{instrument}** {direction} — "
            f"Near-Miss detected! Signal auto-cancelled "
            f"(approached {closest_str} from limit, bounced {bounce_str})"
        )

        # Fetch all limits for the embed
        limits = await self._fetch_limits(signal)

        try:
            msg = await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event="near_miss_cancelled",
                ping_text=ping,
                delete_after_minutes=self.END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._schedule_end_state_deletion(signal_id, event="near_miss_cancelled")
                self.stats["nm_cancelled"] = self.stats.get("nm_cancelled", 0) + 1
                self.stats["total_alerts"] += 1
                logger.info(f"Near-miss cancel alert sent for signal {signal_id} ({instrument})")
                return True
        except Exception as e:
            logger.error(f"Failed to send near-miss cancel alert for signal {signal_id}: {e}")
            self.stats["errors"] += 1

        return False

    async def send_news_activated_alert(self, news_event) -> bool:
        """Send news-mode activated embed to ALL alert channels. Returns list of sent messages."""
        # Collect all distinct alert channels
        channels = []
        seen_ids = set()
        for ch in [
            self.alert_channel,
            self.pa_alert_channel,
            self.toll_alert_channel,
            self.general_toll_alert_channel,
        ]:
            if ch is not None and ch.id not in seen_ids:
                channels.append(ch)
                seen_ids.add(ch.id)

        if not channels:
            return False

        # Build time string using Discord timestamps (auto-localised per viewer)
        start_ts = int(news_event.start_time.timestamp())
        if news_event.is_now_mode:
            if news_event.end_time_override is not None:
                end_ts = int(news_event.end_time_override.timestamp())
                time_str = f"**<t:{start_ts}:t> → <t:{end_ts}:t>**"
            else:
                time_str = f"**Active from <t:{start_ts}:t>**"
        else:
            end_ts = int(news_event.end_time.timestamp())
            time_str = f"**<t:{start_ts}:t> → <t:{end_ts}:t>**"

        embed = discord.Embed(
            title="📰 News Mode Active",
            description=(
                f"News window activated for **{news_event.category.upper()}**\n"
                f"{time_str}"
            ),
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Event #{news_event.event_id} • Signals will be auto-cancelled if hit")

        sent_messages = []
        try:
            for ch in channels:
                msg = await ch.send(embed=embed)
                sent_messages.append(msg)
            self.stats["total_alerts"] += 1

            # Store messages so we can update them when the window ends
            if not hasattr(self, '_news_activation_messages'):
                self._news_activation_messages = {}
            self._news_activation_messages[news_event.event_id] = sent_messages

            return True
        except Exception as e:
            logger.error(f"Failed to send news activated alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_news_ended_alert(self, news_event) -> None:
        """
        Edit all activation embeds for this event to show 'News Mode Ended',
        then schedule deletion after 5 minutes.
        """
        messages = []
        if hasattr(self, '_news_activation_messages'):
            messages = self._news_activation_messages.pop(news_event.event_id, [])

        end_ts = int(datetime.now(timezone.utc).timestamp())
        embed = discord.Embed(
            title="📰 News Mode Ended",
            description=(
                f"News window for **{news_event.category.upper()}** has ended.\n"
                f"**Ended at <t:{end_ts}:t>**"
            ),
            color=0x808080,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Event #{news_event.event_id} • This message will be deleted in 5 minutes")

        for msg in messages:
            try:
                await msg.edit(embed=embed)
            except Exception as e:
                logger.warning(f"Could not edit news activation message {msg.id}: {e}")

        # Auto-delete after 5 minutes
        if messages:
            async def _delete_later():
                await asyncio.sleep(300)
                for msg in messages:
                    try:
                        await msg.delete()
                    except Exception:
                        pass
            asyncio.ensure_future(_delete_later())

    async def _get_profit_channel(self) -> Optional[discord.TextChannel]:
        try:
            from pathlib import Path
            import json
            config_path = Path(__file__).resolve().parent.parent / "config" / "channels.json"
            with open(config_path) as f:
                cfg = json.load(f)
            profit_channel_id = cfg.get("profit_channel")
            if not profit_channel_id:
                return None
            channel = self.bot.get_channel(int(profit_channel_id))
            if not channel:
                channel = await self.bot.fetch_channel(int(profit_channel_id))
            return channel
        except Exception as e:
            logger.error(f"Could not load profit channel: {e}")
            return None

    def get_stats(self) -> Dict:
        return {
            "alerts": {
                "approaching": self.stats["approaching_sent"],
                "hit": self.stats["hit_sent"],
                "stop_loss": self.stats["stop_loss_sent"],
                "auto_tp": self.stats["auto_tp_sent"],
                "total": self.stats["total_alerts"],
            },
            "errors": self.stats["errors"],
            "channel_configured": self.alert_channel is not None,
            "tracked_messages": len(self.alert_messages),
            "persistent_messages": len(self.signal_messages),
            "active_pings": len(self.signal_ping_messages),
            "live_embeds": len(self._live_embeds),
            "pending_archive_moves": len(self._deletion_tasks),
            "finished_channel_messages": len(self.signal_finished_messages),
        }