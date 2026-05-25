"""
Alert System - Handles all alert generation and sending for the price monitor.
One persistent embed per signal; all events edit it in-place.
Separate short ping messages are sent for each event so role pings still fire.
"""

import asyncio
import collections
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import discord

from price_feeds.archive_manager import (
    END_STATE_DELETE_MINUTES,
    ArchiveManager,
    is_end_state,
)
from price_feeds.embed_builders import _build_signal_embed, _fmt
from price_feeds.nm_config import NMConfig
from utils.logger import get_logger

logger = get_logger("alert_system")


class AlertSystem:
    """
    Handles all alert generation and sending for trading signals.

    One persistent Discord message (embed) is created per signal when the first
    approaching / hit alert fires.  All subsequent events (more limits hit,
    stop loss, auto-TP, manual overrides) EDIT that same message instead of
    posting new ones.

    Pinging: editing a message does NOT ping anyone, so a short plain-text ping
    message is sent as a REPLY to the persistent embed for each event.  Previous
    pings are deleted before sending a new one.

    Live Price Updates: active approaching/hit embeds are refreshed every
    LIVE_UPDATE_INTERVAL seconds with the latest price and distance.
    """

    LIVE_UPDATE_INTERVAL = 15

    def __init__(
        self,
        alert_channel: Optional[discord.TextChannel] = None,
        bot=None,
        stream_manager=None,
        alert_config=None,
    ):
        self.alert_channel = alert_channel
        self.pa_alert_channel = None
        self.toll_alert_channel = None
        self.general_toll_alert_channel = None
        self.legends_alert_channel = None
        self.bot = bot
        self.stream_manager = stream_manager
        self.alert_config = alert_config
        self._load_channels_config()

        # signal_id -> discord.Message  (the one persistent embed per signal)
        self.signal_messages: Dict[int, discord.Message] = {}

        # signal_id -> discord.Message  (the most recent ping message per signal)
        self.signal_ping_messages: Dict[int, discord.Message] = {}

        # signal_id -> discord.Message  (embed in finished-signals channel after move)
        self.signal_finished_messages: Dict[int, discord.Message] = {}

        # alert message ID (str) -> signal_id; bounded at 1000 entries
        self.alert_messages: collections.OrderedDict = collections.OrderedDict()

        # signal_id -> {"signal": dict, "event": str, "spread_buffer_enabled": bool}
        self._live_embeds: Dict[int, Dict] = {}

        self._live_update_task: Optional[asyncio.Task] = None
        self._news_activation_messages: Dict[int, list] = {}

        self._archive_manager = ArchiveManager(
            bot=bot,
            signal_messages=self.signal_messages,
            signal_ping_messages=self.signal_ping_messages,
            signal_finished_messages=self.signal_finished_messages,
            alert_messages=self.alert_messages,
            toll_channel_ids=self.toll_channel_ids,
            role_mention=self.role_mention,
            track_alert_message_fn=self.track_alert_message,
            finished_channel_id=self._finished_channel_id,
            profit_channel_id=self._profit_channel_id,
        )

        self.stats = {
            "approaching_sent": 0,
            "hit_sent": 0,
            "stop_loss_sent": 0,
            "auto_tp_sent": 0,
            "spread_hour_cancelled": 0,
            "news_cancelled": 0,
            "nm_cancelled": 0,
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
        self._archive_manager.cancel_all()

    async def _live_update_loop(self):
        await asyncio.sleep(5)
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
        if not self.stream_manager:
            return

        stream_manager = self.stream_manager

        signal_ids = list(self._live_embeds.keys())
        logger.debug(f"Refreshing {len(signal_ids)} live embed(s)")

        for i, signal_id in enumerate(signal_ids):
            if i > 0:
                await asyncio.sleep(1)

            entry = self._live_embeds.get(signal_id)
            if not entry:
                continue

            signal = entry["signal"]
            event = entry["event"]
            spread_buffer_enabled = entry.get("spread_buffer_enabled", False)

            try:
                instrument = signal["instrument"]
                price_data = await stream_manager.get_latest_price(instrument)
                if not price_data:
                    continue

                direction = signal.get("direction", "long").lower()
                current_price = price_data["ask"] if direction == "long" else price_data["bid"]
                spread = price_data.get("spread", 0.0)

                limits = await self._fetch_limits(signal)

                distance_formatted = None
                if event in ("approaching", "hit"):
                    pending_limits = [
                        l
                        for l in limits
                        if l.get("status") != "hit" and not l.get("hit_alert_sent")
                    ]
                    if pending_limits:
                        nearest = min(
                            pending_limits, key=lambda l: abs(current_price - l["price_level"])
                        )
                        distance = abs(current_price - nearest["price_level"])
                        if self.alert_config:
                            distance_formatted = self.alert_config.format_distance_for_display(
                                instrument, distance, current_price
                            )

                if signal_id not in self._live_embeds:
                    logger.debug(
                        f"Live update: signal {signal_id} was unregistered mid-cycle, skipping"
                    )
                    continue

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
                        logger.warning(
                            f"Live update rate-limited for signal {signal_id}, skipping this cycle"
                        )
                    else:
                        logger.warning(f"Live update HTTP error for signal {signal_id}: {e}")

            except Exception as e:
                logger.error(f"Live update failed for signal {signal_id}: {e}", exc_info=True)

    def _register_live_embed(self, signal: Dict, event: str, spread_buffer_enabled: bool = False):
        signal_id = signal["signal_id"]
        self._live_embeds[signal_id] = {
            "signal": signal,
            "event": event,
            "spread_buffer_enabled": spread_buffer_enabled,
        }

    def _unregister_live_embed(self, signal_id: int):
        self._live_embeds.pop(signal_id, None)

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

    def set_legends_channel(self, channel: discord.TextChannel):
        self.legends_alert_channel = channel
        logger.info(f"Legends alert channel set: #{channel.name} ({channel.id})")

    def _load_channels_config(self):
        """Load channels.json once and cache all derived channel ID sets and role mention."""
        config_path = Path(__file__).parent.parent / "config" / "channels.json"
        try:
            with open(config_path) as f:
                cfg = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load channels.json: {e}")
            cfg = {}

        monitored = cfg.get("monitored_channels", {})

        self.pa_channel_ids = {
            str(v) for k, v in monitored.items() if "pa" in k.lower() or "price-action" in k.lower()
        }

        self.toll_channel_ids = set()
        self.general_toll_channel_ids = set()
        self.oil_toll_channel_ids = set()
        self.legends_channel_ids = set()
        for channel_name, channel_id in monitored.items():
            if not channel_id:
                continue
            name_lower = channel_name.lower()
            if name_lower == "general-tolls":
                self.general_toll_channel_ids.add(str(channel_id))
            elif name_lower == "oil-tolls":
                self.oil_toll_channel_ids.add(str(channel_id))
                self.toll_channel_ids.add(str(channel_id))
            elif name_lower == "legends-trades":
                self.legends_channel_ids.add(str(channel_id))
            elif "toll" in name_lower:
                self.toll_channel_ids.add(str(channel_id))

        self._finished_channel_id = cfg.get("finished_signals")
        self._profit_channel_id = cfg.get("profit_channel")

        role_id = cfg.get("alert_role_id", "1334203997107650662")
        self.role_mention = f"<@&{role_id}>"

        logger.info(
            f"Channels loaded: {len(self.pa_channel_ids)} PA, "
            f"{len(self.toll_channel_ids)} toll "
            f"(incl. {len(self.oil_toll_channel_ids)} oil-toll), "
            f"{len(self.general_toll_channel_ids)} general-toll, "
            f"{len(self.legends_channel_ids)} legends"
        )

    def reload_channels(self):
        """Re-read channels.json and refresh all cached channel IDs. Call on !reload."""
        self._load_channels_config()

    def is_pa_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.pa_channel_ids

    def is_toll_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.toll_channel_ids

    def is_general_toll_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.general_toll_channel_ids

    def is_oil_toll_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.oil_toll_channel_ids

    def is_legends_signal(self, signal: Dict) -> bool:
        return str(signal.get("channel_id", "")) in self.legends_channel_ids

    def _get_alert_channel(self, signal: Dict) -> Optional[discord.TextChannel]:
        if self.is_general_toll_signal(signal) or self.is_oil_toll_signal(signal):
            if self.general_toll_alert_channel:
                return self.general_toll_alert_channel
            logger.warning(
                "General/oil-toll signal but no general-toll alert channel; falling back"
            )
        if self.is_toll_signal(signal):
            if self.toll_alert_channel:
                return self.toll_alert_channel
            logger.warning("Toll signal but no toll alert channel; falling back")
        if self.is_pa_signal(signal):
            if self.pa_alert_channel:
                return self.pa_alert_channel
            logger.warning("PA signal but no PA alert channel; falling back")
        if self.is_legends_signal(signal):
            if self.legends_alert_channel:
                return self.legends_alert_channel
            logger.warning("Legends signal but no legends alert channel; falling back")
        return self.alert_channel

    def _get_finished_channel(self):
        return self._archive_manager._get_finished_channel()

    async def _maybe_delete_toll_original(self, signal: Dict, signal_id: int) -> None:
        await self._archive_manager.maybe_delete_toll_original(signal, signal_id)

    # ── Tracking ─────────────────────────────────────────────────────────────

    def track_alert_message(self, message_id: int, signal_id: int):
        """Register a message_id -> signal_id mapping (used by reply handler)."""
        self.alert_messages[str(message_id)] = signal_id
        while len(self.alert_messages) > 1000:
            self.alert_messages.popitem(last=False)

    def get_signal_from_alert(self, message_id: str) -> Optional[int]:
        return self.alert_messages.get(str(message_id))

    # ── Limit fetcher ────────────────────────────────────────────────────────

    async def _fetch_limits(self, signal: Dict) -> List[Dict]:
        """
        Get ALL limits for a signal (hit + pending) from the DB.
        Falls back to signal dict only if the DB call fails.
        """
        if self.bot and self.bot.signal_db:
            try:
                full = await self.bot.signal_db.get_signal_with_limits(signal["signal_id"])
                if full:
                    return full.get("limits", [])
            except Exception as e:
                logger.warning(
                    f"Could not fetch limits from DB for signal {signal['signal_id']}: {e}"
                )
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
                embed_msg = await target_channel.send(content=self.role_mention, embed=embed)
                self.signal_messages[signal_id] = embed_msg
                self.track_alert_message(embed_msg.id, signal_id)
                logger.info(f"Created persistent message for signal {signal_id} (event={event})")
            except Exception as e:
                logger.error(f"Failed to send new persistent message for signal {signal_id}: {e}")
                return None

        if ping_text and embed_msg:
            old_ping = self.signal_ping_messages.get(signal_id)
            if old_ping:
                try:
                    await old_ping.delete()
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.warning(f"Could not delete old ping for signal {signal_id}: {e}")

            try:
                new_ping = await embed_msg.reply(f"{self.role_mention} {ping_text}")
                self.signal_ping_messages[signal_id] = new_ping
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
            logger.debug(
                f"Skipping approaching alert for limit #{limit['sequence_number']} (not first)"
            )
            return False
        try:
            limits = await self._fetch_limits(signal)
            if not limits:
                limits = [limit]
            msg = await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event="approaching",
                current_price=current_price,
                distance_formatted=distance_formatted,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled,
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

            force_hit_up_to_seq = seq if isinstance(seq, int) else 0
            hit_count = sum(
                1
                for l in limits
                if l.get("status") == "hit"
                or l.get("hit_alert_sent")
                or (
                    isinstance(l.get("sequence_number"), int)
                    and l["sequence_number"] <= force_hit_up_to_seq
                )
            )

            suffix = "🎯🎯 **FINAL**" if seq == total else "🎯"
            ping = (
                f"{suffix} **{signal['instrument']}** {signal['direction'].upper()} — "
                f"limit #{seq} hit @ {_fmt(limit['price_level'])} "
                f"({hit_count}/{total} done)"
            )

            distance_formatted = None
            pending_limits = [
                l
                for l in limits
                if l.get("status") != "hit"
                and not l.get("hit_alert_sent")
                and (
                    not isinstance(l.get("sequence_number"), int)
                    or l["sequence_number"] > force_hit_up_to_seq
                )
            ]
            if pending_limits and current_price is not None:
                nearest = min(pending_limits, key=lambda l: abs(current_price - l["price_level"]))
                distance = abs(current_price - nearest["price_level"])
                if self.alert_config:
                    distance_formatted = self.alert_config.format_distance_for_display(
                        signal["instrument"], distance, current_price
                    )
                else:
                    distance_formatted = f"{distance:.5f}".rstrip("0").rstrip(".")

            msg = await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event="hit",
                current_price=current_price,
                distance_formatted=distance_formatted,
                spread=spread,
                spread_buffer_enabled=spread_buffer_enabled,
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
                signal=signal,
                limits=limits,
                event="stop_loss",
                current_price=current_price,
                ping_text=ping,
                delete_after_minutes=END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._archive_manager.schedule_end_state_move(signal_id, event="stop_loss")
                self.stats["stop_loss_sent"] += 1
                self.stats["total_alerts"] += 1
                return True
        except Exception as e:
            logger.error(f"Failed to send stop loss alert: {e}", exc_info=True)
            self.stats["errors"] += 1
        return False

    async def send_auto_tp_alert(
        self,
        signal: Dict,
        hit_limits: list,
        last_pnl: float,
        tp_config,
        cumulative_pnl: Optional[float] = None,
        limit_pnl_map: Optional[Dict] = None,
    ) -> bool:
        """Edit the persistent embed to show auto take-profit. Also posts to profit channel."""
        instrument = signal["instrument"]
        direction = signal["direction"].upper()
        self._unregister_live_embed(signal["signal_id"])
        display_pnl = cumulative_pnl if cumulative_pnl is not None else last_pnl
        pnl_display = tp_config.format_value(instrument, display_pnl)
        num_hit = len(hit_limits)

        hit_limit_ids = {lim.get("limit_id") or lim.get("id") for lim in hit_limits}
        hit_limit_ids.discard(None)

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
                signal=signal,
                limits=limits,
                event="auto_tp",
                ping_text=ping,
                hit_limit_ids=hit_limit_ids,
                pnl_display=pnl_display,
                limit_pnl_map=limit_pnl_map,
                delete_after_minutes=END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._archive_manager.schedule_end_state_move(signal["signal_id"], event="auto_tp")
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
        After a signal is reactivated from cancelled state, rebuild its embed.
        Re-registers the embed for live price updates.
        """
        signal_id = signal["signal_id"]
        if signal_id is None:
            return False

        self._archive_manager.cancel_pending_move(signal_id)

        finished_msg = self.signal_finished_messages.pop(signal_id, None)
        if finished_msg:
            self.alert_messages.pop(str(finished_msg.id), None)
            try:
                await finished_msg.delete()
                logger.info(
                    f"Deleted finished-channel embed for signal {signal_id} on reactivation"
                )
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(
                    f"Could not delete finished-channel embed for signal {signal_id}: {e}"
                )

        limits = await self._fetch_limits(signal)

        hit_count = sum(1 for l in limits if l.get("status") == "hit" or l.get("hit_alert_sent"))
        event = "hit" if hit_count > 0 else "approaching"

        current_price = None
        distance_formatted = None
        spread = 0.0
        spread_buffer_enabled = False

        if self.stream_manager:
            try:
                price_data = await self.stream_manager.get_latest_price(signal["instrument"])
                if price_data:
                    direction = signal.get("direction", "long").lower()
                    current_price = price_data["ask"] if direction == "long" else price_data["bid"]
                    spread = price_data.get("spread", 0.0)

                    monitor = getattr(self.bot, "monitor", None) if self.bot else None
                    _sbe = getattr(monitor, "_spread_buffer_enabled", None) if monitor else None
                    spread_buffer_enabled = _sbe if _sbe is not None else True

                    if event == "approaching" and limits:
                        pending = [
                            l
                            for l in limits
                            if l.get("status") != "hit" and not l.get("hit_alert_sent")
                        ]
                        if pending:
                            nearest = min(
                                pending, key=lambda l: abs(current_price - l["price_level"])
                            )
                            distance = abs(current_price - nearest["price_level"])
                            if self.alert_config:
                                distance_formatted = self.alert_config.format_distance_for_display(
                                    signal["instrument"], distance, current_price
                                )
                            else:
                                distance_formatted = f"{distance:.5f}".rstrip("0").rstrip(".")
            except Exception as e:
                logger.warning(
                    f"reactivate_embed: could not fetch live price for signal {signal_id}: {e}"
                )

        if signal_id not in self.signal_messages:
            logger.info(
                f"reactivate_embed: no existing embed for signal {signal_id} "
                f"(likely auto-deleted) — will send fresh embed to channel"
            )

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
        """
        signal_id = signal["signal_id"]

        if event == "reactivated":
            return await self.reactivate_embed(signal=signal, ping_text=ping_text)

        if signal_id not in self.signal_messages:
            logger.debug(
                f"update_signal_message: signal {signal_id} has no persistent message yet — skipping"
            )
            return False

        _TERMINAL_EVENTS = {
            "stop_loss",
            "auto_tp",
            "profit",
            "breakeven",
            "cancelled",
            "expired",
            "spread_hour_cancelled",
            "near_miss_cancelled",
        }
        if event in _TERMINAL_EVENTS:
            self._unregister_live_embed(signal_id)

        end_state = is_end_state(event)

        try:
            if limits is None:
                limits = await self._fetch_limits(signal)
            await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event=event,
                current_price=current_price,
                ping_text=ping_text,
                delete_after_minutes=END_STATE_DELETE_MINUTES if end_state else None,
            )
            if end_state:
                self._archive_manager.schedule_end_state_move(signal_id, event=event)
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
        Safe to call from anywhere.
        """
        if event != "reactivated" and signal_id not in self.signal_messages:
            logger.debug(
                f"update_embed_for_signal_id: signal {signal_id} has no embed yet — skipping"
            )
            return False
        if not (self.bot and self.bot.signal_db):
            logger.warning("update_embed_for_signal_id: bot/signal_db not available")
            return False
        try:
            signal = await self.bot.signal_db.get_signal_with_limits(signal_id)
            if not signal:
                logger.warning(f"update_embed_for_signal_id: signal {signal_id} not found in DB")
                return False
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

    # ── Spread hour / news cancel ────────────────────────────────────────────

    async def send_spread_hour_cancel_alert(self, signal: Dict, current_price: float) -> bool:
        signal_id = signal.get("signal_id")
        if signal_id not in self.signal_messages:
            logger.debug(
                f"Spread hour cancel for signal {signal_id}: no persistent embed, skipping alert"
            )
            await self._archive_manager.maybe_delete_toll_original(signal, signal_id)
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
        signal_id = signal.get("signal_id")
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            return False

        instrument = signal.get("instrument", "?")
        direction = signal.get("direction", "").upper()

        if signal_id in self.signal_messages:
            try:
                await self.update_signal_message(
                    signal=signal,
                    event="cancelled",
                    current_price=current_price,
                    ping_text=f"📰 **{instrument}** {direction} — cancelled (news: {news_event.category.upper()})",
                )
                self.stats["news_cancelled"] += 1
                self.stats["total_alerts"] += 1
                return True
            except Exception as e:
                logger.error(f"Failed to update embed for news cancel (signal {signal_id}): {e}")
                self.stats["errors"] += 1
                return False

        try:
            news_ts = int(news_event.news_time.timestamp())
            all_limits = signal.get("limits", signal.get("pending_limits", []))
            if all_limits:
                limit_prices = "  |  ".join(
                    _fmt(l["price_level"] if isinstance(l, dict) else l)
                    for l in sorted(
                        all_limits, key=lambda x: x["sequence_number"] if isinstance(x, dict) else 0
                    )
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
                    f"• 🗑️ Deletes in {END_STATE_DELETE_MINUTES} min"
                )
            )
            if signal.get("message_id") and signal.get("channel_id"):
                if not str(signal["message_id"]).startswith("manual_"):
                    guild_id = signal.get("guild_id")
                    if not guild_id and self.bot and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id
                    url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                    embed.add_field(name="Source", value=url, inline=False)
            await target_channel.send(self.role_mention)
            message = await target_channel.send(embed=embed)
            self.track_alert_message(message.id, signal_id)
            self.stats["news_cancelled"] += 1
            self.stats["total_alerts"] += 1

            asyncio.ensure_future(
                self._archive_manager.move_standalone_after_delay(signal, signal_id, message, embed)
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send news cancel alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_near_miss_cancel_alert(self, signal: Dict, nm_state=None) -> bool:
        signal_id = signal["signal_id"]
        instrument = signal["instrument"]
        direction = signal["direction"].upper()

        closest_str = "N/A"
        bounce_str = "N/A"
        if nm_state is not None:
            try:
                _cfg = NMConfig()
                closest_str = _cfg.format_value(instrument, nm_state.closest_distance)
                required_bounce = _cfg.get_required_bounce(instrument, nm_state.closest_distance)
                bounce_str = _cfg.format_value(instrument, required_bounce)
            except Exception:
                pass

        self._unregister_live_embed(signal_id)

        ping = (
            f"❌ **{instrument}** {direction} — "
            f"Near-Miss detected! Signal auto-cancelled "
            f"(approached {closest_str} from limit, bounced {bounce_str})"
        )

        limits = await self._fetch_limits(signal)

        try:
            msg = await self._upsert_signal_message(
                signal=signal,
                limits=limits,
                event="near_miss_cancelled",
                ping_text=ping,
                delete_after_minutes=END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._archive_manager.schedule_end_state_move(
                    signal_id, event="near_miss_cancelled"
                )
                self.stats["nm_cancelled"] += 1
                self.stats["total_alerts"] += 1
                logger.info(f"Near-miss cancel alert sent for signal {signal_id} ({instrument})")
                return True
        except Exception as e:
            logger.error(f"Failed to send near-miss cancel alert for signal {signal_id}: {e}")
            self.stats["errors"] += 1

        return False

    async def send_news_activated_alert(self, news_event) -> bool:
        """Send news-mode activated embed to ALL alert channels."""
        channels = []
        seen_ids = set()
        for ch in [
            self.alert_channel,
            self.pa_alert_channel,
            self.toll_alert_channel,
            self.general_toll_alert_channel,
            self.legends_alert_channel,
        ]:
            if ch is not None and ch.id not in seen_ids:
                channels.append(ch)
                seen_ids.add(ch.id)

        if not channels:
            return False

        start_ts = int(news_event.start_time.timestamp())
        if news_event.is_now_mode:
            if news_event.end_time_override is not None:
                end_ts = int(news_event.end_time_override.timestamp())
                time_str = f"**<t:{start_ts}:t> -> <t:{end_ts}:t>**"
            else:
                time_str = f"**Active from <t:{start_ts}:t>**"
        else:
            end_ts = int(news_event.end_time.timestamp())
            time_str = f"**<t:{start_ts}:t> -> <t:{end_ts}:t>**"

        embed = discord.Embed(
            title="📰 News Mode Active",
            description=(
                f"News window activated for **{news_event.category.upper()}**\n{time_str}"
            ),
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(
            text=f"Event #{news_event.event_id} • Signals will be auto-cancelled if hit"
        )

        sent_messages = []
        try:
            for ch in channels:
                msg = await ch.send(embed=embed)
                sent_messages.append(msg)
            self.stats["total_alerts"] += 1

            self._news_activation_messages[news_event.event_id] = sent_messages

            return True
        except Exception as e:
            logger.error(f"Failed to send news activated alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_news_ended_alert(self, news_event) -> None:
        """Edit all activation embeds for this event to show 'News Mode Ended'."""
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
        embed.set_footer(
            text=f"Event #{news_event.event_id} • This message will be deleted in 5 minutes"
        )

        for msg in messages:
            try:
                await msg.edit(embed=embed)
            except Exception as e:
                logger.warning(f"Could not edit news activation message {msg.id}: {e}")

        if messages:

            async def _delete_later():
                await asyncio.sleep(300)
                for msg in messages:
                    try:
                        await msg.delete()
                    except Exception:
                        pass

            asyncio.ensure_future(_delete_later())

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
            "pending_archive_moves": len(self._archive_manager._deletion_tasks),
            "finished_channel_messages": len(self.signal_finished_messages),
        }
