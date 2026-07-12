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

    # Max concurrent embed edits during a live-refresh pass. Editing different
    # messages uses separate rate-limit buckets, so a bounded fan-out keeps a
    # full pass to ~1-2s regardless of how many embeds are active, instead of
    # the old 1s-per-embed serial stagger that pushed the effective refresh
    # period past 60s once a dozen signals were live.
    _LIVE_REFRESH_CONCURRENCY = 5

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
        self.risky_alert_channel = None
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

        # signal_id -> asyncio.Lock. Serializes message mutations for a single
        # signal so an event edit (hit/SL/TP) and a live-refresh edit on the
        # same embed never overlap or land out of order. Pruned in the refresh
        # loop once a signal's embed is gone.
        self._message_locks: Dict[int, asyncio.Lock] = {}

        # Count of status/event edits currently in flight. While > 0 the live
        # refresh stands down so a hit/SL/TP edit lands without competing for
        # the rate-limit budget. Incremented around every _upsert_signal_message.
        self._priority_edits_active: int = 0

        # signal_id -> signature of the last live-rendered embed, so the refresh
        # loop can skip edits that would produce an identical embed.
        self._last_live_render: Dict[int, str] = {}

        self._live_update_task: Optional[asyncio.Task] = None
        self._news_activation_messages: Dict[int, list] = {}

        self._archive_manager = ArchiveManager(
            bot=bot,
            signal_messages=self.signal_messages,
            signal_ping_messages=self.signal_ping_messages,
            signal_finished_messages=self.signal_finished_messages,
            alert_messages=self.alert_messages,
            auto_purge_channel_ids=self.auto_purge_channel_ids,
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
            "late_market_cancelled": 0,
            "risky_window_cancelled": 0,
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
        """Refresh every live embed with the latest price, fanned out concurrently.

        Edits run under a bounded semaphore (separate messages use separate
        rate-limit buckets, so this is safe) and each edit is serialized against
        event edits via the per-signal lock. A full pass completes in ~1-2s
        regardless of embed count.
        """
        if not self._live_embeds:
            return
        if not self.stream_manager:
            return

        # Prune locks for signals whose embed is gone so the dict stays bounded
        # to roughly the number of live signals.
        for sid in list(self._message_locks.keys()):
            if sid not in self.signal_messages and not self._message_locks[sid].locked():
                del self._message_locks[sid]

        signal_ids = list(self._live_embeds.keys())
        logger.debug("Refreshing %d live embed(s)", len(signal_ids))

        semaphore = asyncio.Semaphore(self._LIVE_REFRESH_CONCURRENCY)

        async def _guarded(signal_id: int):
            async with semaphore:
                await self._refresh_one_embed(signal_id)

        await asyncio.gather(*(_guarded(sid) for sid in signal_ids))

    async def _refresh_one_embed(self, signal_id: int):
        """Re-render a single live embed with the current price and distance."""
        entry = self._live_embeds.get(signal_id)
        if not entry:
            return

        # Stand down while a status/event edit is in flight so it lands first.
        if self._priority_edits_active:
            return

        signal = entry["signal"]
        event = entry["event"]
        spread_buffer_enabled = entry.get("spread_buffer_enabled", False)

        try:
            instrument = signal.instrument
            price_data = await self.stream_manager.get_latest_price(instrument)
            if not price_data:
                return

            direction = (signal.direction or "long").lower()
            current_price = price_data["ask"] if direction == "long" else price_data["bid"]
            spread = price_data.get("spread", 0.0)

            # In-memory limits are kept in lockstep with DB writes by
            # streaming_monitor mutations + the sync helpers called from every
            # command path, so this path no longer round-trips to Postgres.
            limits = signal.limits

            distance_formatted = None
            if event in ("approaching", "hit"):
                pending_limits = [
                    l
                    for l in limits
                    if l.status == "pending" and not l.hit_alert_sent
                ]
                if pending_limits:
                    nearest = min(
                        pending_limits, key=lambda l: abs(current_price - l.price_level)
                    )
                    distance = abs(current_price - nearest.price_level)
                    if self.alert_config:
                        distance_formatted = self.alert_config.format_distance_for_display(
                            instrument, distance, current_price
                        )

            if signal_id not in self._live_embeds:
                logger.debug(
                    f"Live update: signal {signal_id} was unregistered mid-cycle, skipping"
                )
                return

            existing_msg = self.signal_messages.get(signal_id)
            if not existing_msg:
                return

            guild_id = signal.guild_id
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

            # Skip edits that would render an identical embed — keeps the channel
            # quiet for low-volatility signals and frees rate-limit budget.
            signature = self._embed_signature(embed)
            if self._last_live_render.get(signal_id) == signature:
                return

            # Re-check preemption: an event may have fired during the price fetch.
            if self._priority_edits_active:
                return

            try:
                async with self._get_message_lock(signal_id):
                    await existing_msg.edit(embed=embed)
                self._last_live_render[signal_id] = signature
                logger.debug("Live-updated embed for signal %s @ %s", signal_id, current_price)
            except discord.NotFound:
                logger.warning(f"Live update: embed for signal {signal_id} not found, removing")
                self._live_embeds.pop(signal_id, None)
                self.signal_messages.pop(signal_id, None)
                await self._clear_persisted_alert_ids(signal_id)
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
        signal_id = signal.signal_id
        self._live_embeds[signal_id] = {
            "signal": signal,
            "event": event,
            "spread_buffer_enabled": spread_buffer_enabled,
        }

    def _unregister_live_embed(self, signal_id: int):
        self._live_embeds.pop(signal_id, None)
        self._last_live_render.pop(signal_id, None)

    @staticmethod
    def _embed_signature(embed: discord.Embed) -> str:
        """Content signature of an embed, ignoring the volatile timestamp."""
        data = embed.to_dict()
        data.pop("timestamp", None)
        return json.dumps(data, sort_keys=True, default=str)

    def _get_message_lock(self, signal_id: int) -> asyncio.Lock:
        """Return (creating if needed) the per-signal edit lock."""
        lock = self._message_locks.get(signal_id)
        if lock is None:
            lock = asyncio.Lock()
            self._message_locks[signal_id] = lock
        return lock

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

    def set_risky_channel(self, channel: discord.TextChannel):
        self.risky_alert_channel = channel
        logger.info(f"Risky alert channel set: #{channel.name} ({channel.id})")

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
        self.risky_channel_ids = set()
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
            elif name_lower == "risky-gold":
                self.risky_channel_ids.add(str(channel_id))
            elif "toll" in name_lower:
                self.toll_channel_ids.add(str(channel_id))

        # Channels whose original signal messages are auto-deleted on end-state
        # (matches toll-style cleanup). PA channel is exempt to preserve analysis history.
        AUTO_PURGE_EXEMPT_NAMES = {"price-action-trades"}
        self.auto_purge_channel_ids = {
            str(cid)
            for name, cid in monitored.items()
            if cid and name.lower() not in AUTO_PURGE_EXEMPT_NAMES
        }

        self._finished_channel_id = cfg.get("finished_signals")
        self._profit_channel_id = cfg.get("profit_channel")

        role_id = cfg.get("alert_role_id", "1334203997107650662")
        self.role_mention = f"<@&{role_id}>"

        logger.info(
            f"Channels loaded: {len(self.pa_channel_ids)} PA, "
            f"{len(self.toll_channel_ids)} toll "
            f"(incl. {len(self.oil_toll_channel_ids)} oil-toll), "
            f"{len(self.general_toll_channel_ids)} general-toll, "
            f"{len(self.legends_channel_ids)} legends, "
            f"{len(self.risky_channel_ids)} risky, "
            f"{len(self.auto_purge_channel_ids)} auto-purge"
        )

    def reload_channels(self):
        """Re-read channels.json and refresh all cached channel IDs. Call on !reload."""
        self._load_channels_config()

    def is_pa_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.pa_channel_ids

    def is_toll_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.toll_channel_ids

    def is_general_toll_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.general_toll_channel_ids

    def is_oil_toll_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.oil_toll_channel_ids

    def is_legends_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.legends_channel_ids

    def is_risky_signal(self, signal: Dict) -> bool:
        return str(signal.channel_id or "") in self.risky_channel_ids

    def is_auto_purge_channel(self, channel_id) -> bool:
        return str(channel_id) in self.auto_purge_channel_ids

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
        if self.is_risky_signal(signal):
            if self.risky_alert_channel:
                return self.risky_alert_channel
            logger.warning("Risky signal but no risky alert channel; falling back")
        return self.alert_channel

    def _get_finished_channel(self):
        return self._archive_manager._get_finished_channel()

    async def _maybe_delete_original_message(self, signal: Dict, signal_id: int) -> None:
        await self._archive_manager.maybe_delete_original_message(signal, signal_id)

    # ── Tracking ─────────────────────────────────────────────────────────────

    def track_alert_message(self, message_id: int, signal_id: int):
        """Register a message_id -> signal_id mapping (used by reply handler)."""
        self.alert_messages[str(message_id)] = signal_id
        while len(self.alert_messages) > 1000:
            self.alert_messages.popitem(last=False)

    def get_signal_from_alert(self, message_id: str) -> Optional[int]:
        return self.alert_messages.get(str(message_id))

    # ── Persisted message-ID helpers (for restart hydration) ─────────────────

    async def _persist_alert_message(
        self, signal_id: int, message_id: int, channel_id: int
    ) -> None:
        if not self.bot or not self.bot.signal_db:
            return
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                await conn.execute(
                    "UPDATE signals SET alert_message_id = $1, alert_channel_id = $2 WHERE id = $3",
                    int(message_id),
                    int(channel_id),
                    int(signal_id),
                )
        except Exception as e:
            logger.error(f"Failed to persist alert message ID for signal {signal_id}: {e}")

    async def _persist_ping_message(self, signal_id: int, ping_id: int) -> None:
        if not self.bot or not self.bot.signal_db:
            return
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                await conn.execute(
                    "UPDATE signals SET ping_message_id = $1 WHERE id = $2",
                    int(ping_id),
                    int(signal_id),
                )
        except Exception as e:
            logger.error(f"Failed to persist ping message ID for signal {signal_id}: {e}")

    async def _clear_persisted_alert_ids(self, signal_id: int) -> None:
        if not self.bot or not self.bot.signal_db:
            return
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                await conn.execute(
                    "UPDATE signals "
                    "SET alert_message_id = NULL, alert_channel_id = NULL, ping_message_id = NULL "
                    "WHERE id = $1",
                    int(signal_id),
                )
        except Exception as e:
            logger.error(f"Failed to clear persisted alert IDs for signal {signal_id}: {e}")

    async def _reset_approaching_alert_sent(self, signal_id: int) -> None:
        """Clear approaching_alert_sent on all pending limits so the alert can re-fire."""
        if not self.bot or not self.bot.signal_db:
            return
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                await conn.execute(
                    "UPDATE limits SET approaching_alert_sent = FALSE "
                    "WHERE signal_id = $1 AND status = 'pending'",
                    int(signal_id),
                )
        except Exception as e:
            logger.error(f"Failed to reset approaching_alert_sent for signal {signal_id}: {e}")

    # ── Approaching-alert retraction ─────────────────────────────────────────

    async def retract_approaching_embed(self, signal_id: int) -> None:
        """
        Delete the persistent embed and ping for a signal whose price has drifted
        away. Signal stays active; on re-approach a fresh embed is created.
        """
        ping_msg = self.signal_ping_messages.pop(signal_id, None)
        if ping_msg:
            self.alert_messages.pop(str(ping_msg.id), None)
            try:
                await ping_msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"Could not delete ping during retraction for signal {signal_id}: {e}")

        embed_msg = self.signal_messages.pop(signal_id, None)
        if embed_msg:
            self.alert_messages.pop(str(embed_msg.id), None)
            try:
                await embed_msg.delete()
                logger.info(
                    f"Retracted approaching embed for signal {signal_id} (msg {embed_msg.id})"
                )
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(
                    f"Could not delete embed during retraction for signal {signal_id}: {e}"
                )

        self._unregister_live_embed(signal_id)
        await self._clear_persisted_alert_ids(signal_id)

    # ── Restart hydration ────────────────────────────────────────────────────

    async def hydrate_from_db(self, signals: List[Dict]) -> None:
        """
        On startup, recover alert embed references for each loaded signal.

        Found on Discord: reuse the existing embed (re-populate dicts + register
        for live updates). Missing: ACTIVE signals reset approaching_alert_sent
        so the alert can re-fire on the next tick; HIT signals rebuild a fresh
        embed immediately so live updates and future events have a target.
        """
        for signal in signals:
            try:
                await self._hydrate_signal(signal)
            except Exception as e:
                logger.error(
                    f"Hydration failed for signal {signal.signal_id}: {e}",
                    exc_info=True,
                )

    async def recover_pending_archives(self) -> None:
        """
        End-state signals whose alert embed was not archived before the bot
        restarted (the 15-min in-memory countdown is lost on shutdown) get
        re-scheduled here, so the embed + auto-purge original message are
        cleaned up instead of lingering forever.
        """
        if not self.bot or not self.bot.signal_db:
            return
        try:
            rows = await self.bot.signal_db.db.fetch_all(
                """
                SELECT id, status, channel_id, message_id,
                       alert_message_id, alert_channel_id, ping_message_id
                FROM signals
                WHERE alert_message_id IS NOT NULL
                  AND status IN ('profit', 'stop_loss', 'cancelled', 'breakeven')
                """,
                (),
            )
        except Exception as e:
            logger.error(f"recover_pending_archives query failed: {e}")
            return

        if not rows:
            return

        recovered = 0
        for row in rows:
            signal_id = row["id"]
            status = row["status"]
            try:
                channel = None
                if row["alert_channel_id"]:
                    channel = self.bot.get_channel(int(row["alert_channel_id"]))
                if channel is None:
                    # Channel cache miss — fall back to status-based routing.
                    channel = self._get_alert_channel({"channel_id": row["channel_id"]})
                if channel is None:
                    logger.warning(
                        f"Cannot resolve alert channel for orphaned signal {signal_id}; clearing IDs"
                    )
                    await self._clear_persisted_alert_ids(signal_id)
                    continue

                try:
                    embed_msg = await channel.fetch_message(int(row["alert_message_id"]))
                except discord.NotFound:
                    await self._clear_persisted_alert_ids(signal_id)
                    continue

                self.signal_messages[signal_id] = embed_msg
                self.track_alert_message(embed_msg.id, signal_id)

                if row["ping_message_id"]:
                    try:
                        ping_msg = await channel.fetch_message(int(row["ping_message_id"]))
                        self.signal_ping_messages[signal_id] = ping_msg
                        self.track_alert_message(ping_msg.id, signal_id)
                    except discord.NotFound:
                        pass
                    except Exception as e:
                        logger.debug(
                            f"Could not refetch ping {row['ping_message_id']} for signal {signal_id}: {e}"
                        )

                self._archive_manager.schedule_end_state_move(signal_id, event=status)
                recovered += 1
            except Exception as e:
                logger.warning(
                    f"Could not recover pending archive for signal {signal_id}: {e}",
                    exc_info=True,
                )

        if recovered:
            logger.info(f"Re-scheduled archive move for {recovered} orphaned end-state signal(s)")

    async def recover_finished_embeds(self) -> None:
        """
        Re-register finished-channel embeds at startup so reply commands against
        them (e.g. `reactivate`) work after a restart. Uses ``PartialMessage`` —
        no Discord API calls, just local channel-cache lookups, so startup cost
        is O(N) regardless of how many signals are recovered.
        """
        if not self.bot or not self.bot.signal_db:
            return
        try:
            rows = await self.bot.signal_db.db.fetch_all(
                """
                SELECT id, finished_message_id, finished_channel_id
                FROM signals
                WHERE finished_message_id IS NOT NULL
                  AND finished_channel_id IS NOT NULL
                  AND status IN ('profit', 'stop_loss', 'cancelled', 'breakeven')
                  AND (closed_at IS NULL OR closed_at > NOW() - INTERVAL '14 days')
                """,
                (),
            )
        except Exception as e:
            logger.error(f"recover_finished_embeds query failed: {e}")
            return

        if not rows:
            return

        recovered = 0
        for row in rows:
            signal_id = row["id"]
            channel = self.bot.get_channel(int(row["finished_channel_id"]))
            if channel is None:
                continue
            try:
                partial = channel.get_partial_message(int(row["finished_message_id"]))
            except Exception as e:
                logger.warning(
                    f"Could not build partial message for signal {signal_id}: {e}"
                )
                continue
            self.signal_finished_messages[signal_id] = partial
            self.track_alert_message(partial.id, signal_id)
            recovered += 1

        if recovered:
            logger.info(f"Re-registered {recovered} finished-channel embed(s) for reply lookup")

    async def _hydrate_signal(self, signal: Dict) -> None:
        signal_id = signal.signal_id
        status = signal.status
        alert_message_id = signal.alert_message_id
        alert_channel_id = signal.alert_channel_id
        ping_message_id = signal.ping_message_id

        if alert_message_id:
            channel = self._resolve_channel(signal, alert_channel_id)
            if channel:
                try:
                    embed_msg = await channel.fetch_message(int(alert_message_id))
                    self.signal_messages[signal_id] = embed_msg
                    self.track_alert_message(embed_msg.id, signal_id)

                    if ping_message_id:
                        try:
                            ping_msg = await channel.fetch_message(int(ping_message_id))
                            self.signal_ping_messages[signal_id] = ping_msg
                            self.track_alert_message(ping_msg.id, signal_id)
                        except discord.NotFound:
                            pass
                        except Exception as e:
                            logger.debug(
                                f"Could not refetch ping {ping_message_id} for signal {signal_id}: {e}"
                            )

                    event = "hit" if status == "hit" else "approaching"
                    self._register_live_embed(signal, event, spread_buffer_enabled=True)
                    logger.info(
                        f"Hydrated alert embed for signal {signal_id} "
                        f"(channel={channel.id}, msg={embed_msg.id}, event={event})"
                    )
                    return
                except discord.NotFound:
                    logger.info(
                        f"Persisted alert embed for signal {signal_id} no longer exists "
                        f"(msg={alert_message_id}) — falling back"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not fetch persisted alert embed for signal {signal_id}: {e}"
                    )

            await self._clear_persisted_alert_ids(signal_id)

        await self._hydrate_fallback(signal)

    def _resolve_channel(
        self, signal: Dict, persisted_channel_id: Optional[int]
    ) -> Optional[discord.TextChannel]:
        if persisted_channel_id and self.bot:
            channel = self.bot.get_channel(int(persisted_channel_id))
            if channel:
                return channel
        return self._get_alert_channel(signal)

    async def _hydrate_fallback(self, signal: Dict) -> None:
        """
        Embed reference is gone. For HIT signals rebuild immediately; for ACTIVE
        signals reset approaching_alert_sent so the alert re-fires naturally.
        """
        signal_id = signal.signal_id
        status = signal.status

        if status == "hit":
            rebuilt = await self.reactivate_embed(signal, ping_text=None)
            if rebuilt:
                logger.info(f"Rebuilt missing embed for HIT signal {signal_id} on startup")
            else:
                logger.warning(f"Could not rebuild embed for HIT signal {signal_id} on startup")
            return

        limits = signal.limits
        if any(l.approaching_alert_sent for l in limits):
            await self._reset_approaching_alert_sent(signal_id)
            for limit in limits:
                if limit.status == "pending":
                    limit.approaching_alert_sent = False
            logger.info(
                f"Reset approaching_alert_sent for ACTIVE signal {signal_id} "
                f"after losing embed reference"
            )

    # ── Limit fetcher ────────────────────────────────────────────────────────

    async def _fetch_limits(self, signal: Dict) -> List[Dict]:
        """
        Get ALL limits for a signal (hit + pending).

        Prefers the in-memory limits the streaming monitor keeps in lockstep with
        the DB (same source the 15 s live-refresh uses) so an event edit doesn't
        pay a Postgres round-trip before updating the embed. Falls back to the DB
        only when the signal carries no limits (e.g. command paths that pass a
        bare signal).
        """
        in_memory = signal.limits
        if in_memory:
            return in_memory

        if self.bot and self.bot.signal_db:
            try:
                full = await self.bot.signal_db.get_signal_with_limits(signal.signal_id)
                if full:
                    return full.limits
            except Exception as e:
                logger.warning(
                    f"Could not fetch limits from DB for signal {signal.signal_id}: {e}"
                )
        return []

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
        force_hit_up_to_seq: int = 0,
        limit_pnl_map: Optional[Dict] = None,
        delete_after_minutes: Optional[int] = None,
    ) -> Optional[discord.Message]:
        """
        Send a ping then create or edit the persistent embed for this signal.
        """
        signal_id = signal.signal_id
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            logger.error("No alert channel configured")
            return None

        guild_id = signal.guild_id
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
            force_hit_up_to_seq=force_hit_up_to_seq,
            limit_pnl_map=limit_pnl_map,
            delete_after_minutes=delete_after_minutes,
        )

        # Status/event edits preempt cosmetic distance refreshes: while one is
        # in flight the live loop stands down (see _refresh_one_embed) so the
        # event lands without competing for the rate-limit budget. Serialize
        # against the live-refresh edit for this same signal too, so the two
        # never overlap or land out of order on the same message.
        self._priority_edits_active += 1
        try:
            async with self._get_message_lock(signal_id):
                existing_msg = self.signal_messages.get(signal_id)
                embed_msg = None

                if existing_msg:
                    try:
                        await existing_msg.edit(embed=embed)
                        logger.info(
                            f"Edited persistent message for signal {signal_id} (event={event})"
                        )
                        embed_msg = existing_msg
                    except discord.NotFound:
                        logger.warning(
                            f"Persistent message for signal {signal_id} deleted — recreating"
                        )
                        del self.signal_messages[signal_id]
                        existing_msg = None
                    except Exception as e:
                        logger.error(
                            f"Failed to edit persistent message for signal {signal_id}: {e}"
                        )
                        return None

                if not existing_msg:
                    try:
                        embed_msg = await target_channel.send(
                            content=self.role_mention, embed=embed
                        )
                        self.signal_messages[signal_id] = embed_msg
                        self.track_alert_message(embed_msg.id, signal_id)
                        await self._persist_alert_message(
                            signal_id, embed_msg.id, target_channel.id
                        )
                        logger.info(
                            f"Created persistent message for signal {signal_id} (event={event})"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to send new persistent message for signal {signal_id}: {e}"
                        )
                        return None

                if ping_text and embed_msg:
                    old_ping = self.signal_ping_messages.get(signal_id)
                    if old_ping:
                        self.alert_messages.pop(str(old_ping.id), None)
                        try:
                            await old_ping.delete()
                        except discord.NotFound:
                            pass
                        except Exception as e:
                            logger.warning(
                                f"Could not delete old ping for signal {signal_id}: {e}"
                            )

                    try:
                        new_ping = await embed_msg.reply(f"{self.role_mention} {ping_text}")
                        self.signal_ping_messages[signal_id] = new_ping
                        self.track_alert_message(new_ping.id, signal_id)
                        await self._persist_ping_message(signal_id, new_ping.id)
                    except Exception as e:
                        logger.error(f"Failed to send ping for signal {signal_id}: {e}")
        finally:
            self._priority_edits_active -= 1

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
        if limit.sequence_number != 1:
            logger.debug(
                f"Skipping approaching alert for limit #{limit.sequence_number} (not first)"
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
            seq = limit.sequence_number
            total = len(limits)

            force_hit_up_to_seq = seq if isinstance(seq, int) else 0
            hit_count = sum(
                1
                for l in limits
                if l.status == "hit"
                or l.hit_alert_sent
                or l.sequence_number <= force_hit_up_to_seq
            )

            suffix = "🎯🎯 **FINAL**" if seq == total else "🎯"
            ping = (
                f"{suffix} **{signal.instrument}** {signal.direction.upper()} — "
                f"limit #{seq} hit @ {_fmt(limit.price_level)} "
                f"({hit_count}/{total} done)"
            )

            distance_formatted = None
            pending_limits = [
                l
                for l in limits
                if l.status != "hit"
                and not l.hit_alert_sent
                and l.sequence_number > force_hit_up_to_seq
            ]
            if pending_limits and current_price is not None:
                nearest = min(pending_limits, key=lambda l: abs(current_price - l.price_level))
                distance = abs(current_price - nearest.price_level)
                if self.alert_config:
                    distance_formatted = self.alert_config.format_distance_for_display(
                        signal.instrument, distance, current_price
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
            signal_id = signal.signal_id
            self._unregister_live_embed(signal_id)
            limits = await self._fetch_limits(signal)
            ping = (
                f"🛑 **{signal.instrument}** {signal.direction.upper()} — "
                f"stop loss hit @ {_fmt(current_price)} (SL: {_fmt(signal.stop_loss)})"
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
        instrument = signal.instrument
        direction = signal.direction.upper()
        self._unregister_live_embed(signal.signal_id)
        display_pnl = cumulative_pnl if cumulative_pnl is not None else last_pnl
        pnl_display = tp_config.format_value(instrument, display_pnl)
        num_hit = len(hit_limits)

        hit_limit_ids = {lim.id for lim in hit_limits}
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
                limit_pnl_map=limit_pnl_map,
                delete_after_minutes=END_STATE_DELETE_MINUTES,
            )
            if msg:
                self._archive_manager.schedule_end_state_move(signal.signal_id, event="auto_tp")
                self.stats["auto_tp_sent"] += 1
                self.stats["total_alerts"] += 1
        except Exception as e:
            logger.error(f"Failed to update embed for auto-TP signal {signal.signal_id}: {e}")
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
        signal_id = signal.signal_id
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

        if self.bot and self.bot.signal_db:
            try:
                async with self.bot.signal_db.db.get_connection() as conn:
                    await conn.execute(
                        "UPDATE signals "
                        "SET finished_message_id = NULL, finished_channel_id = NULL "
                        "WHERE id = $1",
                        int(signal_id),
                    )
            except Exception as e:
                logger.warning(
                    f"Could not clear persisted finished IDs for signal {signal_id}: {e}"
                )

        limits = await self._fetch_limits(signal)

        # Caller often passes a signal dict fetched pre-reactivation, so its
        # attached limits still carry status='cancelled'. The live-update loop
        # would then render the embed with strikethrough/❌ rows on the next
        # 15s tick. Sync the dict before _register_live_embed stores it.
        try:
            signal.limits = limits
        except Exception:
            pass

        hit_count = sum(1 for l in limits if l.status == "hit" or l.hit_alert_sent)
        event = "hit" if hit_count > 0 else "approaching"

        current_price = None
        distance_formatted = None
        spread = 0.0
        spread_buffer_enabled = False

        if self.stream_manager:
            try:
                price_data = await self.stream_manager.get_latest_price(signal.instrument)
                if price_data:
                    direction = (signal.direction or "long").lower()
                    current_price = price_data["ask"] if direction == "long" else price_data["bid"]
                    spread = price_data.get("spread", 0.0)

                    monitor = getattr(self.bot, "monitor", None) if self.bot else None
                    _sbe = getattr(monitor, "_spread_buffer_enabled", None) if monitor else None
                    spread_buffer_enabled = _sbe if _sbe is not None else True

                    if event == "approaching" and limits:
                        pending = [
                            l for l in limits if l.status != "hit" and not l.hit_alert_sent
                        ]
                        if pending:
                            nearest = min(
                                pending, key=lambda l: abs(current_price - l.price_level)
                            )
                            distance = abs(current_price - nearest.price_level)
                            if self.alert_config:
                                distance_formatted = self.alert_config.format_distance_for_display(
                                    signal.instrument, distance, current_price
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

    async def _reattach_persisted_embed(self, signal: Dict) -> bool:
        """Re-attach a signal's persistent embed from its DB-persisted reference.

        Used when the in-memory ``signal_messages`` entry was lost on restart but
        the embed still exists in Discord (e.g. hydration hit a transient fetch
        error). Mirrors the success path of ``_hydrate_signal``. Returns True if
        the embed was re-attached and registered for live updates.
        """
        signal_id = signal.signal_id
        alert_message_id = signal.alert_message_id
        if not alert_message_id:
            return False
        channel = self._resolve_channel(signal, signal.alert_channel_id)
        if not channel:
            return False
        try:
            embed_msg = await channel.fetch_message(int(alert_message_id))
        except discord.NotFound:
            return False
        except Exception as e:
            logger.warning(f"Could not re-attach embed for signal {signal_id}: {e}")
            return False

        self.signal_messages[signal_id] = embed_msg
        self.track_alert_message(embed_msg.id, signal_id)
        event = "hit" if signal.status == "hit" else "approaching"
        self._register_live_embed(signal, event, spread_buffer_enabled=True)
        logger.info(f"Re-attached persistent embed for signal {signal_id} (msg={embed_msg.id})")
        return True

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
        signal_id = signal.signal_id

        if event == "reactivated":
            return await self.reactivate_embed(signal=signal, ping_text=ping_text)

        if signal_id not in self.signal_messages and not await self._reattach_persisted_embed(
            signal
        ):
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
            "late_market_cancelled",
            "risky_window_cancelled",
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
            # Keep the live-refresh loop's cached dict in lockstep with the limits
            # just rendered, so the next 15s cycle does not re-render stale limits
            # after an edit. Terminal events are already unregistered above.
            if signal_id in self._live_embeds:
                signal.limits = limits
                self._live_embeds[signal_id]["signal"] = signal
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
        signal_id = signal.signal_id
        if signal_id not in self.signal_messages:
            logger.debug(
                f"Spread hour cancel for signal {signal_id}: no persistent embed, skipping alert"
            )
            await self._archive_manager.maybe_delete_original_message(signal, signal_id)
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

    async def send_late_market_cancel_alert(self, signal: Dict, current_price: float) -> bool:
        signal_id = signal.signal_id
        if signal_id not in self.signal_messages:
            logger.debug(
                f"Late market cancel for signal {signal_id}: no persistent embed, skipping alert"
            )
            await self._archive_manager.maybe_delete_original_message(signal, signal_id)
            return True
        try:
            await self.update_signal_message(
                signal=signal,
                event="late_market_cancelled",
                current_price=current_price,
                ping_text="Signal cancelled — late market hours.",
            )
            self.stats["late_market_cancelled"] += 1
            self.stats["total_alerts"] += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send late market cancel alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_risky_window_cancel_alert(self, signal: Dict, current_price: float) -> bool:
        signal_id = signal.signal_id
        if signal_id not in self.signal_messages:
            logger.debug(
                f"Risky window cancel for signal {signal_id}: no persistent embed, skipping alert"
            )
            await self._archive_manager.maybe_delete_original_message(signal, signal_id)
            return True
        try:
            await self.update_signal_message(
                signal=signal,
                event="risky_window_cancelled",
                current_price=current_price,
                ping_text="Signal cancelled — risky trades disabled.",
            )
            self.stats["risky_window_cancelled"] = self.stats.get("risky_window_cancelled", 0) + 1
            self.stats["total_alerts"] += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send risky window cancel alert: {e}")
            self.stats["errors"] += 1
            return False

    async def send_news_cancel_alert(self, signal: Dict, current_price: float, news_event) -> bool:
        signal_id = signal.signal_id
        target_channel = self._get_alert_channel(signal)
        if not target_channel:
            return False

        instrument = signal.instrument or "?"
        direction = signal.direction.upper()

        if signal_id in self.signal_messages:
            try:
                await self.update_signal_message(
                    signal=signal,
                    event="cancelled",
                    current_price=current_price,
                    ping_text=f"📰 **{instrument}** {direction} — cancelled (news: {news_event.display_label})",
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
            all_limits = signal.limits
            if all_limits:
                limit_prices = "  |  ".join(
                    _fmt(l.price_level)
                    for l in sorted(all_limits, key=lambda l: l.sequence_number)
                )
            else:
                limit_prices = "—"
            signal_summary = (
                f"**{instrument}** {direction}\n"
                f"Limits: {limit_prices}\n"
                f"SL: {_fmt(signal.stop_loss)}"
            )
            embed = discord.Embed(
                title="📰 Signal Cancelled — News",
                description=(
                    f"The following signal was cancelled due to news "
                    f"({news_event.display_label} @ "
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
            if signal.message_id and signal.channel_id:
                if not str(signal.message_id).startswith("manual_"):
                    guild_id = signal.guild_id
                    if not guild_id and self.bot and self.bot.guilds:
                        guild_id = self.bot.guilds[0].id
                    url = f"https://discord.com/channels/{guild_id}/{signal.channel_id}/{signal.message_id}"
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
        signal_id = signal.signal_id
        instrument = signal.instrument
        direction = signal.direction.upper()

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
            self.risky_alert_channel,
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
                f"News window activated for **{news_event.display_label}**\n{time_str}"
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
                f"News window for **{news_event.display_label}** has ended.\n"
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
