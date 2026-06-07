"""
Signal Lifecycle Commands — create, view, status changes, bulk cancels.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import discord
from discord.ext import commands

from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.tp_config import TPConfig
from utils.formatting import format_price, format_signal_type, get_status_emoji
from utils.logger import get_logger

from .._base import BaseCog
from ..views import ActiveSignalsView

logger = get_logger("lifecycle_commands")


class LifecycleCog(BaseCog):
    """Signal lifecycle commands: create, view, status changes, bulk cancels"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()

    @commands.command(name="active")
    async def active_signals(self, ctx: commands.Context, *, args: str = None):
        """
        Display active trading signals with sorting and pagination

        Usage:
            !active - Show most recent active signals
            !active BTCUSDT - Filter by instrument
            !active sort:distance - Sort by distance to limit
            !active sort:recent - Sort by most recent (default)
            !active sort:oldest - Sort by oldest first
            !active sort:progress - Sort by most limits hit
            !active BTCUSDT sort:distance - Combine filter and sort
        """
        loading_msg = await ctx.send("🔄 Loading active signals...")

        # Parse arguments
        instrument = None
        sort_method = "recent"  # default

        if args:
            args_parts = args.split()
            for part in args_parts:
                if part.startswith("sort:"):
                    sort_method = part.split(":", 1)[1].lower()
                else:
                    # Assume it's an instrument filter
                    instrument = part.upper()

        # Validate sort method
        valid_sorts = ["recent", "oldest", "distance", "progress"]
        if sort_method not in valid_sorts:
            await loading_msg.edit(
                content=f"❌ Invalid sort method. Valid options: {', '.join(valid_sorts)}"
            )
            return

        signals = await self.signal_db.get_active_signals_detailed_sorted(
            instrument if instrument else None
        )

        if not signals:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found"
                + (f" for {instrument}" if instrument else ""),
                color=0xFFA500,
            )
            await loading_msg.edit(content=None, embed=embed)
            return

        # Add asset type flags and calculate distances
        mapper = self.services.stream_manager.symbol_mapper
        dollar_distance_classes = {"crypto", "indices", "metals", "oil"}
        for signal in signals:
            asset_class = mapper.determine_asset_class(signal["instrument"])
            signal["is_crypto"] = asset_class == "crypto"
            signal["is_index"] = asset_class == "indices"

            # Calculate distance to next limit
            if self.services.monitor and signal.get("pending_limits"):
                try:
                    alert_config = AlertDistanceConfig()

                    symbol = signal["instrument"]
                    cached_price = await self.services.stream_manager.get_latest_price(symbol)

                    if cached_price:
                        direction = signal["direction"].lower()
                        current_price = (
                            cached_price["ask"] if direction == "long" else cached_price["bid"]
                        )
                        limit_price = signal["pending_limits"][0]

                        # Calculate raw price distance
                        if direction == "long":
                            distance = current_price - limit_price
                        else:
                            distance = limit_price - current_price

                        # Format based on asset type. Metals/oil display in dollars
                        # (same as crypto/indices) so the sort key is the visible number.
                        if asset_class in dollar_distance_classes:
                            distance_value = abs(distance)
                            formatted = f"${distance_value:.2f} away"
                        else:
                            formatted = alert_config.format_distance_for_display(
                                symbol, abs(distance), current_price
                            )
                            pip_size = alert_config.get_pip_size(symbol)
                            distance_value = abs(distance) / pip_size

                        signal["distance_info"] = {
                            "distance": distance_value,
                            "current_price": current_price,
                            "formatted": formatted,
                        }
                except Exception as e:
                    logger.warning(f"Could not get price for {symbol}: {e}")

        # Apply sorting
        if sort_method == "recent":
            # Already sorted by created_at DESC from database
            pass
        elif sort_method == "oldest":
            signals.reverse()
        elif sort_method == "distance":
            # Sort by distance (closest first)
            def get_distance_key(signal):
                if signal.get("distance_info"):
                    return signal["distance_info"]["distance"]
                return float("inf")  # Put signals without distance at the end

            signals.sort(key=get_distance_key)
        elif sort_method == "progress":
            # Sort by number of limits hit (most progress first)
            signals.sort(key=lambda s: len(s.get("hit_limits", [])), reverse=True)

        # Create pagination view
        view = ActiveSignalsView(signals=signals, guild_id=ctx.guild.id, instrument=instrument)

        # Get initial embed
        embed = view.get_page_embed()

        # Add sort info to footer
        sort_descriptions = {
            "recent": "Most Recent First",
            "oldest": "Oldest First",
            "distance": "Closest to Limit",
            "progress": "Most Progress",
        }

        current_footer = embed.footer.text if embed.footer else ""
        sort_info = f" | Sorted by: {sort_descriptions.get(sort_method, sort_method.title())}"
        embed.set_footer(text=current_footer + sort_info)

        await loading_msg.edit(content=None, embed=embed, view=view)

    @commands.command(name="info")
    async def signal_info(self, ctx: commands.Context, signal_id: int):
        """Show detailed information about a signal"""
        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        status_emoji = get_status_emoji(signal["status"])

        embed = discord.Embed(
            title=f"{status_emoji} Signal #{signal_id} - {signal['instrument']}", color=0x00BFFF
        )

        embed.add_field(name="Direction", value=signal["direction"].upper(), inline=True)
        embed.add_field(name="Status", value=signal["status"].upper(), inline=True)
        embed.add_field(
            name="Type",
            value=format_signal_type(signal.get("type", "standard")),
            inline=True,
        )

        stop_loss_formatted = (
            format_price(signal["stop_loss"], signal["instrument"])
            if signal["stop_loss"]
            else "N/A"
        )
        embed.add_field(name="Stop Loss", value=stop_loss_formatted, inline=True)

        # Streaming status
        if self.services.monitor:
            is_subscribed = signal["instrument"] in self.services.stream_manager.subscribed_symbols
            embed.add_field(
                name="Streaming Status",
                value="🟢 Subscribed" if is_subscribed else "⚪ Not Subscribed",
                inline=True,
            )

        # Limits info
        if signal["limits"]:
            pending_limits = [l for l in signal["limits"] if l["status"] == "pending"]
            hit_limits = [l for l in signal["limits"] if l["status"] == "hit"]

            if pending_limits:
                pending_str = "\n".join(
                    [
                        f"• {format_price(l['price_level'], signal['instrument'])}"
                        for l in pending_limits[:5]
                    ]
                )
                if len(pending_limits) > 5:
                    pending_str += f"\n... +{len(pending_limits) - 5} more"
                embed.add_field(
                    name=f"Pending Limits ({len(pending_limits)})", value=pending_str, inline=False
                )

            if hit_limits:
                hit_str = "\n".join(
                    [
                        f"• {format_price(l['price_level'], signal['instrument'])} ✅"
                        for l in hit_limits[:5]
                    ]
                )
                if len(hit_limits) > 5:
                    hit_str += f"\n... +{len(hit_limits) - 5} more"
                embed.add_field(name=f"Hit Limits ({len(hit_limits)})", value=hit_str, inline=False)

        # Progress
        embed.add_field(
            name="Progress",
            value=f"{signal.get('limits_hit', 0)}/{signal.get('total_limits', 0)} limits hit",
            inline=True,
        )

        # Timestamps
        if signal.get("first_limit_hit_time"):
            try:
                timestamp = signal["first_limit_hit_time"]
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                embed.add_field(
                    name="First Hit", value=f"<t:{int(timestamp.timestamp())}:R>", inline=True
                )
            except:
                pass

        if signal.get("closed_at"):
            try:
                timestamp = signal["closed_at"]
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                embed.add_field(
                    name="Closed", value=f"<t:{int(timestamp.timestamp())}:R>", inline=True
                )
            except:
                pass

        # Link to original message
        if not str(signal["message_id"]).startswith("manual_"):
            message_url = f"https://discord.com/channels/{ctx.guild.id}/{signal['channel_id']}/{signal['message_id']}"
            embed.add_field(name="Source", value=f"[Jump to message]({message_url})", inline=False)
        else:
            embed.add_field(name="Source", value="Manual Entry", inline=False)

        created_at = signal["created_at"]
        if hasattr(created_at, "strftime"):
            created_at = created_at.strftime("%Y-%m-%d %H:%M UTC")
        embed.set_footer(text=f"Created {created_at}")

        await ctx.send(embed=embed)

    @commands.command(name="setstatus", description="Set signal status")
    async def set_signal_status(
        self, ctx: commands.Context, signal_id: int, status: str, *, rest: str = ""
    ):
        """Manually set a signal's status. Append --force to bypass the reactivation guard (admin only)."""
        valid_statuses = [
            "active",
            "hit",
            "profit",
            "breakeven",
            "stop_loss",
            "cancelled",
            "cancel",
        ]
        status = status.lower()
        force = "--force" in rest.split()

        if status == "cancel":
            status = "cancelled"

        if status not in valid_statuses:
            await ctx.send(f"❌ Invalid status. Valid options: {', '.join(valid_statuses)}")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        # Reactivation guard: block if current price has passed any pending limits,
        # unless the admin explicitly requests --force.
        if status == "active" and signal["status"] in ("cancelled", "stop_loss"):
            if force:
                is_admin = (
                    hasattr(ctx.author, "guild_permissions")
                    and ctx.author.guild_permissions.administrator
                )
                if not is_admin:
                    await ctx.send("❌ `--force` requires administrator permissions.")
                    return
            else:
                try:
                    guard = await self.signal_db.check_reactivation_guard(signal_id)
                except Exception as _ge:
                    logger.warning(
                        f"Reactivation guard check failed for signal {signal_id}: {_ge}"
                    )
                    guard = None

                if guard and guard["blocked"]:
                    instrument = guard["instrument"]
                    cur = format_price(guard["current_price"], instrument)
                    limit_lines = "\n".join(
                        f"• Limit #{lim['sequence_number']}: "
                        f"{format_price(float(lim['price_level']), instrument)}"
                        for lim in guard["blocked_limits"]
                    )
                    embed = discord.Embed(
                        title="❌ Reactivation Blocked",
                        description=(
                            f"Price has already moved past pending limits for signal #{signal_id}.\n"
                            f"Current price: **{cur}**\n\n"
                            f"**Limits past:**\n{limit_lines}"
                        ),
                        color=0xFF0000,
                    )
                    embed.set_footer(
                        text=f"Use '!setstatus {signal_id} active --force' to override (admin only)"
                    )
                    await ctx.send(embed=embed)
                    return

        if status == "profit":
            # If signal is approaching (no limits hit yet), mirror !hit behaviour:
            # mark the first pending limit as hit before setting status to profit.
            current_hit_count = len(signal.get("hit_limits") or [])
            if current_hit_count == 0:
                pending_limits = signal.get("pending_limits") or []
                if pending_limits:
                    sorted_pending = sorted(
                        pending_limits, key=lambda l: l.get("sequence_number", 999)
                    )
                    first_limit = sorted_pending[0]
                    try:
                        from database import db as _db

                        await _db.mark_limit_hit(first_limit["id"], first_limit["price_level"])
                        if self.services.monitor:
                            self.services.monitor._mutate_limit_hit_in_memory(
                                signal_id, first_limit["id"], first_limit["price_level"]
                            )
                        logger.info(
                            f"Auto-hit limit #{first_limit.get('sequence_number')} "
                            f"for signal {signal_id} as part of manual profit (approaching→profit)"
                        )
                    except Exception as _hit_err:
                        logger.warning(
                            f"Could not auto-hit limit for signal {signal_id} on profit: {_hit_err}"
                        )

        success = await self.signal_db.manually_set_signal_status(
            signal_id,
            status,
            f"Manual override by {ctx.author.name}",
        )

        if success:
            if self.services.monitor:
                self.services.monitor.sync_signal_status_in_memory(signal_id, status)
            status_emoji = get_status_emoji(status)

            embed = discord.Embed(
                title=f"{status_emoji} Status Updated",
                description=f"Signal #{signal_id} status changed to **{status.upper()}**",
                color=0x00FF00,
            )
            embed.add_field(name="Instrument", value=signal["instrument"], inline=True)
            embed.add_field(name="Previous Status", value=signal["status"], inline=True)
            embed.set_footer(text=f"Changed by {ctx.author.name}")

            await ctx.send(embed=embed)

            # Update the persistent alert embed
            status_to_event = {
                "profit": "profit",
                "breakeven": "breakeven",
                "stop_loss": "stop_loss",
                "cancelled": "cancelled",
                "active": "reactivated",
                "hit": "hit",
            }
            embed_event = status_to_event.get(status)
            if embed_event:
                try:
                    if self.services.monitor:
                        await self.services.alert_system.update_embed_for_signal_id(
                            signal_id,
                            embed_event,
                        )
                except Exception as _ue:
                    logger.warning(f"Could not update embed after setstatus: {_ue}")

            # If reactivating, mark signal as NM-immune so the monitor can't re-fire.
            # The existing embed is edited in place by reactivate_embed (no duplicate sent).
            if status == "active":
                try:
                    if self.services.monitor:
                        self.services.nm_monitor.mark_immune(signal_id)
                except Exception as _ne:
                    logger.warning(f"Could not mark signal {signal_id} NM-immune: {_ne}")
        else:
            await ctx.send("❌ Failed to update signal status")

    # Shortcut commands for status changes
    @commands.command(name="profit", aliases=[], description="Mark signal as profit")
    async def set_profit(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "profit")

    @commands.command(name="hit", description="Mark signal as hit")
    async def set_hit(self, ctx: commands.Context, signal_id: int):
        """Manually mark a signal as HIT, treating limit 1 as hit and starting auto-TP."""
        try:
            transitioned = await asyncio.wait_for(
                self.signal_db.manually_set_signal_to_hit(
                    signal_id, f"Manually set to HIT by {ctx.author.name}"
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            await ctx.send("❌ Operation timed out. Please try again.")
            return
        except Exception as e:
            self.logger.error(f"Error in !hit command for signal {signal_id}: {e}", exc_info=True)
            await ctx.send("❌ Error processing hit command.")
            return

        if transitioned:
            # Populate TP cache immediately so auto-TP starts on the next tick
            if self.services.monitor:
                await self.services.tp_monitor.refresh_hit_limits(signal_id)
                if signal_id in self.services.monitor.active_signals:
                    self.services.monitor.active_signals[signal_id]["status"] = "hit"
                    self.services.monitor.mark_first_pending_limit_hit_in_memory(signal_id)
            await ctx.send(f"✅ Signal {signal_id} marked as HIT (limit 1 hit, auto-TP active)")

            # Update the persistent alert embed
            try:
                if self.services.monitor:
                    await self.services.alert_system.update_embed_for_signal_id(signal_id, "hit")
            except Exception as _ue:
                logger.warning(f"Could not update embed after !hit: {_ue}")
        else:
            # Either already HIT or not in a valid state
            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if signal and signal.get("status") == "hit":
                await ctx.send(f"ℹ️ Signal {signal_id} is already HIT — auto-TP is already active.")
            else:
                await ctx.send(
                    f"❌ Could not mark signal {signal_id} as HIT. Signal must be in ACTIVE status."
                )

    @commands.command(name="stoploss", aliases=["sl"], description="Mark signal as stop loss")
    async def set_stop_loss(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "stop_loss")

    @commands.command(
        name="cancel", aliases=["nm"], description="Cancel a signal or bulk cancel signals"
    )
    async def set_cancelled(self, ctx: commands.Context, *, args: str = None):
        """
        Cancel signals. Supports:
          !cancel <id>                              - Cancel a specific signal
          !cancel gold longs/shorts/both setups     - Cancel gold setup signals
          !cancel gold longs/shorts/both pa         - Cancel gold price action signals
          !cancel gold longs/shorts/both tolls      - Cancel gold toll signals
          !cancel gold longs/shorts/both everything - Cancel all gold signals
          !cancel all <PAIR>                        - Cancel all signals for a pair (e.g. !cancel all EURUSD)
          !cancel all <CURRENCY>                    - Cancel all signals containing a currency (e.g. !cancel all EUR)
        For detailed help: !help cancel
        """
        if args is None:
            await ctx.send(
                "❌ Usage: `!cancel <id>` or `!cancel gold longs/shorts/both <type>` or `!cancel all <PAIR/CURRENCY>`\nSee `!help cancel` for full details."
            )
            return

        args = args.strip()

        # --- !cancel <integer id> ---
        if args.isdigit():
            await self.set_signal_status(ctx, int(args), "cancelled")
            return

        # Normalise to lowercase for matching
        args_lower = args.lower()

        # --- !cancel all <PAIR or CURRENCY> ---
        if args_lower.startswith("all "):
            target = args[4:].strip().upper()
            await self._bulk_cancel_by_target(ctx, target)
            return

        # --- !cancel gold ... ---
        if args_lower.startswith("gold"):
            tokens = args_lower.split()
            # tokens[0] = "gold"; remaining tokens may be in any order
            if len(tokens) < 3:
                await ctx.send(
                    "❌ Usage: `!cancel gold <longs|shorts|both> <setups|pa|tolls|everything>`\nSee `!help cancel` for details."
                )
                return

            remaining = tokens[1:]  # everything after "gold"

            _direction_tokens = {"longs", "shorts", "both"}
            _type_tokens = {"setups", "pa", "priceaction", "price_action", "tolls", "everything"}

            direction_token = next((t for t in remaining if t in _direction_tokens), None)
            type_token = next((t for t in remaining if t in _type_tokens), None)

            if direction_token is None:
                await ctx.send("❌ Direction must be `longs`, `shorts`, or `both`.")
                return

            if type_token is None:
                await ctx.send("❌ Type must be `setups`, `pa`, `tolls`, or `everything`.")
                return

            # Map direction
            direction_filter = (
                None if direction_token == "both" else direction_token.rstrip("s")
            )  # longs->long, shorts->short

            # Map type to channel category
            channel_category = None
            if type_token in ("pa", "priceaction", "price_action"):
                channel_category = "pa"
            elif type_token == "tolls":
                channel_category = "tolls"
            elif type_token == "setups":
                channel_category = "setups"
            # "everything" -> channel_category stays None (all categories)

            await self._bulk_cancel_gold(ctx, direction_filter, channel_category)
            return

        # Unrecognised syntax
        await ctx.send("❌ Unrecognised cancel syntax. See `!help cancel` for usage.")

    # ── Bulk cancel helpers ────────────────────────────────────────────────

    async def _load_channel_name_map(self):
        """Return {channel_id_str: channel_name_lower} from channels.json"""
        channels_file = Path(__file__).resolve().parent.parent.parent / "config" / "channels.json"
        try:
            with open(channels_file) as f:
                channels_data = json.load(f)
            monitored = channels_data.get("monitored_channels", {})
            return {str(cid): name.lower() for name, cid in monitored.items()}
        except Exception as e:
            logger.warning(f"Could not load channels.json: {e}")
            return {}

    def _channel_category(self, channel_name: str) -> str:
        """Classify a channel name as 'tolls', 'pa', 'setups', or 'other'."""
        if "toll" in channel_name:
            return "tolls"
        if "pa" in channel_name or "price" in channel_name or "action" in channel_name:
            return "pa"
        return "setups"

    async def _get_active_signals_for_instrument(self, instrument: str):
        """Fetch all active/hit signals for an instrument (case-insensitive)."""
        from database import db

        async with db.get_connection() as conn:
            rows = await conn.fetch(
                """SELECT id, instrument, direction, channel_id
                   FROM signals
                   WHERE UPPER(instrument) = $1
                     AND status IN ('active', 'hit')""",
                instrument.upper(),
            )
        return [dict(r) for r in rows]

    async def _cancel_signal_ids(self, signals: list, reason: str) -> int:
        """
        Cancel a list of signals. Returns number successfully cancelled.

        `signals` may be a list of full signal dicts (with at least 'id',
        'message_id', 'channel_id', 'instrument', 'direction') or plain ints.
        Passing full dicts enables original-message reactions, monitor eviction,
        and embed pings. Plain ints fall back to embed-only behaviour.

        For toll signals:
          - If a persistent alert embed exists, update_embed_for_signal_id handles
            the embed update, archive-move, and original-message deletion.
          - If NO persistent alert embed exists, we delete the original signal
            message immediately (since there's nothing to archive).
        """
        if not signals:
            return 0

        monitor = self.services.monitor
        alert_system = monitor.alert_system if monitor else None

        count = 0
        for item in signals:
            # Support both plain IDs and full signal dicts
            if isinstance(item, dict):
                sid = item["id"]
                signal_dict = item
            else:
                sid = item
                signal_dict = None

            success = await self.signal_db.manually_set_signal_status(sid, "cancelled", reason)
            if not success:
                continue

            count += 1

            # 1. Evict from streaming monitor so price-checking stops immediately
            if monitor:
                monitor.sync_signal_status_in_memory(sid, "cancelled")
                monitor.active_signals.pop(sid, None)
                monitor.nm_monitor.evict_signal(sid)
                monitor.tp_monitor.evict_signal(sid)

            # 2. React to the original signal message
            if signal_dict and monitor:
                try:
                    from price_feeds.streaming_monitor import react_to_original_signal

                    await react_to_original_signal(self.bot, signal_dict, "❌")
                except Exception as _re:
                    logger.warning(f"Could not react to original message for signal {sid}: {_re}")

            # 3. Update the persistent alert embed with a ping so the role is notified.
            #    For toll signals WITH an embed: update_embed_for_signal_id will schedule
            #    the archive-move task, which also deletes the original signal message.
            #    For toll signals WITHOUT an embed: we delete the original message now.
            if alert_system:
                try:
                    ping_text = None
                    if signal_dict:
                        instrument = signal_dict.get("instrument", "")
                        direction = (signal_dict.get("direction") or "").upper()
                        ping_text = f"❌ **{instrument}** {direction} — signal cancelled"
                    embed_existed = await alert_system.update_embed_for_signal_id(
                        sid, "cancelled", ping_text=ping_text
                    )
                    # For signals with no persistent embed in auto-purge channels,
                    # delete the original signal message immediately (nothing to archive).
                    if not embed_existed and signal_dict:
                        try:
                            await alert_system._maybe_delete_original_message(signal_dict, sid)
                        except Exception as _td:
                            logger.warning(
                                f"Could not delete original message for signal {sid} (no embed): {_td}"
                            )
                except Exception as _ue:
                    logger.warning(
                        f"Could not update embed after bulk cancel for signal {sid}: {_ue}"
                    )

        return count

    async def _bulk_cancel_gold(self, ctx, direction_filter, channel_category):
        """
        Cancel active XAUUSD/GOLD signals filtered by direction and channel category.
        direction_filter: 'long' | 'short' | None (both)
        channel_category: 'setups' | 'pa' | 'tolls' | None (all)
        """
        loading = await ctx.send("🔄 Finding signals to cancel...")

        channel_map = await self._load_channel_name_map()

        # Fetch all active gold signals
        from database import db

        async with db.get_connection() as conn:
            rows = await conn.fetch(
                """SELECT id, instrument, direction, channel_id, message_id
                   FROM signals
                   WHERE UPPER(instrument) IN ('XAUUSD', 'GOLD')
                     AND status IN ('active', 'hit')"""
            )
        signals = [dict(r) for r in rows]

        # Filter by direction
        if direction_filter:
            signals = [s for s in signals if s["direction"].lower() == direction_filter]

        # Filter by channel category
        if channel_category:
            filtered = []
            for s in signals:
                ch_name = channel_map.get(str(s["channel_id"]), "")
                if self._channel_category(ch_name) == channel_category:
                    filtered.append(s)
            signals = filtered

        if not signals:
            dir_label = direction_filter.title() + "s" if direction_filter else "Long/Short"
            cat_label = channel_category.title() if channel_category else "All"
            await loading.edit(content=f"ℹ️ No active Gold {dir_label} {cat_label} signals found.")
            return

        cancelled = await self._cancel_signal_ids(signals, f"Bulk cancel by {ctx.author.name}")

        dir_label = direction_filter.title() + "s" if direction_filter else "Longs & Shorts"
        cat_label = channel_category.title() if channel_category else "All Categories"

        embed = discord.Embed(
            title="🚫 Bulk Cancel Complete",
            description=f"Cancelled **{cancelled}/{len(signals)}** Gold {dir_label} ({cat_label}) signals",
            color=0xFFA500,
        )
        embed.set_footer(text=f"Actioned by {ctx.author.name}")
        await loading.edit(content=None, embed=embed)

        # Notify finished-signals channel that all of this type were cancelled
        if cancelled > 0:
            alert_system = self.services.alert_system if self.services.monitor else None
            if alert_system:
                finished_channel = alert_system._get_finished_channel()
                if finished_channel:
                    try:
                        summary_embed = discord.Embed(
                            title="🚫 Mass Cancellation",
                            description=(
                                f"All **Gold {dir_label} ({cat_label})** signals have been cancelled.\n"
                                f"**{cancelled}** signal(s) cancelled by {ctx.author.name}."
                            ),
                            color=0xFF4500,
                        )
                        summary_embed.set_footer(text="Mass cancellation complete")
                        await finished_channel.send(embed=summary_embed)
                        logger.info(
                            f"Sent mass-cancel summary to finished-signals channel: "
                            f"Gold {dir_label} ({cat_label}), {cancelled} signals"
                        )
                    except Exception as _fe:
                        logger.warning(
                            f"Could not send mass-cancel summary to finished-signals: {_fe}"
                        )

    async def _bulk_cancel_by_target(self, ctx, target: str):
        """
        Cancel all active signals whose instrument contains `target`.
        Works for exact pairs (EURUSD) and currencies (EUR).
        """
        loading = await ctx.send(f"🔄 Finding signals for `{target}` to cancel...")

        from database import db

        async with db.get_connection() as conn:
            rows = await conn.fetch(
                """SELECT id, instrument, direction, channel_id, message_id
                   FROM signals
                   WHERE UPPER(instrument) LIKE $1
                     AND status IN ('active', 'hit')""",
                f"%{target.upper()}%",
            )
        signals = [dict(r) for r in rows]

        if not signals:
            await loading.edit(content=f"ℹ️ No active signals found matching `{target}`.")
            return

        cancelled = await self._cancel_signal_ids(signals, f"Bulk cancel by {ctx.author.name}")

        # Summarise by instrument
        instruments = {}
        for s in signals:
            instruments[s["instrument"]] = instruments.get(s["instrument"], 0) + 1

        embed = discord.Embed(
            title="🚫 Bulk Cancel Complete",
            description=f"Cancelled **{cancelled}/{len(signals)}** signals matching `{target}`",
            color=0xFFA500,
        )
        summary = "\n".join(
            f"• {instr}: {cnt} signal(s)" for instr, cnt in sorted(instruments.items())
        )
        embed.add_field(name="Instruments", value=summary or "—", inline=False)
        embed.set_footer(text=f"Actioned by {ctx.author.name}")
        await loading.edit(content=None, embed=embed)

        # Notify finished-signals channel that all of this type were cancelled
        if cancelled > 0:
            alert_system = self.services.alert_system if self.services.monitor else None
            if alert_system:
                finished_channel = alert_system._get_finished_channel()
                if finished_channel:
                    try:
                        instr_list = ", ".join(sorted(instruments.keys()))
                        summary_embed = discord.Embed(
                            title="🚫 Mass Cancellation",
                            description=(
                                f"All **{target}** signals have been cancelled.\n"
                                f"**{cancelled}** signal(s) cancelled by {ctx.author.name}.\n"
                                f"Instruments: {instr_list}"
                            ),
                            color=0xFF4500,
                        )
                        summary_embed.set_footer(text="Mass cancellation complete")
                        await finished_channel.send(embed=summary_embed)
                        logger.info(
                            f"Sent mass-cancel summary to finished-signals channel: "
                            f"{target}, {cancelled} signals"
                        )
                    except Exception as _fe:
                        logger.warning(
                            f"Could not send mass-cancel summary to finished-signals: {_fe}"
                        )

    # ── End bulk cancel helpers ────────────────────────────────────────────

    @commands.command(name="setexpiry", description="Set signal expiry")
    async def set_expiry(self, ctx: commands.Context, signal_id: int, expiry_type: str):
        """
        Set signal expiry
        Valid types: day_end, week_end, month_end, no_expiry
        """
        valid_types = ["day_end", "week_end", "month_end", "no_expiry"]

        if expiry_type.lower() not in valid_types:
            await ctx.send(f"❌ Invalid expiry type. Valid options: {', '.join(valid_types)}")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        success = await self.signal_db.manually_set_signal_expiry(signal_id, expiry_type.lower())

        if success:
            embed = discord.Embed(
                title="⏰ Expiry Updated",
                description=f"Signal #{signal_id} expiry set to **{expiry_type}**",
                color=0x00FF00,
            )
            embed.add_field(name="Instrument", value=signal["instrument"], inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update expiry")
