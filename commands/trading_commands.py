"""
Trading Commands — signal management + news mode, combined.
"""
from .base_command import BaseCog
from utils.logger import get_logger
from utils.formatting import (
    format_price, is_crypto_symbol,
    is_index_symbol, get_status_emoji
)
import asyncio
import discord
from discord.ext import commands
from typing import Optional, List, Dict
from datetime import datetime
from price_feeds.tp_config import TPConfig
from price_feeds.alert_config import AlertDistanceConfig
from price_feeds.nm_config import NMConfig
import pytz
from core.news_manager import (
    NewsManager,
    NewsEvent,
    FOREX_CURRENCIES,
    NAMED_CATEGORIES,
    parse_news_command,
    EST,
)

ASSET_CLASSES = ["forex", "forex_jpy", "metals", "indices", "stocks", "crypto", "oil"]
VALID_TP_TYPES = ["pips", "dollars"]

logger = get_logger("trading_commands")
EST = pytz.timezone('America/New_York')


class ActiveSignalsView(discord.ui.View):
    """Pagination view for active signals"""

    def __init__(self, signals: List[Dict], guild_id: int,
                 instrument: Optional[str], page_size: int = 10, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.signals = signals
        self.guild_id = guild_id
        self.instrument = instrument
        self.page_size = page_size
        self.current_page = 0
        self.max_page = (len(signals) - 1) // page_size if signals else 0
        self.update_buttons()

    def update_buttons(self):
        """Update button states based on current page"""
        self.previous_button.disabled = self.current_page <= 0
        self.next_button.disabled = self.current_page >= self.max_page
        self.page_label.label = f"Page {self.current_page + 1}/{self.max_page + 1}"

    def get_page_embed(self) -> discord.Embed:
        """Get embed for current page"""
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.signals))
        page_signals = self.signals[start_idx:end_idx]

        return self.create_active_signals_embed(
            page_signals,
            self.guild_id,
            self.instrument,
            page_info=(self.current_page + 1, self.max_page + 1, len(self.signals))
        )

    def create_active_signals_embed(self, signals: List[Dict], guild_id: int,
                                    instrument: Optional[str], page_info: tuple) -> discord.Embed:
        """Create embed for active signals with pagination info"""
        current_page, total_pages, total_signals = page_info

        if not signals and current_page == 1:
            return discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500
            )

        embed = discord.Embed(
            title="Active Signals",
            description=f"Showing page {current_page}/{total_pages} ({total_signals} total signals)" +
                        (f" for {instrument}" if instrument else ""),
            color=0x00BFFF
        )

        for signal in signals:
            status_emoji = get_status_emoji(signal.get('status', 'active'))

            # Format limits - show ALL limits
            pending_limits = signal.get('pending_limits', [])
            hit_limits = signal.get('hit_limits', [])

            if pending_limits:
                limits_str = ", ".join([format_price(p, signal['instrument']) for p in pending_limits])
            else:
                limits_str = "None pending"

            if hit_limits:
                limits_str += f" | {len(hit_limits)} hit"

            # Create link or label
            if str(signal['message_id']).startswith("manual_"):
                link_label = "Manual Entry"
            else:
                message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                link_label = f"{message_url}"

            # Build field value
            field_value = f"**Limits:** {limits_str}"

            # Add distance information if available
            if signal.get('distance_info') and signal.get('status', 'active').lower() in ['active', 'hit']:
                distance_info = signal['distance_info']
                is_crypto = signal.get('is_crypto', False)
                is_index = signal.get('is_index', False)

                if is_crypto or is_index:
                    distance_dollars = abs(distance_info.get('distance', 0))
                    if distance_dollars > 0 and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** ${distance_dollars:.2f} away"
                else:
                    formatted_distance = distance_info.get('formatted', '')
                    if formatted_distance and signal.get('status', 'active').upper() != "HIT":
                        field_value += f"\n**Distance:** {formatted_distance}"

            # Add expiry time
            if signal.get('time_remaining'):
                field_value += f"\n**Expiry:** {signal['time_remaining']}"

            # Add source
            field_value += f"\n**Source:** {link_label}"

            embed.add_field(
                name=f"{status_emoji} #{signal['id']} - {signal['instrument']} - {signal['direction'].upper()}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Total: {total_signals} signals | Use buttons to navigate")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary, custom_id="previous")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page"""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

    @discord.ui.button(label="Page 1/1", style=discord.ButtonStyle.secondary, custom_id="page_label", disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Page indicator (disabled button)"""
        pass

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page"""
        if self.current_page < self.max_page:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)


class TradingCommands(BaseCog):
    """Signal management and news mode commands"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()
        self.alert_dist_config = AlertDistanceConfig()
        self.nm_config = NMConfig()

    @commands.command(name="signal")
    async def add_signal(
            self,
            ctx: commands.Context,
            instrument: str,
            direction: str,
            entry: float,
            stop_loss: float,
            limit1: float,
            limit2: Optional[float] = None,
            limit3: Optional[float] = None,
            limit4: Optional[float] = None
    ):
        """Add a new signal manually"""
        limits = [limit1]
        if limit2:
            limits.append(limit2)
        if limit3:
            limits.append(limit3)
        if limit4:
            limits.append(limit4)

        signal_data = {
            'instrument': instrument.upper(),
            'direction': direction.lower(),
            'entry': entry,
            'stop_loss': stop_loss,
            'limits': limits
        }

        signal_id = await self.signal_db.save_signal(
            signal_data,
            ctx.guild.id,
            ctx.channel.id,
            f"manual_{ctx.author.id}_{int(datetime.utcnow().timestamp())}"
        )

        embed = discord.Embed(
            title="✅ Signal Added",
            description=f"Signal #{signal_id} created successfully",
            color=0x00FF00
        )
        embed.add_field(name="Instrument", value=instrument.upper(), inline=True)
        embed.add_field(name="Direction", value=direction.upper(), inline=True)
        embed.add_field(name="Entry", value=str(entry), inline=True)
        embed.add_field(name="Stop Loss", value=str(stop_loss), inline=True)
        embed.add_field(name="Limits", value=", ".join(map(str, limits)), inline=False)

        await ctx.send(embed=embed)

    @commands.command(name='active')
    async def active_signals(
            self,
            ctx: commands.Context,
            *,
            args: str = None
    ):
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
        sort_method = 'recent'  # default

        if args:
            args_parts = args.split()
            for part in args_parts:
                if part.startswith('sort:'):
                    sort_method = part.split(':', 1)[1].lower()
                else:
                    # Assume it's an instrument filter
                    instrument = part.upper()

        # Validate sort method
        valid_sorts = ['recent', 'oldest', 'distance', 'progress']
        if sort_method not in valid_sorts:
            await loading_msg.edit(content=f"❌ Invalid sort method. Valid options: {', '.join(valid_sorts)}")
            return

        signals = await self.signal_db.get_active_signals_detailed(
            instrument if instrument else None
        )

        if not signals:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=0xFFA500
            )
            await loading_msg.edit(content=None, embed=embed)
            return

        # Add asset type flags and calculate distances
        for signal in signals:
            signal['is_crypto'] = is_crypto_symbol(signal['instrument'])
            signal['is_index'] = is_index_symbol(signal['instrument'])

            # Calculate distance to next limit
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                if hasattr(self.bot.monitor, 'stream_manager') and signal.get('pending_limits'):
                    try:
                        from price_feeds.alert_config import AlertDistanceConfig
                        alert_config = AlertDistanceConfig()

                        symbol = signal['instrument']
                        cached_price = await self.bot.monitor.stream_manager.get_latest_price(symbol)

                        if cached_price:
                            direction = signal['direction'].lower()
                            current_price = cached_price['ask'] if direction == 'long' else cached_price['bid']
                            limit_price = signal['pending_limits'][0]

                            # Calculate raw price distance
                            if direction == 'long':
                                distance = current_price - limit_price
                            else:
                                distance = limit_price - current_price

                            # Format based on asset type
                            if signal['is_crypto'] or signal['is_index']:
                                # For crypto and indices, show dollar distance
                                distance_value = abs(distance)
                                formatted = f"${distance_value:.2f} away"
                            else:
                                # For forex, use the format_distance_for_display which handles pip conversion
                                formatted = alert_config.format_distance_for_display(symbol, abs(distance),
                                                                                     current_price)
                                # Extract pip value for sorting
                                pip_size = alert_config.get_pip_size(symbol)
                                distance_value = abs(distance) / pip_size

                            signal['distance_info'] = {
                                'distance': distance_value,
                                'current_price': current_price,
                                'formatted': formatted
                            }
                    except Exception as e:
                        logger.warning(f"Could not get price for {symbol}: {e}")

        # Apply sorting
        if sort_method == 'recent':
            # Already sorted by created_at DESC from database
            pass
        elif sort_method == 'oldest':
            signals.reverse()
        elif sort_method == 'distance':
            # Sort by distance (closest first)
            def get_distance_key(signal):
                if signal.get('distance_info'):
                    return signal['distance_info']['distance']
                return float('inf')  # Put signals without distance at the end

            signals.sort(key=get_distance_key)
        elif sort_method == 'progress':
            # Sort by number of limits hit (most progress first)
            signals.sort(key=lambda s: len(s.get('hit_limits', [])), reverse=True)

        # Create pagination view
        view = ActiveSignalsView(
            signals=signals,
            guild_id=ctx.guild.id,
            instrument=instrument
        )

        # Get initial embed
        embed = view.get_page_embed()

        # Add sort info to footer
        sort_descriptions = {
            'recent': 'Most Recent First',
            'oldest': 'Oldest First',
            'distance': 'Closest to Limit',
            'progress': 'Most Progress'
        }

        current_footer = embed.footer.text if embed.footer else ""
        sort_info = f" | Sorted by: {sort_descriptions.get(sort_method, sort_method.title())}"
        embed.set_footer(text=current_footer + sort_info)

        await loading_msg.edit(content=None, embed=embed, view=view)

    @commands.command(name="delete")
    async def delete_signal(self, ctx: commands.Context, signal_id: int):
        """Delete a signal permanently"""
        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        from database import db
        async with db.get_connection() as conn:
            await conn.execute("DELETE FROM signals WHERE id = $1", signal_id)

        embed = discord.Embed(
            title="🗑️ Signal Deleted",
            description=f"Signal #{signal_id} has been deleted",
            color=0xFFA500
        )
        embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
        embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
        embed.add_field(name="Status", value=signal['status'], inline=True)
        embed.set_footer(text=f"Deleted by {ctx.author.name}")

        await ctx.send(embed=embed)

    @commands.command(name="info")
    async def signal_info(self, ctx: commands.Context, signal_id: int):
        """Show detailed information about a signal"""
        signal = await self.signal_db.get_signal_with_limits(signal_id)

        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        status_emoji = get_status_emoji(signal['status'])

        embed = discord.Embed(
            title=f"{status_emoji} Signal #{signal_id} - {signal['instrument']}",
            color=0x00BFFF
        )

        embed.add_field(name="Direction", value=signal['direction'].upper(), inline=True)
        embed.add_field(name="Status", value=signal['status'].upper(), inline=True)
        embed.add_field(name="Type", value="⚡ Scalp" if signal.get('scalp') else "📊 Setup", inline=True)

        stop_loss_formatted = format_price(signal['stop_loss'], signal['instrument']) if signal['stop_loss'] else "N/A"
        embed.add_field(name="Stop Loss", value=stop_loss_formatted, inline=True)

        # Streaming status
        if hasattr(self.bot, 'monitor') and self.bot.monitor:
            if hasattr(self.bot.monitor, 'stream_manager'):
                is_subscribed = signal['instrument'] in self.bot.monitor.stream_manager.subscribed_symbols
                embed.add_field(
                    name="Streaming Status",
                    value="🟢 Subscribed" if is_subscribed else "⚪ Not Subscribed",
                    inline=True
                )

        # Limits info
        if signal['limits']:
            pending_limits = [l for l in signal['limits'] if l['status'] == 'pending']
            hit_limits = [l for l in signal['limits'] if l['status'] == 'hit']

            if pending_limits:
                pending_str = "\n".join([f"• {format_price(l['price_level'], signal['instrument'])}"
                                         for l in pending_limits[:5]])
                if len(pending_limits) > 5:
                    pending_str += f"\n... +{len(pending_limits) - 5} more"
                embed.add_field(name=f"Pending Limits ({len(pending_limits)})", value=pending_str, inline=False)

            if hit_limits:
                hit_str = "\n".join([f"• {format_price(l['price_level'], signal['instrument'])} ✅"
                                     for l in hit_limits[:5]])
                if len(hit_limits) > 5:
                    hit_str += f"\n... +{len(hit_limits) - 5} more"
                embed.add_field(name=f"Hit Limits ({len(hit_limits)})", value=hit_str, inline=False)

        # Progress
        embed.add_field(
            name="Progress",
            value=f"{signal.get('limits_hit', 0)}/{signal.get('total_limits', 0)} limits hit",
            inline=True
        )

        # Timestamps
        if signal.get('first_limit_hit_time'):
            try:
                timestamp = signal['first_limit_hit_time']
                if isinstance(timestamp, str):
                    # Parse ISO format string
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                embed.add_field(name="First Hit", value=f"<t:{int(timestamp.timestamp())}:R>", inline=True)
            except:
                pass

        if signal.get('closed_at'):
            try:
                timestamp = signal['closed_at']
                if isinstance(timestamp, str):
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                embed.add_field(name="Closed", value=f"<t:{int(timestamp.timestamp())}:R>", inline=True)
            except:
                pass

        # Link to original message
        if not str(signal['message_id']).startswith("manual_"):
            message_url = f"https://discord.com/channels/{ctx.guild.id}/{signal['channel_id']}/{signal['message_id']}"
            embed.add_field(name="Source", value=f"[Jump to message]({message_url})", inline=False)
        else:
            embed.add_field(name="Source", value="Manual Entry", inline=False)

        created_at = signal['created_at']
        if hasattr(created_at, 'strftime'):
            created_at = created_at.strftime('%Y-%m-%d %H:%M UTC')
        embed.set_footer(text=f"Created {created_at}")

        await ctx.send(embed=embed)

    @commands.command(name="setstatus", description="Set signal status")
    async def set_signal_status(self, ctx: commands.Context, signal_id: int, status: str):
        """Manually set a signal's status"""
        valid_statuses = ['active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled', 'cancel']
        status = status.lower()

        if status == "cancel":
            status = "cancelled"

        if status not in valid_statuses:
            await ctx.send(f"❌ Invalid status. Valid options: {', '.join(valid_statuses)}")
            return

        signal = await self.signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        # Calculate result_pips for profit and stop_loss
        result_pips = None
        if status == 'profit':
            # If signal is approaching (no limits hit yet), mirror !hit behaviour:
            # mark the first pending limit as hit before setting status to profit.
            current_hit_count = len(signal.get('hit_limits') or [])
            if current_hit_count == 0:
                pending_limits = signal.get('pending_limits') or []
                if pending_limits:
                    # Sort by sequence_number and hit the first one
                    sorted_pending = sorted(pending_limits, key=lambda l: l.get('sequence_number', 999))
                    first_limit = sorted_pending[0]
                    try:
                        from database import db as _db
                        await _db.mark_limit_hit(first_limit['id'], first_limit['price_level'])
                        logger.info(
                            f"Auto-hit limit #{first_limit.get('sequence_number')} "
                            f"for signal {signal_id} as part of manual profit (approaching→profit)"
                        )
                        # Refresh signal so hit_limits count is correct for result_pips calc
                        signal = await self.signal_db.get_signal_with_limits(signal_id) or signal
                    except Exception as _hit_err:
                        logger.warning(f"Could not auto-hit limit for signal {signal_id} on profit: {_hit_err}")

            # Use the TP threshold from config as the recorded result
            result_pips = self.tp_config.get_tp_value(signal['instrument'], scalp=signal.get('scalp', False))
        elif status == 'stop_loss':
            # Sum P&L of all hit limits using stop_loss price as the exit price
            try:
                hit_limits = await self.signal_db.get_hit_limits_for_signal(signal_id)
                stop_price = signal.get('stop_loss')
                if hit_limits and stop_price:
                    combined = 0.0
                    for lim in hit_limits:
                        entry = lim.get('hit_price') or lim.get('price_level')
                        if entry is not None:
                            combined += self.tp_config.calculate_pnl(
                                signal['instrument'], signal['direction'], entry, stop_price,
                                scalp=signal.get('scalp', False)
                            )
                    result_pips = combined
            except Exception as e:
                logger.warning(f"Could not calculate SL result_pips for signal {signal_id}: {e}")

        success = await self.signal_db.manually_set_signal_status(
            signal_id,
            status,
            f"Manual override by {ctx.author.name}",
            result_pips=result_pips,
        )

        if success:
            status_emoji = get_status_emoji(status)

            embed = discord.Embed(
                title=f"{status_emoji} Status Updated",
                description=f"Signal #{signal_id} status changed to **{status.upper()}**",
                color=0x00FF00
            )
            embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
            embed.add_field(name="Previous Status", value=signal['status'], inline=True)
            embed.set_footer(text=f"Changed by {ctx.author.name}")

            await ctx.send(embed=embed)

            # Update the persistent alert embed
            status_to_event = {
                'profit': 'profit', 'breakeven': 'breakeven',
                'stop_loss': 'stop_loss', 'cancelled': 'cancelled',
                'active': 'reactivated', 'hit': 'hit',
            }
            embed_event = status_to_event.get(status)
            if embed_event:
                try:
                    alert_system = (
                        self.bot.monitor.alert_system
                        if hasattr(self.bot, 'monitor') and self.bot.monitor else None
                    )
                    if alert_system:
                        await alert_system.update_embed_for_signal_id(
                            signal_id, embed_event,
                        )
                except Exception as _ue:
                    logger.warning(f"Could not update embed after setstatus: {_ue}")

            # If reactivating, mark signal as NM-immune so the monitor can't re-fire.
            # The existing embed is edited in place by reactivate_embed (no duplicate sent).
            if status == 'active':
                try:
                    if hasattr(self.bot, 'monitor') and self.bot.monitor:
                        if hasattr(self.bot.monitor, 'nm_monitor'):
                            self.bot.monitor.nm_monitor.mark_immune(signal_id)
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
                timeout=5.0
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
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                monitor = self.bot.monitor
                await monitor.tp_monitor.refresh_hit_limits(signal_id)
                if signal_id in monitor.active_signals:
                    monitor.active_signals[signal_id]['status'] = 'hit'
            await ctx.send(f"✅ Signal {signal_id} marked as HIT (limit 1 hit, auto-TP active)")

            # Update the persistent alert embed
            try:
                alert_system = (
                    self.bot.monitor.alert_system
                    if hasattr(self.bot, 'monitor') and self.bot.monitor else None
                )
                if alert_system:
                    await alert_system.update_embed_for_signal_id(signal_id, 'hit')
            except Exception as _ue:
                logger.warning(f"Could not update embed after !hit: {_ue}")
        else:
            # Either already HIT or not in a valid state
            signal = await self.signal_db.get_signal_with_limits(signal_id)
            if signal and signal.get('status') == 'hit':
                await ctx.send(f"ℹ️ Signal {signal_id} is already HIT — auto-TP is already active.")
            else:
                await ctx.send(f"❌ Could not mark signal {signal_id} as HIT. Signal must be in ACTIVE status.")

    @commands.command(name="stoploss", aliases=["sl"], description="Mark signal as stop loss")
    async def set_stop_loss(self, ctx: commands.Context, signal_id: int):
        await self.set_signal_status(ctx, signal_id, "stop_loss")

    @commands.command(name="cancel", aliases=["nm"], description="Cancel a signal or bulk cancel signals")
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
            await ctx.send("❌ Usage: `!cancel <id>` or `!cancel gold longs/shorts/both <type>` or `!cancel all <PAIR/CURRENCY>`\nSee `!help cancel` for full details.")
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
            # tokens[0] = "gold", tokens[1] = direction, tokens[2] = type
            if len(tokens) < 3:
                await ctx.send("❌ Usage: `!cancel gold <longs|shorts|both> <setups|pa|tolls|everything>`\nSee `!help cancel` for details.")
                return

            direction_token = tokens[1]
            type_token = tokens[2]

            if direction_token not in ("longs", "shorts", "both"):
                await ctx.send("❌ Direction must be `longs`, `shorts`, or `both`.")
                return

            if type_token not in ("setups", "pa", "priceaction", "price_action", "tolls", "everything"):
                await ctx.send("❌ Type must be `setups`, `pa`, `tolls`, or `everything`.")
                return

            # Map direction
            direction_filter = None if direction_token == "both" else direction_token.rstrip("s")  # longs->long, shorts->short

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
        import json
        from pathlib import Path
        channels_file = Path(__file__).resolve().parent.parent / 'config' / 'channels.json'
        try:
            with open(channels_file, 'r') as f:
                channels_data = json.load(f)
            monitored = channels_data.get('monitored_channels', {})
            return {str(cid): name.lower() for name, cid in monitored.items()}
        except Exception as e:
            logger.warning(f"Could not load channels.json: {e}")
            return {}

    def _channel_category(self, channel_name: str) -> str:
        """Classify a channel name as 'tolls', 'pa', 'setups', or 'other'."""
        if 'toll' in channel_name:
            return 'tolls'
        if 'pa' in channel_name or 'price' in channel_name or 'action' in channel_name:
            return 'pa'
        return 'setups'

    async def _get_active_signals_for_instrument(self, instrument: str):
        """Fetch all active/hit signals for an instrument (case-insensitive)."""
        from database import db
        async with db.get_connection() as conn:
            rows = await conn.fetch(
                """SELECT id, instrument, direction, channel_id
                   FROM signals
                   WHERE UPPER(instrument) = $1
                     AND status IN ('active', 'hit')""",
                instrument.upper()
            )
        return [dict(r) for r in rows]

    async def _cancel_signal_ids(self, signals: list, reason: str) -> int:
        """
        Cancel a list of signals. Returns number successfully cancelled.

        `signals` may be a list of full signal dicts (with at least 'id',
        'message_id', 'channel_id', 'instrument', 'direction') or plain ints.
        Passing full dicts enables original-message reactions, monitor eviction,
        and embed pings. Plain ints fall back to embed-only behaviour.
        """
        if not signals:
            return 0

        monitor = self.bot.monitor if hasattr(self.bot, 'monitor') and self.bot.monitor else None
        alert_system = monitor.alert_system if monitor else None

        count = 0
        for item in signals:
            # Support both plain IDs and full signal dicts
            if isinstance(item, dict):
                sid = item['id']
                signal_dict = item
            else:
                sid = item
                signal_dict = None

            success = await self.signal_db.manually_set_signal_status(
                sid, 'cancelled', reason
            )
            if not success:
                continue

            count += 1

            # 1. Evict from streaming monitor so price-checking stops immediately
            if monitor:
                monitor.active_signals.pop(sid, None)
                if hasattr(monitor, 'nm_monitor'):
                    monitor.nm_monitor.evict_signal(sid)
                if hasattr(monitor, 'tp_monitor'):
                    monitor.tp_monitor.evict_signal(sid)

            # 2. React to the original signal message
            if signal_dict and monitor:
                try:
                    await monitor._react_to_original_signal(signal_dict, "\u274c")
                except Exception as _re:
                    logger.warning(f"Could not react to original message for signal {sid}: {_re}")

            # 3. Update the persistent alert embed with a ping so the role is notified
            if alert_system:
                try:
                    ping_text = None
                    if signal_dict:
                        instrument = signal_dict.get('instrument', '')
                        direction = (signal_dict.get('direction') or '').upper()
                        ping_text = f"\u274c **{instrument}** {direction} \u2014 signal cancelled"
                    await alert_system.update_embed_for_signal_id(
                        sid, 'cancelled', ping_text=ping_text
                    )
                except Exception as _ue:
                    logger.warning(f"Could not update embed after bulk cancel for signal {sid}: {_ue}")

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
            signals = [s for s in signals if s['direction'].lower() == direction_filter]

        # Filter by channel category
        if channel_category:
            filtered = []
            for s in signals:
                ch_name = channel_map.get(str(s['channel_id']), '')
                if self._channel_category(ch_name) == channel_category:
                    filtered.append(s)
            signals = filtered

        if not signals:
            dir_label = direction_filter.title() + "s" if direction_filter else "Long/Short"
            cat_label = channel_category.title() if channel_category else "All"
            await loading.edit(content=f"ℹ️ No active Gold {dir_label} {cat_label} signals found.")
            return

        cancelled = await self._cancel_signal_ids(
            signals, f"Bulk cancel by {ctx.author.name}"
        )

        dir_label = direction_filter.title() + "s" if direction_filter else "Longs & Shorts"
        cat_label = channel_category.title() if channel_category else "All Categories"

        embed = discord.Embed(
            title="🚫 Bulk Cancel Complete",
            description=f"Cancelled **{cancelled}/{len(signals)}** Gold {dir_label} ({cat_label}) signals",
            color=0xFFA500
        )
        embed.set_footer(text=f"Actioned by {ctx.author.name}")
        await loading.edit(content=None, embed=embed)

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
                f"%{target.upper()}%"
            )
        signals = [dict(r) for r in rows]

        if not signals:
            await loading.edit(content=f"ℹ️ No active signals found matching `{target}`.")
            return

        cancelled = await self._cancel_signal_ids(
            signals, f"Bulk cancel by {ctx.author.name}"
        )

        # Summarise by instrument
        instruments = {}
        for s in signals:
            instruments[s['instrument']] = instruments.get(s['instrument'], 0) + 1

        embed = discord.Embed(
            title="🚫 Bulk Cancel Complete",
            description=f"Cancelled **{cancelled}/{len(signals)}** signals matching `{target}`",
            color=0xFFA500
        )
        summary = "\n".join(f"• {instr}: {cnt} signal(s)" for instr, cnt in sorted(instruments.items()))
        embed.add_field(name="Instruments", value=summary or "—", inline=False)
        embed.set_footer(text=f"Actioned by {ctx.author.name}")
        await loading.edit(content=None, embed=embed)

    # ── End bulk cancel helpers ────────────────────────────────────────────

    @commands.command(name="setexpiry", description="Set signal expiry")
    async def set_expiry(self, ctx: commands.Context, signal_id: int, expiry_type: str):
        """
        Set signal expiry
        Valid types: day_end, week_end, month_end, no_expiry
        """
        from database import signal_db

        valid_types = ['day_end', 'week_end', 'month_end', 'no_expiry']

        if expiry_type.lower() not in valid_types:
            await ctx.send(f"❌ Invalid expiry type. Valid options: {', '.join(valid_types)}")
            return

        signal = await signal_db.get_signal_with_limits(signal_id)
        if not signal:
            await ctx.send(f"❌ Signal #{signal_id} not found")
            return

        success = await signal_db.set_signal_expiry(signal_id, expiry_type.lower())

        if success:
            embed = discord.Embed(
                title="⏰ Expiry Updated",
                description=f"Signal #{signal_id} expiry set to **{expiry_type}**",
                color=0x00FF00
            )
            embed.add_field(name="Instrument", value=signal['instrument'], inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to update expiry")

    @commands.command(name="report", description="Generate trading report")
    async def generate_report(
            self,
            ctx: commands.Context,
            period: str = "week",
            filter_type: str = None
    ):
        """
        Generate a trading report for specified period

        Args:
            period: 'day', 'week', or 'month'
            filter_type: Optional - 'stoploss', 'sl', 'profit', 'win' to filter results
        """
        if period.lower() not in ['day', 'week', 'month']:
            await ctx.send("❌ Period must be 'day', 'week', or 'month'")
            return

        # Normalize filter type
        filter_normalized = None
        if filter_type:
            filter_lower = filter_type.lower()
            if filter_lower in ['stoploss', 'sl', 'stop', 'stop_loss']:
                filter_normalized = 'stoploss'
            elif filter_lower in ['profit', 'win', 'tp']:
                filter_normalized = 'profit'
            else:
                await ctx.send("❌ Filter must be 'stoploss'/'sl' or 'profit'/'win'")
                return

        # Update loading message based on filter
        if filter_normalized:
            loading_msg = await ctx.send(f"📊 Generating {period} report ({filter_normalized} only)...")
        else:
            loading_msg = await ctx.send(f"📊 Generating {period} report...")

        def cap_field_value(lines: list, max_length: int = 1024) -> str:
            """
            Cap field value to max_length by truncating lines and adding summary.
            Discord embed fields have a 1024 character limit.
            """
            if not lines:
                return ""

            result_lines = []
            current_length = 0
            omitted_count = 0

            for line in lines:
                line_length = len(line) + 1  # +1 for newline
                if current_length + line_length > max_length - 50:  # Reserve 50 chars for "... +X more"
                    omitted_count = len(lines) - len(result_lines)
                    break
                result_lines.append(line)
                current_length += line_length

            result = "\n".join(result_lines)
            if omitted_count > 0:
                result += f"\n... +{omitted_count} more signal{'s' if omitted_count > 1 else ''}"

            return result

        try:
            date_range = await self.signal_db.get_trading_period_range(period)
            start_date = date_range['start']
            end_date = date_range['end']

            signals = await self.signal_db.get_period_signals_with_results(
                start_date,
                end_date
            )

            if not signals:
                embed = discord.Embed(
                    title=f"📊 {period.title()} Trading Report",
                    description=f"No signals found for the current {period}",
                    color=0xFFA500
                )
                await loading_msg.edit(content=None, embed=embed)
                return

            # Fetch full signal details with limits for each signal
            enriched_signals = []
            for signal in signals:
                full_signal = await self.signal_db.get_signal_with_limits(signal['id'])
                if full_signal:
                    # Merge the status and other info from period query
                    full_signal['status'] = signal['status']
                    full_signal['channel_id'] = signal['channel_id']
                    enriched_signals.append(full_signal)

            signals = enriched_signals

            # Load channels.json directly
            import json
            from pathlib import Path

            channels_file = Path(__file__).resolve().parent.parent / 'config' / 'channels.json'
            try:
                with open(channels_file, 'r') as f:
                    channels_data = json.load(f)
                monitored_channels = channels_data.get('monitored_channels', {})
            except Exception as e:
                logger.warning(f"Could not load channels.json: {e}")
                monitored_channels = {}
                channels_data = {}

            # Create reverse mapping: channel_id -> channel_name
            channel_id_to_name = {str(channel_id): name for name, channel_id in monitored_channels.items()}

            # Separate signals into PA, toll, and regular based on channel
            pa_signals = []
            toll_signals = []
            regular_signals = []

            for signal in signals:
                channel_id = str(signal.get('channel_id', ''))
                channel_name = channel_id_to_name.get(channel_id, '').lower()

                if 'toll' in channel_name:
                    toll_signals.append(signal)
                elif any(x in channel_name for x in ['pa', 'price-action']):
                    pa_signals.append(signal)
                else:
                    regular_signals.append(signal)

            # Process regular signals
            regular_profit = [s for s in regular_signals if s.get('status', '').lower() == 'profit']
            regular_stoploss = [s for s in regular_signals if s.get('status', '').lower() in ['stoploss', 'stop_loss']]

            # Process PA signals
            pa_profit = [s for s in pa_signals if s.get('status', '').lower() == 'profit']
            pa_stoploss = [s for s in pa_signals if s.get('status', '').lower() in ['stoploss', 'stop_loss']]

            # Process toll signals
            toll_profit = [s for s in toll_signals if s.get('status', '').lower() == 'profit']
            toll_stoploss = [s for s in toll_signals if s.get('status', '').lower() in ['stoploss', 'stop_loss']]

            # Apply filter if specified
            if filter_normalized == 'stoploss':
                regular_profit = []
                pa_profit = []
                toll_profit = []
            elif filter_normalized == 'profit':
                regular_stoploss = []
                pa_stoploss = []
                toll_stoploss = []

            # Check if filter resulted in no signals
            if filter_normalized:
                filtered_count = (len(regular_profit) + len(regular_stoploss) +
                                  len(pa_profit) + len(pa_stoploss) +
                                  len(toll_profit) + len(toll_stoploss))
                if filtered_count == 0:
                    filter_label = "stop loss" if filter_normalized == 'stoploss' else "profit"
                    embed = discord.Embed(
                        title=f"📊 {period.title()} Trading Report - {filter_label.title()} Only",
                        description=f"No {filter_label} signals found for the current {period}",
                        color=0xFFA500
                    )
                    await loading_msg.edit(content=None, embed=embed)
                    return

            # Calculate overall statistics
            total_regular = len(
                [s for s in regular_signals if s.get('status', '').lower() in ['profit', 'stoploss', 'stop_loss']])
            total_pa = len(
                [s for s in pa_signals if s.get('status', '').lower() in ['profit', 'stoploss', 'stop_loss']])
            total_tolls = len(
                [s for s in toll_signals if s.get('status', '').lower() in ['profit', 'stoploss', 'stop_loss']])
            total_signals = total_regular + total_pa + total_tolls

            regular_profit_count = len(regular_profit)
            regular_sl_count = len(regular_stoploss)
            pa_profit_count = len(pa_profit)
            pa_sl_count = len(pa_stoploss)
            toll_profit_count = len(toll_profit)
            toll_sl_count = len(toll_stoploss)

            total_profit = regular_profit_count + pa_profit_count + toll_profit_count
            total_sl = regular_sl_count + pa_sl_count + toll_sl_count

            # Calculate win rates
            regular_win_rate = (regular_profit_count / total_regular * 100) if total_regular > 0 else 0
            pa_win_rate = (pa_profit_count / total_pa * 100) if total_pa > 0 else 0
            toll_win_rate = (toll_profit_count / total_tolls * 100) if total_tolls > 0 else 0
            overall_win_rate = (total_profit / total_signals * 100) if total_signals > 0 else 0

            # Create embed
            title_suffix = ""
            description_suffix = ""
            if filter_normalized == 'stoploss':
                title_suffix = " - Stop Losses Only"
                description_suffix = " (stop loss signals only)"
            elif filter_normalized == 'profit':
                title_suffix = " - Profits Only"
                description_suffix = " (profit signals only)"

            embed = discord.Embed(
                title=f"📊 {period.title()} Trading Report{title_suffix}",
                description=f"Date: {date_range['display_start']} - {date_range['display_end']}",
                color=0x00FF00 if overall_win_rate >= 50 else 0xFF0000
            )

            # Regular Signals Section
            if total_regular > 0:
                embed.add_field(
                    name="Regular Signals",
                    value=f"Total: {total_regular} | Win Rate: {regular_win_rate:.1f}%\n"
                          f"Profit: {regular_profit_count} | Stop Loss: {regular_sl_count}",
                    inline=True
                )

            # PA Signals Section
            if total_pa > 0:
                embed.add_field(
                    name="PA Signals",
                    value=f"Total: {total_pa} | Win Rate: {pa_win_rate:.1f}%\n"
                          f"Profit: {pa_profit_count} | Stop Loss: {pa_sl_count}",
                    inline=True
                )

            # Tolls Signals Section
            if total_tolls > 0:
                embed.add_field(
                    name="Tolls Signals",
                    value=f"Total: {total_tolls} | Win Rate: {toll_win_rate:.1f}%\n"
                          f"Profit: {toll_profit_count} | Stop Loss: {toll_sl_count}",
                    inline=True
                )

            # Build REGULAR TRADES section (profit first, then stop loss)
            if total_regular > 0:
                trade_lines = []

                for signal in regular_profit:
                    limits = signal.get('limits', [])
                    if limits:
                        first_limit = format_price(limits[0]['price_level'], signal['instrument'])
                        limit_display = f"{first_limit}, +{len(limits) - 1} more" if len(limits) > 1 else first_limit
                    else:
                        limit_display = "N/A"
                    trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in regular_stoploss:
                    sl_value = format_price(signal.get('stop_loss'), signal['instrument']) if signal.get(
                        'stop_loss') else "N/A"
                    trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if trade_lines:
                    embed.add_field(
                        name=f"Regular Trades ({total_regular})",
                        value=cap_field_value(trade_lines),
                        inline=False
                    )

            # Build PA TRADES section (profit first, then stop loss)
            if total_pa > 0:
                pa_trade_lines = []

                for signal in pa_profit:
                    limits = signal.get('limits', [])
                    if limits:
                        first_limit = format_price(limits[0]['price_level'], signal['instrument'])
                        limit_display = f"{first_limit}, +{len(limits) - 1} more" if len(limits) > 1 else first_limit
                    else:
                        limit_display = "N/A"
                    pa_trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in pa_stoploss:
                    sl_value = format_price(signal.get('stop_loss'), signal['instrument']) if signal.get(
                        'stop_loss') else "N/A"
                    pa_trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if pa_trade_lines:
                    embed.add_field(
                        name=f"PA Trades ({total_pa})",
                        value=cap_field_value(pa_trade_lines),
                        inline=False
                    )

            # Build TOLLS TRADES section (profit first, then stop loss)
            if total_tolls > 0:
                toll_trade_lines = []

                for signal in toll_profit:
                    limits = signal.get('limits', [])
                    if limits:
                        first_limit = format_price(limits[0]['price_level'], signal['instrument'])
                        limit_display = f"{first_limit}, +{len(limits) - 1} more" if len(limits) > 1 else first_limit
                    else:
                        limit_display = "N/A"
                    toll_trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | {limit_display} | {signal['direction'].upper()} 🟢"
                    )

                for signal in toll_stoploss:
                    sl_value = format_price(signal.get('stop_loss'), signal['instrument']) if signal.get(
                        'stop_loss') else "N/A"
                    toll_trade_lines.append(
                        f"#{signal['id']} | {signal['instrument']} | SL: {sl_value} | {signal['direction'].upper()} 🛑"
                    )

                if toll_trade_lines:
                    embed.add_field(
                        name=f"Tolls Trades ({total_tolls})",
                        value=cap_field_value(toll_trade_lines),
                        inline=False
                    )

            # Add live proof link from profit_channel
            profit_channel_id = channels_data.get('profit_channel')
            if profit_channel_id:
                profit_channel_url = f"https://discord.com/channels/{ctx.guild.id}/{profit_channel_id}"
                embed.add_field(
                    name="Live Proof",
                    value=f"{profit_channel_url}",
                    inline=False
                )

            embed.set_footer(
                text=f"Report generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
            )

            await loading_msg.edit(content=None, embed=embed)

        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error Generating Report",
                description=f"An error occurred: {str(e)}",
                color=0xFF0000
            )
            await loading_msg.edit(content=None, embed=error_embed)
            logger.error(f"Error in report command: {e}")


    # ── News mode commands ─────────────────────────────────────────────────

    @commands.command(
        name='news',
        description='Schedule a news window that auto-cancels signals when hit',
    )
    async def news(self, ctx: commands.Context, *, args: str = None):
        """
        Schedule a news window, or use special subcommands.

        Usage:
            !news <category> <time> [window] [tz:<tz>] [date:<date>]
            !news now [category]   → open-ended window active immediately (default: all)
            !news off              → deactivate all 'now' windows

        Tags (optional, add in any order):
            tz:<timezone>  — timezone for the time, e.g. tz:UTC  tz:EST  tz:London  (default: EST)
            date:<date>    — specific date, e.g. date:2025-06-15  date:06/15  date:tomorrow

        Examples:
            !news USD 12:30pm 15
            !news gold 8:30am tz:UTC
            !news all 14:00 30 date:2025-06-20
            !news JPY 9:30am date:tomorrow tz:CET
            !news now
            !news now USD
            !news off
        """
        if not args:
            await ctx.send(
                "❌ Usage: `!news <category> <time> [window] [tz:<tz>] [date:<date>]`\n"
                "Or: `!news now [category]` / `!news off`\n"
                "Categories: any currency code (USD, EUR, GBP…), `gold`, `oil`, `btc`, `crypto`, or `all`\n"
                "Timezone tag example: `tz:UTC`  `tz:EST`  `tz:London`  `tz:CET`\n"
                "Date tag example: `date:2025-06-15`  `date:06/15`  `date:tomorrow`"
            )
            return

        tokens = args.strip().split()
        subcommand = tokens[0].lower()

        # ── !news off ──────────────────────────────────────────────────────
        if subcommand == 'off':
            news_manager: NewsManager = self.bot.news_manager
            removed_events = news_manager.remove_now_events()
            if removed_events:
                await ctx.send(f"✅ Deactivated {len(removed_events)} open-ended news window(s).")
                alert_system = getattr(self.bot.monitor, 'alert_system', None)
                if alert_system:
                    for event in removed_events:
                        try:
                            await alert_system.send_news_ended_alert(event)
                        except Exception as e:
                            logger.warning(f"Failed to send news ended alert for event #{event.event_id}: {e}")
            else:
                await ctx.send("ℹ️ No open-ended news windows were active.")
            return

        # ── !news now [category] [N minutes] ──────────────────────────────
        if subcommand == 'now':
            import datetime as _dt
            import re as _re
            rest_tokens = tokens[1:]
            category = 'ALL'
            duration_minutes = None

            if rest_tokens:
                # Strip optional trailing duration: "5 minutes", "5m", "5 min", bare "5"
                if len(rest_tokens) >= 2:
                    last = rest_tokens[-1].lower()
                    if last in ('minutes', 'mins', 'min', 'm', 'minute'):
                        try:
                            duration_minutes = int(rest_tokens[-2])
                            rest_tokens = rest_tokens[:-2]
                        except ValueError:
                            pass
                if duration_minutes is None and rest_tokens:
                    last = rest_tokens[-1].lower()
                    m2 = _re.match(r'^(\d+)(m|min|mins|minute|minutes)$', last)
                    if m2:
                        duration_minutes = int(m2.group(1))
                        rest_tokens = rest_tokens[:-1]
                    elif _re.match(r'^\d+$', last) and len(rest_tokens) == 1:
                        # Bare number only, no category token — treat as duration
                        duration_minutes = int(last)
                        rest_tokens = rest_tokens[:-1]
                if rest_tokens:
                    category = rest_tokens[0].upper()

            now_utc = _dt.datetime.now(pytz.utc)
            from datetime import timedelta as _td
            end_time_override = (now_utc + _td(minutes=duration_minutes)) if duration_minutes else None
            news_manager: NewsManager = self.bot.news_manager
            event = news_manager.add_event(
                category=category,
                news_time=now_utc,
                window_minutes=0,
                created_by=str(ctx.author),
                is_now_mode=True,
                display_tz='EST',
                end_time_override=end_time_override,
            )

            activated_ts = int(now_utc.timestamp())

            if end_time_override:
                end_ts = int(end_time_override.timestamp())
                ends_val = f"<t:{end_ts}:t> (auto)"
                desc = (
                    f"Signals matching **{category}** will be automatically cancelled "
                    f"for the next **{duration_minutes} minute(s)**."
                )
            else:
                ends_val = "Manual (`!news off`)"
                desc = (
                    f"Signals matching **{category}** will be automatically cancelled "
                    f"until you run `!news off`."
                )

            embed = discord.Embed(
                title="📰 News Mode — ACTIVE NOW",
                description=desc,
                color=0xFF4444,
            )
            embed.add_field(name="Category", value=category, inline=True)
            embed.add_field(name="Activated", value=f"<t:{activated_ts}:t>", inline=True)
            embed.add_field(name="Ends", value=ends_val, inline=True)
            embed.set_footer(text=f"Event #{event.event_id} • Set by {ctx.author}")
            await ctx.send(embed=embed)
            logger.info(
                f"News NOW event #{event.event_id} activated by {ctx.author} for {category}"
                + (f" for {duration_minutes} min" if duration_minutes else "")
            )
            return

        # ── Normal scheduled news ──────────────────────────────────────────
        try:
            category, news_time_utc, window_minutes, tz_label = parse_news_command(args)
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        news_manager: NewsManager = self.bot.news_manager
        event = news_manager.add_event(
            category=category,
            news_time=news_time_utc,
            window_minutes=window_minutes,
            created_by=str(ctx.author),
            display_tz=tz_label,
        )

        news_est = news_time_utc.astimezone(EST)
        start_est = event.start_time.astimezone(EST)
        end_est = event.end_time.astimezone(EST)

        # Detect whether the time was auto-advanced to tomorrow
        import datetime as _dt
        today_in_tz = _dt.datetime.now(pytz.utc).astimezone(EST).date()
        scheduled_date = news_est.date()
        auto_advanced = scheduled_date > today_in_tz

        # Also show in original timezone if not EST
        # Use Discord timestamps so each viewer sees their local time
        news_ts = int(news_time_utc.timestamp())
        start_ts = int(event.start_time.timestamp())
        end_ts = int(event.end_time.timestamp())
        tz_display = f"<t:{news_ts}:t>"
        if tz_label not in ('EST', 'EDT', 'ET'):
            tz_display += f" ({tz_label})"

        # Add date hint when auto-advanced (Discord timestamps show the date but an
        # explicit note makes it clear this slipped to tomorrow)
        if auto_advanced:
            tz_display += f" — <t:{news_ts}:D>"

        embed = discord.Embed(
            title="📰 News Mode Scheduled",
            description=(
                f"Signals matching **{category.upper()}** will be automatically cancelled "
                f"if hit during this window."
            ),
            color=0x5865F2,
        )
        embed.add_field(name="Category", value=category.upper(), inline=True)
        embed.add_field(name="News Time", value=tz_display, inline=True)
        embed.add_field(name="Window", value=f"±{window_minutes} min", inline=True)
        embed.add_field(
            name="Active From → To",
            value=f"<t:{start_ts}:t> → <t:{end_ts}:t>",
            inline=False,
        )
        if auto_advanced:
            embed.add_field(
                name="ℹ️ Note",
                value="That time has already passed today — scheduled for **tomorrow** automatically. Use `date:today` to override.",
                inline=False,
            )
        embed.set_footer(text=f"Event #{event.event_id} • Set by {ctx.author}")

        await ctx.send(embed=embed)
        logger.info(f"News event #{event.event_id} scheduled by {ctx.author}: {event}")

    @commands.command(
        name='newslist',
        aliases=['newsstatus', 'newsmode'],
        description='Show all pending / active news events',
    )
    async def newslist(self, ctx: commands.Context):
        """Show all upcoming and currently-active news windows."""
        news_manager: NewsManager = self.bot.news_manager
        events = news_manager.get_all_events()

        if not events:
            await ctx.send("ℹ️ No news events are currently scheduled.")
            return

        embed = discord.Embed(title="📰 Scheduled News Events", color=0x5865F2)

        import datetime as dt
        now = dt.datetime.now(pytz.utc)

        for event in events:
            if event.is_now_mode:
                activated_ts = int(event.news_time.timestamp())
                status = "🔴 **ACTIVE NOW**"
                if event.end_time_override is not None:
                    end_ts2 = int(event.end_time_override.timestamp())
                    window_str = f"<t:{activated_ts}:t> → <t:{end_ts2}:t> (auto-end)"
                else:
                    window_str = f"From <t:{activated_ts}:t> — Until `!news off`"
                embed.add_field(
                    name=f"#{event.event_id}  {event.category.upper()}",
                    value=(
                        f"{status}\n"
                        f"Window: {window_str}\n"
                        f"Set by: {event.created_by}"
                    ),
                    inline=False,
                )
            else:
                s_ts = int(event.start_time.timestamp())
                e_ts = int(event.end_time.timestamp())
                status = "🟢 **ACTIVE NOW**" if event.is_active(now) else "🕐 Upcoming"
                tz_note = f" ({event.display_tz})" if event.display_tz not in ('EST', 'EDT', 'ET') else ""
                embed.add_field(
                    name=f"#{event.event_id}  {event.category.upper()}{tz_note}",
                    value=(
                        f"{status}\n"
                        f"Window: <t:{s_ts}:t> → <t:{e_ts}:t>\n"
                        f"Set by: {event.created_by}"
                    ),
                    inline=False,
                )

        await ctx.send(embed=embed)

    @commands.command(
        name='newsclear',
        aliases=['newsdel', 'newsremove'],
        description='Remove a news event by ID, or clear all events',
    )
    async def newsclear(self, ctx: commands.Context, event_id: int = None):
        """
        Remove a scheduled news event.

        Usage:
            !newsclear 3      → remove event #3
            !newsclear        → remove all events
        """
        news_manager: NewsManager = self.bot.news_manager
        alert_system = getattr(self.bot.monitor, 'alert_system', None)

        if event_id is None:
            events = news_manager.get_all_events()
            count = len(events)
            for ev in events:
                news_manager.remove_event(ev.event_id)
                if alert_system and ev.event_id in getattr(alert_system, '_news_activation_messages', {}):
                    try:
                        await alert_system.send_news_ended_alert(ev)
                    except Exception as e:
                        logger.warning(f"Failed to send news ended alert for event #{ev.event_id}: {e}")
            await ctx.send(f"🗑️ Removed all {count} scheduled news event(s).")
            return

        removed_event = news_manager.remove_event(event_id)
        if removed_event:
            await ctx.send(f"✅ News event #{event_id} removed.")
            if alert_system and event_id in getattr(alert_system, '_news_activation_messages', {}):
                try:
                    await alert_system.send_news_ended_alert(removed_event)
                except Exception as e:
                    logger.warning(f"Failed to send news ended alert for event #{event_id}: {e}")
        else:
            await ctx.send(f"❌ No news event with ID #{event_id} found.")

    # ── Take-Profit commands ───────────────────────────────────────────────

    @commands.command(name="tp")
    async def tp_command(self, ctx: commands.Context, subcommand: str = None, *args):
        """
        Take-profit configuration.

          !tp config [symbol]       — Show TP config (all, or for one symbol)
          !tp set <target> <value> [pips|dollars]  — Set TP threshold (admin)
          !tp remove <symbol>       — Remove per-symbol override (admin)

        See !help tp for full details.
        """
        if subcommand is None:
            await ctx.send("Usage: `!tp config`, `!tp set`, `!tp remove` — see `!help tp` for details.")
            return

        sub = subcommand.lower()

        if sub == "config":
            symbol = args[0] if args else None
            await self._tp_show(ctx, symbol)

        elif sub == "set":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if len(args) < 2:
                await ctx.send("❌ Usage: `!tp set <target> <value> [pips|dollars]`")
                return
            target, value = args[0], args[1]
            tp_type = args[2] if len(args) >= 3 else None
            await self._tp_set(ctx, target, value, tp_type)

        elif sub == "remove":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if not args:
                await ctx.send("❌ Usage: `!tp remove <symbol>`")
                return
            await self._tp_remove(ctx, args[0])

        else:
            await ctx.send(f"❌ Unknown subcommand `{subcommand}`. See `!help tp` for usage.")

    async def _tp_show(self, ctx: commands.Context, symbol: str = None):
        """Show TP config for a symbol, or all config."""
        try:
            if symbol:
                symbol = symbol.upper()
                info = self.tp_config.get_display_info(symbol)
                value_str = self.tp_config.format_value(symbol, info["value"])

                embed = discord.Embed(
                    title=f"TP Config — {info['symbol']}",
                    color=discord.Color.blue(),
                )
                embed.add_field(name="Asset Class", value=info["asset_class"], inline=True)
                embed.add_field(name="TP Threshold", value=value_str, inline=True)
                embed.add_field(name="Source", value="Override" if info["is_override"] else "Default", inline=True)

                if info["is_override"]:
                    embed.add_field(name="Set By", value=info.get("set_by", "Unknown"), inline=True)
                    set_at = info.get("set_at", "")
                    if set_at:
                        try:
                            dt = datetime.fromisoformat(set_at.replace("Z", "+00:00"))
                            embed.add_field(name="Set At", value=dt.strftime("%Y-%m-%d %H:%M UTC"), inline=True)
                        except Exception:
                            embed.add_field(name="Set At", value=set_at[:19], inline=True)

                embed.set_footer(text="Auto-TP triggers when last limit hits threshold and earlier limits are combined breakeven")
                await ctx.send(embed=embed)

            else:
                info = self.tp_config.get_display_info()

                embed = discord.Embed(
                    title="Auto Take-Profit Configuration",
                    color=discord.Color.blue(),
                )

                defaults_lines = []
                for cls, settings in info["defaults"].items():
                    val_str = f"{settings['value']:.1f} pips" if settings["type"] == "pips" else f"${settings['value']:.2f}"
                    defaults_lines.append(f"**{cls}**: {val_str}")

                embed.add_field(
                    name="Defaults",
                    value="\n".join(defaults_lines) or "None",
                    inline=False,
                )

                if info["overrides"]:
                    override_lines = []
                    for sym, ov in info["overrides"].items():
                        val_str = f"{ov['value']:.1f} pips" if ov["type"] == "pips" else f"${ov['value']:.2f}"
                        override_lines.append(f"**{sym}**: {val_str} _(by {ov.get('set_by', '?')})_")
                    embed.add_field(
                        name=f"Per-Symbol Overrides ({info['total_overrides']})",
                        value="\n".join(override_lines),
                        inline=False,
                    )
                else:
                    embed.add_field(name="Per-Symbol Overrides", value="None", inline=False)

                embed.set_footer(text="Use !tp set and !tp remove to manage thresholds")
                await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in tp config: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching TP config: {e}")

    async def _tp_set(self, ctx: commands.Context, target: str, value: str, tp_type: str = None):
        """Set TP threshold for an asset class or symbol."""
        try:
            try:
                float_value = float(value)
            except ValueError:
                await ctx.send(f"❌ Invalid value `{value}` — must be a number.")
                return

            if float_value <= 0:
                await ctx.send("❌ TP value must be positive.")
                return

            target_lower = target.lower()
            target_upper = target.upper()

            if tp_type is not None:
                tp_type_lower = tp_type.lower()
                if tp_type_lower not in VALID_TP_TYPES:
                    await ctx.send(f"❌ Invalid type `{tp_type}`. Valid types: {', '.join(VALID_TP_TYPES)}")
                    return
            else:
                tp_type_lower = self.tp_config.get_tp_type(target_upper)

            if target_lower in ASSET_CLASSES:
                success = self.tp_config.set_default(target_lower, float_value, tp_type_lower, set_by=ctx.author.name)
                label = f"**{target_lower}** (default)"
            else:
                success = self.tp_config.set_override(target_upper, float_value, tp_type_lower, set_by=ctx.author.name)
                label = f"**{target_upper}** (override)"

            if not success:
                await ctx.send(f"❌ Failed to set TP for `{target}`. Check logs for details.")
                return

            val_display = f"{float_value:.1f} pips" if tp_type_lower == "pips" else f"${float_value:.2f}"

            # Reload live monitor config
            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "tp_config"):
                    self.bot.monitor.tp_config.reload_config()
                    self.bot.monitor.tp_monitor.tp_config = self.bot.monitor.tp_config

            embed = discord.Embed(title="TP Configuration Updated", color=discord.Color.green())
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="New TP Threshold", value=val_display, inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in tp set: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting TP: {e}")

    async def _tp_remove(self, ctx: commands.Context, symbol: str):
        """Remove a per-symbol TP override."""
        try:
            symbol_upper = symbol.upper()
            removed = self.tp_config.remove_override(symbol_upper)

            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "tp_config"):
                    self.bot.monitor.tp_config.reload_config()
                    self.bot.monitor.tp_monitor.tp_config = self.bot.monitor.tp_config

            if removed:
                fallback_val = self.tp_config.get_tp_value(symbol_upper)
                fallback_display = self.tp_config.format_value(symbol_upper, fallback_val)
                asset_class = self.tp_config.determine_asset_class(symbol_upper)

                embed = discord.Embed(title="TP Override Removed", color=discord.Color.green())
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Now Using", value=f"{asset_class} default: {fallback_display}", inline=True)
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No override found for `{symbol_upper}`. It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in tp remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing TP override: {e}")

    # ── End Take-Profit commands ───────────────────────────────────────────

    # ── Alert Distance commands ────────────────────────────────────────────

    @commands.command(
        name='alertdist',
        aliases=['alertdistance', 'adist'],
        description='View or manage approaching-alert distance configuration',
    )
    async def alertdist_command(self, ctx: commands.Context, subcommand: str = None, *args):
        """
        View and manage alert distance thresholds.

        Usage:
            !alertdist config [symbol]          — Show config (all, or for one symbol)
            !alertdist set <target> <value> [type]  — Set threshold (admin)
            !alertdist remove <symbol>          — Remove per-symbol override (admin)

        See !help alertdist for full details.
        """
        if not subcommand:
            await ctx.send("Usage: `!alertdist config`, `!alertdist set`, `!alertdist remove` — see `!help alertdist` for details.")
            return

        sub = subcommand.lower()

        if sub == 'config':
            symbol = args[0] if args else None
            await self._adist_show(ctx, symbol)

        elif sub == 'set':
            if len(args) < 2:
                await ctx.send("❌ Usage: `!alertdist set <target> <value> [pips|dollars|percentage]`")
                return
            target = args[0]
            value = args[1]
            dist_type = args[2] if len(args) >= 3 else None
            await self._adist_set(ctx, target, value, dist_type)

        elif sub == 'remove':
            if not args:
                await ctx.send("❌ Usage: `!alertdist remove <symbol>`")
                return
            await self._adist_remove(ctx, args[0])

        else:
            await ctx.send(f"❌ Unknown subcommand `{subcommand}`. See `!help alertdist` for usage.")

    async def _adist_show(self, ctx: commands.Context, symbol: str = None):
        """Show alert distance config — for one symbol or all defaults/overrides."""
        try:
            if symbol:
                symbol_upper = symbol.upper()
                info = self.alert_dist_config.get_config_display(symbol_upper)
                asset_class = info['asset_class']
                dist_type = info['type']
                value = info['value']
                is_override = info['is_override']

                if dist_type == 'dollars':
                    val_str = f"${value}"
                elif dist_type == 'percentage':
                    val_str = f"{value}%"
                else:
                    val_str = f"{value} pips"

                source = "Per-symbol override" if is_override else f"{asset_class} default"

                embed = discord.Embed(
                    title=f"Alert Distance — {symbol_upper}",
                    color=0x00BFFF,
                )
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Distance", value=val_str, inline=True)
                embed.add_field(name="Type", value=dist_type, inline=True)
                embed.add_field(name="Asset Class", value=asset_class, inline=True)
                embed.add_field(name="Source", value=source, inline=True)

                if is_override:
                    embed.add_field(name="Set By", value=info.get('set_by', 'Unknown'), inline=True)

                embed.set_footer(text="Use !alertdist set / remove to manage thresholds")
                await ctx.send(embed=embed)

            else:
                info = self.alert_dist_config.get_config_display()
                defaults = info['defaults']
                overrides = info['overrides']

                def _fmt_val(t, v):
                    if t == 'dollars':
                        return f"${v}"
                    elif t == 'percentage':
                        return f"{v}%"
                    return f"{v} pips"

                # ── Page 1: defaults (always fits) ──
                def_lines = [
                    f"`{ac}` → {_fmt_val(cfg['type'], cfg['value'])}"
                    for ac, cfg in defaults.items()
                ]
                embed1 = discord.Embed(
                    title="Alert Distance Configuration",
                    description=f"{len(overrides)} per-symbol override(s) total.",
                    color=0x00BFFF,
                )
                embed1.add_field(
                    name="Asset-Class Defaults",
                    value="\n".join(def_lines) or "None",
                    inline=False,
                )
                await ctx.send(embed=embed1)

                if not overrides:
                    return

                # ── Paginate overrides: ~15 per embed to stay well under 1024 chars ──
                PAGE_SIZE = 15
                ov_items = sorted(overrides.items())
                total_pages = (len(ov_items) + PAGE_SIZE - 1) // PAGE_SIZE

                for page_num, start in enumerate(range(0, len(ov_items), PAGE_SIZE), start=1):
                    chunk = ov_items[start:start + PAGE_SIZE]
                    lines = [
                        f"`{sym}` → {_fmt_val(cfg['type'], cfg['value'])}"
                        for sym, cfg in chunk
                    ]
                    embed = discord.Embed(color=0x00BFFF)
                    embed.add_field(
                        name=f"Per-Symbol Overrides ({len(overrides)}) — Page {page_num}/{total_pages}",
                        value="\n".join(lines),
                        inline=False,
                    )
                    await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in alertdist config: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching alert distance config: {e}")

    async def _adist_set(self, ctx: commands.Context, target: str, value: str, dist_type: str = None):
        """Set alert distance threshold for an asset class or per-symbol."""
        try:
            try:
                float_value = float(value)
            except ValueError:
                await ctx.send(f"❌ Invalid value `{value}` — must be a number.")
                return

            if float_value <= 0:
                await ctx.send("❌ Distance value must be positive.")
                return

            target_lower = target.lower()
            target_upper = target.upper()

            VALID_DIST_TYPES = ['pips', 'dollars', 'percentage']

            if dist_type is not None:
                dist_type_lower = dist_type.lower()
                if dist_type_lower not in VALID_DIST_TYPES:
                    await ctx.send(f"❌ Invalid type `{dist_type}`. Valid types: pips, dollars, percentage")
                    return
            else:
                # Infer type from existing config for this target
                if target_lower in ASSET_CLASSES:
                    existing = self.alert_dist_config.config['defaults'].get(target_lower, {})
                    dist_type_lower = existing.get('type', 'pips')
                else:
                    existing_cfg = self.alert_dist_config._get_config_for_symbol(target_upper)
                    dist_type_lower = existing_cfg.get('type', 'pips')

            if target_lower in ASSET_CLASSES:
                # Update asset-class default
                if target_lower not in self.alert_dist_config.config['defaults']:
                    await ctx.send(f"❌ Unknown asset class `{target_lower}`. Valid: {', '.join(ASSET_CLASSES)}")
                    return
                self.alert_dist_config.config['defaults'][target_lower]['value'] = float_value
                self.alert_dist_config.config['defaults'][target_lower]['type'] = dist_type_lower
                self.alert_dist_config._save_config()
                label = f"**{target_lower}** (default)"
            else:
                # Set per-symbol override
                success = self.alert_dist_config.set_override(
                    target_upper, float_value, dist_type_lower, set_by=ctx.author.name
                )
                if not success:
                    await ctx.send(f"❌ Failed to set alert distance for `{target}`. Check logs.")
                    return
                label = f"**{target_upper}** (override)"

            # Format for display
            if dist_type_lower == 'dollars':
                val_display = f"${float_value}"
            elif dist_type_lower == 'percentage':
                val_display = f"{float_value}%"
            else:
                val_display = f"{float_value} pips"

            # Reload live monitor config
            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                if hasattr(self.bot.monitor, 'alert_config'):
                    self.bot.monitor.alert_config.reload_config()

            embed = discord.Embed(title="Alert Distance Updated", color=discord.Color.green())
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="New Threshold", value=val_display, inline=True)
            embed.add_field(name="Type", value=dist_type_lower, inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in alertdist set: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting alert distance: {e}")

    async def _adist_remove(self, ctx: commands.Context, symbol: str):
        """Remove a per-symbol alert distance override."""
        try:
            symbol_upper = symbol.upper()
            removed = self.alert_dist_config.remove_override(symbol_upper)

            if hasattr(self.bot, 'monitor') and self.bot.monitor:
                if hasattr(self.bot.monitor, 'alert_config'):
                    self.bot.monitor.alert_config.reload_config()

            if removed:
                fallback_cfg = self.alert_dist_config._get_config_for_symbol(symbol_upper)
                t = fallback_cfg['type']
                v = fallback_cfg['value']
                if t == 'dollars':
                    fallback_str = f"${v}"
                elif t == 'percentage':
                    fallback_str = f"{v}%"
                else:
                    fallback_str = f"{v} pips"
                asset_class = self.alert_dist_config._determine_asset_class(symbol_upper)

                embed = discord.Embed(title="Alert Distance Override Removed", color=discord.Color.green())
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Now Using", value=f"{asset_class} default: {fallback_str}", inline=True)
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No override found for `{symbol_upper}`. It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in alertdist remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing alert distance override: {e}")

    # ── End Alert Distance commands ────────────────────────────────────────

    # ── Near-Miss (NM) configuration commands ─────────────────────────────

    @commands.command(name="nmconfig", aliases=["nmc", "nm_config"])
    async def nm_config_command(self, ctx: commands.Context, subcommand: str = None, *args):
        """
        Near-miss auto-cancel configuration (linear bounce model).

          !nmconfig show [symbol]                                         — Show NM config
          !nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]  — Set (admin)
          !nmconfig remove <symbol>                                       — Remove override (admin)

        The required bounce scales linearly: required = closest_distance + base_bounce
        So price that got within 2 pips needs less bounce than one that stayed 6 pips away.

        Examples:
          !nmconfig show XAUUSD
          !nmconfig set XAUUSD 6 3 dollars      (within $6; bounce = closest + $3)
          !nmconfig set forex 7 4 pips          (within 7 pips; bounce = closest + 4 pips)
          !nmconfig remove XAUUSD
        """
        if subcommand is None:
            await ctx.send(
                "Usage: `!nmconfig show [symbol]`, `!nmconfig set <target> <proximity> <bounce> [pips|dollars]`, "
                "`!nmconfig remove <symbol>`"
            )
            return

        sub = subcommand.lower()

        if sub == "show":
            symbol = args[0] if args else None
            await self._nm_show(ctx, symbol)

        elif sub == "set":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if len(args) < 2:
                await ctx.send("❌ Usage: `!nmconfig set <target> <proximity> <bounce> [pips|dollars]`")
                return
            target = args[0]
            proximity_str = args[1]
            bounce_str = args[2] if len(args) >= 3 else None
            nm_type = args[3] if len(args) >= 4 else None
            await self._nm_set(ctx, target, proximity_str, bounce_str, nm_type)

        elif sub == "remove":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if not args:
                await ctx.send("❌ Usage: `!nmconfig remove <symbol>`")
                return
            await self._nm_remove(ctx, args[0])

        else:
            await ctx.send(f"❌ Unknown subcommand `{subcommand}`. Use `show`, `set`, or `remove`.")

    async def _nm_show(self, ctx: commands.Context, symbol: str = None):
        """Show NM config for one symbol or all."""
        try:
            if symbol:
                symbol = symbol.upper()
                info = self.nm_config.get_params_display(symbol)
                nm_type = info["type"]
                unit = "pips" if nm_type == "pips" else "$"

                prox_str = f"{info['max_proximity']} {unit}" if nm_type == "pips" else f"${info['max_proximity']}"
                base_str = f"{info['base_bounce']} {unit}" if nm_type == "pips" else f"${info['base_bounce']}"

                is_override = symbol in self.nm_config.get_all_overrides()

                embed = discord.Embed(
                    title=f"NM Config — {symbol}",
                    color=discord.Color.orange(),
                    description=(
                        f"**Formula:** `required_bounce = closest_distance + base_bounce`\n"
                        f"Price must enter the proximity zone first; any bounce beyond this formula triggers an NM."
                    ),
                )
                embed.add_field(name="Max Proximity", value=prox_str, inline=True)
                embed.add_field(name="Base Bounce", value=base_str, inline=True)
                embed.add_field(name="Source", value="Override" if is_override else "Default", inline=True)
                embed.add_field(
                    name="Curve Preview",
                    value=f"```\n{self.nm_config.describe_curve(symbol)}\n```",
                    inline=False,
                )
                if info.get("description"):
                    embed.add_field(name="Note", value=info["description"], inline=False)
                embed.set_footer(text="!nmconfig set to adjust | closer approach = less bounce needed")
                await ctx.send(embed=embed)

            else:
                defaults = self.nm_config.get_all_defaults()
                overrides = self.nm_config.get_all_overrides()

                embed = discord.Embed(
                    title="Near-Miss Auto-Cancel Configuration",
                    color=discord.Color.orange(),
                    description=(
                        "**Linear model:** `required_bounce = closest_distance + base_bounce`\n"
                        "Price must enter the proximity zone to start tracking."
                    ),
                )

                defaults_lines = []
                for cls, cfg in defaults.items():
                    t = cfg.get("type", "pips")
                    p = cfg.get("max_proximity", 0)
                    b = cfg.get("base_bounce", 0)
                    if t == "pips":
                        defaults_lines.append(f"**{cls}**: within {p} pips, base bounce {b} pips")
                    else:
                        defaults_lines.append(f"**{cls}**: within ${p}, base bounce ${b}")

                embed.add_field(
                    name="Defaults",
                    value="\n".join(defaults_lines) or "None",
                    inline=False,
                )

                if overrides:
                    override_lines = []
                    for sym, ov in overrides.items():
                        t = ov.get("type", "pips")
                        p = ov.get("max_proximity", 0)
                        b = ov.get("base_bounce", 0)
                        set_by = ov.get("set_by", "?")
                        if t == "pips":
                            override_lines.append(f"**{sym}**: {p} pip / +{b} pip base _(by {set_by})_")
                        else:
                            override_lines.append(f"**{sym}**: ${p} / +${b} base _(by {set_by})_")
                    embed.add_field(
                        name=f"Per-Symbol Overrides ({len(overrides)})",
                        value="\n".join(override_lines),
                        inline=False,
                    )
                else:
                    embed.add_field(name="Per-Symbol Overrides", value="None", inline=False)

                embed.set_footer(text="Use !nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]")
                await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in nmconfig show: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching NM config: {e}")

    async def _nm_set(self, ctx, target: str, proximity_str: str, bounce_str: str = None, nm_type: str = None):
        """Set NM thresholds for an asset class or symbol."""
        ASSET_CLASSES = {"forex", "forex_jpy", "metals", "indices", "stocks", "crypto", "oil"}
        VALID_TYPES = {"pips", "dollars"}

        try:
            try:
                max_proximity = float(proximity_str)
            except (ValueError, TypeError):
                await ctx.send(f"❌ Invalid max_proximity `{proximity_str}` — must be a number.")
                return

            if bounce_str is None:
                await ctx.send("❌ Usage: `!nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]`")
                return

            try:
                base_bounce = float(bounce_str)
            except ValueError:
                await ctx.send(f"❌ Invalid base_bounce `{bounce_str}` — must be a number.")
                return

            if max_proximity <= 0 or base_bounce <= 0:
                await ctx.send("❌ Both values must be positive numbers.")
                return

            if nm_type is not None:
                nm_type = nm_type.lower()
                if nm_type not in VALID_TYPES:
                    await ctx.send(f"❌ Invalid type `{nm_type}`. Valid: {', '.join(VALID_TYPES)}")
                    return

            target_lower = target.lower()
            target_upper = target.upper()

            if target_lower in ASSET_CLASSES:
                success = self.nm_config.set_default(target_lower, max_proximity, base_bounce, nm_type, set_by=ctx.author.name)
                label = f"**{target_lower}** (default)"
            else:
                success = self.nm_config.set_override(target_upper, max_proximity, base_bounce, nm_type, set_by=ctx.author.name)
                label = f"**{target_upper}** (override)"

            # Reload live monitor config
            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "nm_config"):
                    self.bot.monitor.nm_config = NMConfig()
                    self.bot.monitor.nm_monitor.nm_config = self.bot.monitor.nm_config

            unit = nm_type if nm_type else "?"
            embed = discord.Embed(title="NM Configuration Updated", color=discord.Color.green())
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="Max Proximity", value=f"{max_proximity} {unit}", inline=True)
            embed.add_field(name="Base Bounce", value=f"{base_bounce} {unit}", inline=True)
            embed.add_field(
                name="Curve Preview",
                value=f"```\n{self.nm_config.describe_curve(target_upper if target_lower not in ASSET_CLASSES else 'EURUSD')}\n```",
                inline=False,
            )
            embed.set_footer(text=f"Set by {ctx.author.name} | required_bounce = closest_distance + base_bounce")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in nmconfig set: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting NM config: {e}")

    async def _nm_remove(self, ctx, symbol: str):
        """Remove a per-symbol NM override."""
        try:
            symbol_upper = symbol.upper()
            removed = self.nm_config.remove_override(symbol_upper)

            # Reload live monitor config
            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "nm_config"):
                    self.bot.monitor.nm_config = NMConfig()
                    self.bot.monitor.nm_monitor.nm_config = self.bot.monitor.nm_config

            if removed:
                info = self.nm_config.get_params_display(symbol_upper)
                t = info["type"]
                p, b = info["max_proximity"], info["base_bounce"]
                fallback_str = f"{p} pip proximity, +{b} pip base" if t == "pips" else f"${p} proximity, +${b} base"
                asset_class = self.nm_config._get_asset_class(symbol_upper)

                embed = discord.Embed(title="NM Override Removed", color=discord.Color.green())
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Now Using", value=f"{asset_class} default: {fallback_str}", inline=True)
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No NM override found for `{symbol_upper}`. It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in nmconfig remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing NM override: {e}")

    # ── End Near-Miss commands ─────────────────────────────────────────────


async def setup(bot):
    """Setup function for Discord.py to load this cog"""
    await bot.add_cog(TradingCommands(bot))