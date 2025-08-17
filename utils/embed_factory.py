"""
Embed Factory - Standardized embed creation for the bot
Fixed to work with new enhanced database structure
"""
import discord
from typing import Optional, List, Dict, Any


class EmbedFactory:
    """Factory class for creating standardized Discord embeds"""

    # Standard colors
    SUCCESS = discord.Color.green()
    ERROR = discord.Color.red()
    WARNING = discord.Color.orange()
    INFO = discord.Color.blue()
    NEUTRAL = discord.Color.light_gray()

    @staticmethod
    def success(title: str, description: str = None, **kwargs) -> discord.Embed:
        """Create a success embed with green color"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=EmbedFactory.SUCCESS
        )
        return EmbedFactory._add_fields(embed, kwargs)

    @staticmethod
    def error(title: str, description: str = None, **kwargs) -> discord.Embed:
        """Create an error embed with red color"""
        embed = discord.Embed(
            title=f"❌ {title}",
            description=description,
            color=EmbedFactory.ERROR
        )
        return EmbedFactory._add_fields(embed, kwargs)

    @staticmethod
    def warning(title: str, description: str = None, **kwargs) -> discord.Embed:
        """Create a warning embed with orange color"""
        embed = discord.Embed(
            title=f"⚠️ {title}",
            description=description,
            color=EmbedFactory.WARNING
        )
        return EmbedFactory._add_fields(embed, kwargs)

    @staticmethod
    def info(title: str, description: str = None, **kwargs) -> discord.Embed:
        """Create an info embed with blue color"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=EmbedFactory.INFO
        )
        return EmbedFactory._add_fields(embed, kwargs)

    @staticmethod
    def signal_display(signal: Dict[str, Any], detailed: bool = False) -> discord.Embed:
        """Create an embed for displaying a signal"""
        # Get status emoji
        status_emoji = EmbedFactory._get_status_emoji(signal.get('status', 'active'))
        direction_emoji = "📈" if signal.get('direction') == 'long' else "📉"

        embed = discord.Embed(
            title=f"{status_emoji} {direction_emoji} {signal.get('instrument', 'Unknown')} - {signal.get('direction', '').upper()}",
            color=EmbedFactory._get_status_color(signal.get('status', 'active'))
        )

        # Format limits - handle both list formats
        limits = signal.get('limits', [])
        pending_limits = signal.get('pending_limits', [])
        hit_limits = signal.get('hit_limits', [])

        # If limits is a list of dicts (from get_signal_with_limits)
        if limits and isinstance(limits[0], dict):
            pending_limits = [l['price_level'] for l in limits if l['status'] == 'pending']
            hit_limits = [l['price_level'] for l in limits if l['status'] == 'hit']

        # Format limits display
        if pending_limits:
            limits_str = EmbedFactory._format_price_list(pending_limits[:3])
            if len(pending_limits) > 3:
                limits_str += f" (+{len(pending_limits) - 3} more)"
        else:
            limits_str = "None pending"

        # Format stop loss
        stop_loss = signal.get('stop_loss', 0)
        stop_str = EmbedFactory._format_price(stop_loss) if stop_loss else "None"

        # Add fields
        embed.add_field(name="Pending Limits", value=limits_str, inline=True)
        embed.add_field(name="Stop Loss", value=stop_str, inline=True)

        # Add progress if available
        if signal.get('progress'):
            embed.add_field(name="Progress", value=signal['progress'], inline=True)
        elif 'limits_hit' in signal and 'total_limits' in signal:
            embed.add_field(
                name="Progress",
                value=f"{signal['limits_hit']}/{signal['total_limits']} hit",
                inline=True
            )

        if signal.get('expiry_type'):
            embed.add_field(
                name="Expiry",
                value=signal['expiry_type'].replace('_', ' ').title(),
                inline=True
            )

        if detailed:
            if signal.get('id'):
                embed.add_field(name="Signal ID", value=f"#{signal['id']}", inline=True)
            if signal.get('time_remaining'):
                embed.add_field(name="Time Remaining", value=signal['time_remaining'], inline=True)
            if signal.get('status'):
                embed.add_field(name="Status", value=signal['status'].upper(), inline=True)

        return embed

    @staticmethod
    def signal_added(signal_id: int, parsed_signal: Any, author: str = None) -> discord.Embed:
        """Create an embed for a successfully added signal"""
        embed = discord.Embed(
            title="✅ Signal Added",
            description=f"Signal #{signal_id} added successfully",
            color=EmbedFactory.SUCCESS
        )

        embed.add_field(
            name="Instrument",
            value=parsed_signal.instrument,
            inline=True
        )
        embed.add_field(
            name="Direction",
            value=parsed_signal.direction.upper(),
            inline=True
        )
        embed.add_field(
            name="Stop Loss",
            value=EmbedFactory._format_price(parsed_signal.stop_loss),
            inline=True
        )

        limits_str = EmbedFactory._format_price_list(parsed_signal.limits[:3])
        if len(parsed_signal.limits) > 3:
            limits_str += f" (+{len(parsed_signal.limits) - 3} more)"

        embed.add_field(
            name="Limits",
            value=limits_str,
            inline=False
        )
        embed.add_field(
            name="Expiry",
            value=parsed_signal.expiry_type.replace('_', ' ').title(),
            inline=True
        )

        if author:
            embed.set_footer(text=f"Added by {author}")

        return embed

    @staticmethod
    def bot_status(stats: Dict[str, Any], bot_info: Dict[str, Any]) -> discord.Embed:
        """Create a comprehensive bot status embed"""
        embed = discord.Embed(
            title="🤖 Bot Status",
            color=EmbedFactory.INFO
        )

        # Basic info
        embed.add_field(
            name="Version",
            value=bot_info.get('version', '1.0.0'),
            inline=True
        )
        embed.add_field(
            name="Guilds",
            value=bot_info.get('guilds', 0),
            inline=True
        )
        embed.add_field(
            name="Latency",
            value=f"{bot_info.get('latency', 0)}ms",
            inline=True
        )

        # Monitoring info
        embed.add_field(
            name="Monitored Channels",
            value=bot_info.get('monitored_channels', 0),
            inline=True
        )
        embed.add_field(
            name="Database",
            value="✅ Connected" if bot_info.get('db_connected', False) else "❌ Disconnected",
            inline=True
        )
        embed.add_field(
            name="Debug Mode",
            value="✅" if bot_info.get('debug_mode', False) else "❌",
            inline=True
        )

        # Signal statistics
        embed.add_field(
            name="📊 Signal Statistics",
            value="─" * 20,
            inline=False
        )
        embed.add_field(
            name="Total Signals",
            value=stats.get('total_signals', 0),
            inline=True
        )
        embed.add_field(
            name="Currently Tracking",
            value=stats.get('tracking_count', 0),
            inline=True
        )

        # Overall performance
        overall = stats.get('overall', {})
        if overall:
            embed.add_field(
                name="Win Rate",
                value=f"{overall.get('win_rate', 0)}%",
                inline=True
            )

        return embed

    @staticmethod
    def active_signals_list(signals: List[Dict[str, Any]], guild_id: int,
                            instrument: Optional[str] = None) -> discord.Embed:
        """Create an embed for displaying active signals"""
        if not signals:
            embed = discord.Embed(
                title="📊 Active Signals",
                description="No active signals found" + (f" for {instrument}" if instrument else ""),
                color=EmbedFactory.WARNING
            )
            return embed

        embed = discord.Embed(
            title="📊 Active Signals",
            description=f"Found {len(signals)} active signal(s)" + (f" for {instrument}" if instrument else ""),
            color=EmbedFactory.INFO
        )

        for i, signal in enumerate(signals[:10], 1):
            # Get status emoji
            status_emoji = EmbedFactory._get_status_emoji(signal.get('status', 'active'))

            # Format limits - handle the new structure
            pending_limits = signal.get('pending_limits', [])
            hit_limits = signal.get('hit_limits', [])

            if pending_limits:
                limits_str = EmbedFactory._format_price_list(pending_limits[:3])
                if len(pending_limits) > 3:
                    limits_str += f" (+{len(pending_limits) - 3} more)"
            else:
                limits_str = "None pending"

            # Show hit limits if any
            if hit_limits:
                limits_str += f" | {len(hit_limits)} hit"

            # Format stop loss
            stop_str = EmbedFactory._format_price(signal.get('stop_loss', 0))

            # Create link or label for message
            if str(signal['message_id']).startswith("manual_"):
                link_label = "Manual Entry"
            else:
                message_url = f"https://discord.com/channels/{guild_id}/{signal['channel_id']}/{signal['message_id']}"
                link_label = f"[Jump]({message_url})"

            # Build field value
            field_value = (
                f"**Limits:** {limits_str}\n"
                f"**Stop:** {stop_str}\n"
                f"**Status:** {signal.get('status', 'active').upper()}"
            )

            # Add distance information if available
            if signal.get('distance_info') and signal.get('status', 'active').lower() in ['active', 'hit']:
                distance_info = signal['distance_info']
                formatted_distance = distance_info['formatted']

                # Add direction indicator (approaching or moving away)
                if distance_info['distance'] > 0:
                    # Price is in the right direction (away from limit)
                    field_value += f"\n**Distance:** {formatted_distance}"
                else:
                    # Price has passed the limit (shouldn't happen for pending limits)
                    field_value += f"\n**Distance:** Past limit by {formatted_distance}"

            if signal.get('time_remaining'):
                field_value += f"\n**Expiry:** {signal['time_remaining']}"

            field_value += f"\n**Source:** {link_label}"

            embed.add_field(
                name=f"{status_emoji} #{signal['id']} - {signal['instrument']} - {signal['direction'].upper()}",
                value=field_value,
                inline=False
            )

        if len(signals) > 10:
            embed.set_footer(text=f"Showing first 10 of {len(signals)} signals")

        return embed

    @staticmethod
    def _format_price(price: float) -> str:
        """Format a price based on its value"""
        if not price:
            return "None"
        return f"{price:.5f}" if price < 10 else f"{price:.2f}"

    @staticmethod
    def _format_price_list(prices: List[float]) -> str:
        """Format a list of prices"""
        return ", ".join([EmbedFactory._format_price(p) for p in prices])

    @staticmethod
    def _get_status_emoji(status: str) -> str:
        """Get emoji for status"""
        emoji_map = {
            'active': '🟢',
            'hit': '🎯',
            'profit': '💰',
            'breakeven': '➖',
            'stop_loss': '🛑',
            'cancelled': '❌'
        }
        return emoji_map.get(status, '❓')

    @staticmethod
    def _get_status_color(status: str) -> discord.Color:
        """Get color for status"""
        color_map = {
            'active': discord.Color.green(),
            'hit': discord.Color.blue(),
            'profit': discord.Color.gold(),
            'breakeven': discord.Color.light_gray(),
            'stop_loss': discord.Color.red(),
            'cancelled': discord.Color.dark_gray()
        }
        return color_map.get(status, discord.Color.default())

    @staticmethod
    def _add_fields(embed: discord.Embed, fields: Dict[str, Any]) -> discord.Embed:
        """Add fields from kwargs to embed"""
        for key, value in fields.items():
            if key == 'footer':
                embed.set_footer(text=value)
            elif key == 'thumbnail':
                embed.set_thumbnail(url=value)
            elif key == 'image':
                embed.set_image(url=value)
            elif key == 'author':
                embed.set_author(**value)
        return embed