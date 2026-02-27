"""
News Mode Commands

!news <category> <time> [window_minutes]
    Schedule a news window. Any signals whose instrument matches <category>
    that are hit within the window will be automatically cancelled.

    Examples:
        !news USD 12:30pm 15    → ±15 min around 12:30 PM EST for all USD pairs
        !news gold 8:30am       → ±10 min around 8:30 AM EST for gold (default window)
        !news all 14:00 30      → ±30 min around 14:00 EST for every signal

!newslist             — Show all pending / active news events
!newsclear [id]       — Remove a specific event (or all events if no ID given)

Valid categories:
    Any forex currency:  USD, EUR, GBP, JPY, AUD, NZD, CAD, CHF, …
    Named assets:        gold / xau, oil, btc, eth, crypto
    Wildcard:            all
"""

import discord
from discord.ext import commands
import pytz

from commands.base_command import BaseCog
from core.news_manager import (
    NewsManager,
    NewsEvent,
    FOREX_CURRENCIES,
    NAMED_CATEGORIES,
    parse_news_command,
)
from utils.logger import get_logger

logger = get_logger('news_commands')
EST = pytz.timezone('America/New_York')


class NewsCommands(BaseCog):
    """Commands for managing news mode."""

    # ------------------------------------------------------------------
    # !news
    # ------------------------------------------------------------------

    @commands.command(
        name='news',
        description='Schedule a news window that auto-cancels signals when hit',
    )
    async def news(self, ctx: commands.Context, *, args: str = None):
        """
        Schedule a news window.

        Usage:
            !news <category> <time> [window_minutes]

        Examples:
            !news USD 12:30pm 15
            !news gold 8:30am
            !news all 14:00 30
            !news JPY 9:30am 20
        """
        if not args:
            await ctx.send(
                "❌ Usage: `!news <category> <time> [window_minutes]`\n"
                "Example: `!news USD 12:30pm 15`\n"
                "Categories: any currency code (USD, EUR, GBP…), `gold`, `oil`, `btc`, `crypto`, or `all`"
            )
            return

        try:
            category, news_time_utc, window_minutes = parse_news_command(args)
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        news_manager: NewsManager = self.bot.news_manager
        event = news_manager.add_event(
            category=category,
            news_time=news_time_utc,
            window_minutes=window_minutes,
            created_by=str(ctx.author),
        )

        news_est = news_time_utc.astimezone(EST)
        start_est = event.start_time.astimezone(EST)
        end_est = event.end_time.astimezone(EST)

        embed = discord.Embed(
            title="📰 News Mode Scheduled",
            description=(
                f"Signals matching **{category.upper()}** will be automatically cancelled "
                f"if hit during this window."
            ),
            color=0x5865F2,
        )
        embed.add_field(name="Category", value=category.upper(), inline=True)
        embed.add_field(name="News Time", value=news_est.strftime('%I:%M %p EST'), inline=True)
        embed.add_field(name="Window", value=f"±{window_minutes} min", inline=True)
        embed.add_field(
            name="Active From → To",
            value=(
                f"{start_est.strftime('%I:%M %p')} → {end_est.strftime('%I:%M %p')} EST"
            ),
            inline=False,
        )
        embed.set_footer(text=f"Event #{event.event_id} • Set by {ctx.author}")

        await ctx.send(embed=embed)
        logger.info(
            f"News event #{event.event_id} scheduled by {ctx.author}: {event}"
        )

    # ------------------------------------------------------------------
    # !newslist
    # ------------------------------------------------------------------

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

        embed = discord.Embed(
            title="📰 Scheduled News Events",
            color=0x5865F2,
        )

        import datetime as dt
        now = dt.datetime.now(pytz.utc)

        for event in events:
            start_est = event.start_time.astimezone(EST)
            end_est = event.end_time.astimezone(EST)

            if event.is_active(now):
                status = "🟢 **ACTIVE NOW**"
            else:
                status = "🕐 Upcoming"

            embed.add_field(
                name=f"#{event.event_id}  {event.category.upper()}",
                value=(
                    f"{status}\n"
                    f"Window: {start_est.strftime('%I:%M %p')} → {end_est.strftime('%I:%M %p')} EST\n"
                    f"Set by: {event.created_by}"
                ),
                inline=False,
            )

        await ctx.send(embed=embed)

    # ------------------------------------------------------------------
    # !newsclear
    # ------------------------------------------------------------------

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

        if event_id is None:
            # Clear all
            events = news_manager.get_all_events()
            count = len(events)
            for ev in events:
                news_manager.remove_event(ev.event_id)
            await ctx.send(f"🗑️ Removed all {count} scheduled news event(s).")
            return

        removed = news_manager.remove_event(event_id)
        if removed:
            await ctx.send(f"✅ News event #{event_id} removed.")
        else:
            await ctx.send(f"❌ No news event with ID #{event_id} found.")


async def setup(bot):
    await bot.add_cog(NewsCommands(bot))