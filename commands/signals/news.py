"""
News Commands — news mode scheduling and management.
"""

import datetime as _dt
import re as _re
from datetime import timedelta as _td

import discord
import pytz
from discord.ext import commands

from core.news_manager import (
    EST,
    NewsManager,
    parse_news_command,
)
from utils.logger import get_logger

from .._base import BaseCog

logger = get_logger("news_commands")


class NewsCog(BaseCog):
    """News mode commands: schedule, list, and clear news windows"""

    @commands.command(
        name="news",
        description="Schedule a news window that auto-cancels signals when hit",
    )
    async def news(self, ctx: commands.Context, *, args: str = None):
        """
        Schedule a news window, or use special subcommands.

        Usage:
            !news <category> <time> [window] [tz:<tz>] [date:<date>]
            !news now [category]   → open-ended window active immediately (default: all)
            !news on [category]    → alias for !news now
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
                "Or: `!news now [category]` / `!news on [category]` / `!news off`\n"
                "Categories: any currency code (USD, EUR, GBP…), `gold`, `oil`, `btc`, `crypto`, or `all`\n"
                "Timezone tag example: `tz:UTC`  `tz:EST`  `tz:London`  `tz:CET`\n"
                "Date tag example: `date:2025-06-15`  `date:06/15`  `date:tomorrow`"
            )
            return

        tokens = args.strip().split()
        subcommand = tokens[0].lower()

        # ── !news off ──────────────────────────────────────────────────────
        if subcommand == "off":
            news_manager: NewsManager = self.bot.news_manager
            removed_events = news_manager.remove_now_events()
            if removed_events:
                await ctx.send(f"✅ Deactivated {len(removed_events)} open-ended news window(s).")
                alert_system = self.services.alert_system
                if alert_system:
                    for event in removed_events:
                        try:
                            await alert_system.send_news_ended_alert(event)
                        except Exception as e:
                            logger.warning(
                                f"Failed to send news ended alert for event #{event.event_id}: {e}"
                            )
                # Update DB: news mode is off unless a scheduled event is still active
                still_active = any(e.is_active() for e in news_manager.get_all_events())
                try:
                    await self.bot.db.set_news_mode(still_active)
                except Exception as e:
                    logger.warning(f"Failed to update news_mode in DB after !news off: {e}")
            else:
                await ctx.send("ℹ️ No open-ended news windows were active.")
            return

        # ── !news now / !news on [category] [N minutes] ───────────────────
        if subcommand in ("now", "on"):
            rest_tokens = tokens[1:]
            category = "ALL"
            duration_minutes = None

            if rest_tokens:
                # Strip optional trailing duration: "5 minutes", "5m", "5 min", bare "5"
                if len(rest_tokens) >= 2:
                    last = rest_tokens[-1].lower()
                    if last in ("minutes", "mins", "min", "m", "minute"):
                        try:
                            duration_minutes = int(rest_tokens[-2])
                            rest_tokens = rest_tokens[:-2]
                        except ValueError:
                            pass
                if duration_minutes is None and rest_tokens:
                    last = rest_tokens[-1].lower()
                    m2 = _re.match(r"^(\d+)(m|min|mins|minute|minutes)$", last)
                    if m2:
                        duration_minutes = int(m2.group(1))
                        rest_tokens = rest_tokens[:-1]
                    elif _re.match(r"^\d+$", last) and len(rest_tokens) == 1:
                        # Bare number only, no category token — treat as duration
                        duration_minutes = int(last)
                        rest_tokens = rest_tokens[:-1]
                if rest_tokens:
                    category = rest_tokens[0].upper()

            now_utc = _dt.datetime.now(pytz.utc)

            end_time_override = (
                (now_utc + _td(minutes=duration_minutes)) if duration_minutes else None
            )
            news_manager: NewsManager = self.bot.news_manager
            event = news_manager.add_event(
                category=category,
                news_time=now_utc,
                window_minutes=0,
                created_by=str(ctx.author),
                is_now_mode=True,
                display_tz="EST",
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
        today_in_tz = _dt.datetime.now(pytz.utc).astimezone(EST).date()
        scheduled_date = news_est.date()
        auto_advanced = scheduled_date > today_in_tz

        # Also show in original timezone if not EST
        # Use Discord timestamps so each viewer sees their local time
        news_ts = int(news_time_utc.timestamp())
        start_ts = int(event.start_time.timestamp())
        end_ts = int(event.end_time.timestamp())
        tz_display = f"<t:{news_ts}:t>"
        if tz_label not in ("EST", "EDT", "ET"):
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
        name="newslist",
        aliases=["newsstatus", "newsmode"],
        description="Show all pending / active news events",
    )
    async def newslist(self, ctx: commands.Context):
        """Show all upcoming and currently-active news windows."""
        news_manager: NewsManager = self.bot.news_manager
        events = news_manager.get_all_events()

        if not events:
            await ctx.send("ℹ️ No news events are currently scheduled.")
            return

        embed = discord.Embed(title="📰 Scheduled News Events", color=0x5865F2)

        now = _dt.datetime.now(pytz.utc)

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
                    value=(f"{status}\nWindow: {window_str}\nSet by: {event.created_by}"),
                    inline=False,
                )
            else:
                s_ts = int(event.start_time.timestamp())
                e_ts = int(event.end_time.timestamp())
                status = "🟢 **ACTIVE NOW**" if event.is_active(now) else "🕐 Upcoming"
                tz_note = (
                    f" ({event.display_tz})" if event.display_tz not in ("EST", "EDT", "ET") else ""
                )
                embed.add_field(
                    name=f"#{event.event_id}  {event.category.upper()}{tz_note}",
                    value=(
                        f"{status}\nWindow: <t:{s_ts}:t> → <t:{e_ts}:t>\nSet by: {event.created_by}"
                    ),
                    inline=False,
                )

        await ctx.send(embed=embed)

    @commands.command(
        name="newsclear",
        aliases=["newsdel", "newsremove"],
        description="Remove a news event by ID, or clear all events",
    )
    async def newsclear(self, ctx: commands.Context, event_id: int = None):
        """
        Remove a scheduled news event.

        Usage:
            !newsclear 3      → remove event #3
            !newsclear        → remove all events
        """
        news_manager: NewsManager = self.bot.news_manager
        alert_system = self.services.alert_system

        if event_id is None:
            events = news_manager.get_all_events()
            count = len(events)
            for ev in events:
                news_manager.remove_event(ev.event_id)
                if alert_system and ev.event_id in getattr(
                    alert_system, "_news_activation_messages", {}
                ):
                    try:
                        await alert_system.send_news_ended_alert(ev)
                    except Exception as e:
                        logger.warning(
                            f"Failed to send news ended alert for event #{ev.event_id}: {e}"
                        )
            await ctx.send(f"🗑️ Removed all {count} scheduled news event(s).")
            return

        removed_event = news_manager.remove_event(event_id)
        if removed_event:
            await ctx.send(f"✅ News event #{event_id} removed.")
            if alert_system and event_id in getattr(alert_system, "_news_activation_messages", {}):
                try:
                    await alert_system.send_news_ended_alert(removed_event)
                except Exception as e:
                    logger.warning(f"Failed to send news ended alert for event #{event_id}: {e}")
        else:
            await ctx.send(f"❌ No news event with ID #{event_id} found.")
