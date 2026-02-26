"""
Take-Profit Configuration Commands

Commands:
  !tpconfig [symbol]          — Show current TP configuration (all or per symbol)
  !settp <target> <value> [type]  — Set TP for an asset class or symbol
  !removetp <symbol>          — Remove a per-symbol override

Only admins can use !settp and !removetp.
"""

import discord
from discord.ext import commands
from datetime import datetime

from commands.base_command import BaseCog
from price_feeds.tp_config import TPConfig


ASSET_CLASSES = ["forex", "forex_jpy", "metals", "indices", "stocks", "crypto", "oil"]
VALID_TYPES = ["pips", "dollars"]

# Emoji per asset class for display
CLASS_EMOJI = {
    "forex":     "💱",
    "forex_jpy": "🇯🇵",
    "metals":    "🥇",
    "indices":   "📈",
    "stocks":    "🏢",
    "crypto":    "🔗",
    "oil":       "🛢️",
}


class TPCommands(BaseCog):
    """Take-profit configuration commands"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()

    # ------------------------------------------------------------------
    # !tpconfig
    # ------------------------------------------------------------------

    @commands.command(name="tpconfig", aliases=["tpshow", "showtp"])
    async def show_tp_config(self, ctx: commands.Context, symbol: str = None):
        """
        Show current TP configuration.

        Usage:
          !tpconfig             — Show all defaults and overrides
          !tpconfig XAUUSD      — Show TP for a specific symbol
        """
        try:
            if symbol:
                await self._show_symbol_config(ctx, symbol)
            else:
                await self._show_all_config(ctx)
        except Exception as e:
            self.logger.error(f"Error in tpconfig command: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching TP config: {e}")

    async def _show_symbol_config(self, ctx: commands.Context, symbol: str):
        """Show TP config for a specific symbol."""
        info = self.tp_config.get_display_info(symbol)

        value_str = self.tp_config.format_value(symbol, info["value"])
        asset_emoji = CLASS_EMOJI.get(info["asset_class"], "📊")

        embed = discord.Embed(
            title=f"📊 TP Config — {info['symbol']}",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Asset Class", value=f"{asset_emoji} {info['asset_class']}", inline=True)
        embed.add_field(name="TP Threshold", value=value_str, inline=True)
        embed.add_field(name="Source", value="🔧 Override" if info["is_override"] else "📋 Default", inline=True)

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

    async def _show_all_config(self, ctx: commands.Context):
        """Show full TP configuration."""
        info = self.tp_config.get_display_info()

        embed = discord.Embed(
            title="📊 Auto Take-Profit Configuration",
            color=discord.Color.blue(),
        )

        # Defaults
        defaults_lines = []
        for cls, settings in info["defaults"].items():
            emoji = CLASS_EMOJI.get(cls, "📊")
            if settings["type"] == "pips":
                val_str = f"{settings['value']:.1f} pips"
            else:
                val_str = f"${settings['value']:.2f}"
            defaults_lines.append(f"{emoji} **{cls}**: {val_str}")

        embed.add_field(
            name="Defaults",
            value="\n".join(defaults_lines) or "None",
            inline=False,
        )

        # Overrides
        if info["overrides"]:
            override_lines = []
            for sym, ov in info["overrides"].items():
                if ov["type"] == "pips":
                    val_str = f"{ov['value']:.1f} pips"
                else:
                    val_str = f"${ov['value']:.2f}"
                override_lines.append(f"**{sym}**: {val_str} _(by {ov.get('set_by', '?')})_")
            embed.add_field(
                name=f"Per-Symbol Overrides ({info['total_overrides']})",
                value="\n".join(override_lines),
                inline=False,
            )
        else:
            embed.add_field(name="Per-Symbol Overrides", value="None", inline=False)

        embed.set_footer(text="Use !settp <class|symbol> <value> [pips|dollars] to change • !removetp <symbol> to remove override")
        await ctx.send(embed=embed)

    # ------------------------------------------------------------------
    # !settp
    # ------------------------------------------------------------------

    @commands.command(name="settp")
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def set_tp(self, ctx: commands.Context, target: str, value: str, tp_type: str = None):
        """
        Set TP threshold for an asset class or specific symbol. Admin only.

        Usage:
          !settp metals 5           — Set metals default to $5
          !settp XAUUSD 5           — Set XAUUSD override to $5
          !settp forex 10 pips      — Set forex default to 10 pips
          !settp EURUSD 15 pips     — Set EURUSD override to 15 pips

        Valid types: pips, dollars
        If type is omitted, the current type for the asset class is kept.
        """
        try:
            # Parse value
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

            # Resolve TP type
            if tp_type is not None:
                tp_type_lower = tp_type.lower()
                if tp_type_lower not in VALID_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{tp_type}`. Valid types: {', '.join(VALID_TYPES)}"
                    )
                    return
            else:
                # Infer type from current config
                current_type = self.tp_config.get_tp_type(target_upper)
                tp_type_lower = current_type

            # Is it an asset class or a symbol?
            if target_lower in ASSET_CLASSES:
                success = self.tp_config.set_default(
                    target_lower, float_value, tp_type_lower,  # type: ignore
                    set_by=ctx.author.name,
                )
                label = f"**{target_lower}** (default)"
            else:
                success = self.tp_config.set_override(
                    target_upper, float_value, tp_type_lower,  # type: ignore
                    set_by=ctx.author.name,
                )
                label = f"**{target_upper}** (override)"

            if not success:
                await ctx.send(f"❌ Failed to set TP for `{target}`. Check logs for details.")
                return

            # Format display value
            if tp_type_lower == "pips":
                val_display = f"{float_value:.1f} pips"
            else:
                val_display = f"${float_value:.2f}"

            # Reload config on the live monitor if available
            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "tp_config"):
                    self.bot.monitor.tp_config.reload_config()
                    self.bot.monitor.tp_monitor.tp_config = self.bot.monitor.tp_config

            embed = discord.Embed(
                title="✅ TP Configuration Updated",
                color=discord.Color.green(),
            )
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="New TP Threshold", value=val_display, inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in settp command: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting TP: {e}")

    # ------------------------------------------------------------------
    # !removetp
    # ------------------------------------------------------------------

    @commands.command(name="removetp", aliases=["rmtp", "deletetp"])
    @commands.check(lambda ctx: ctx.cog.is_admin(ctx.author))
    async def remove_tp_override(self, ctx: commands.Context, symbol: str):
        """
        Remove a per-symbol TP override, reverting to the asset-class default. Admin only.

        Usage:
          !removetp XAUUSD
        """
        try:
            symbol_upper = symbol.upper()
            removed = self.tp_config.remove_override(symbol_upper)

            # Reload config on live monitor
            if hasattr(self.bot, "monitor") and self.bot.monitor:
                if hasattr(self.bot.monitor, "tp_config"):
                    self.bot.monitor.tp_config.reload_config()
                    self.bot.monitor.tp_monitor.tp_config = self.bot.monitor.tp_config

            if removed:
                # Show what the symbol falls back to
                fallback_type = self.tp_config.get_tp_type(symbol_upper)
                fallback_val = self.tp_config.get_tp_value(symbol_upper)
                fallback_display = self.tp_config.format_value(symbol_upper, fallback_val)
                asset_class = self.tp_config.determine_asset_class(symbol_upper)

                embed = discord.Embed(
                    title="✅ TP Override Removed",
                    color=discord.Color.green(),
                )
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Now Using", value=f"{asset_class} default: {fallback_display}", inline=True)
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"⚠️ No override found for `{symbol_upper}`. "
                    f"It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in removetp command: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing TP override: {e}")


async def setup(bot):
    await bot.add_cog(TPCommands(bot))