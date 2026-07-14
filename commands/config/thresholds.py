"""
Config Commands — runtime configuration for TP, alert distances, and near-miss thresholds.
"""

from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands

from database.config_history_ops import log_config_change
from price_feeds.config.alert_config import AlertDistanceConfig
from price_feeds.config.nm_config import NMConfig
from price_feeds.config.tp_config import VALID_SIGNAL_TYPES, TPConfig

from .._base import BaseCog

ASSET_CLASSES = frozenset(["forex", "forex_jpy", "metals", "indices", "stocks", "crypto", "oil"])
VALID_TP_TYPES = ["pips", "dollars"]
VALID_DIST_TYPES = ["pips", "dollars", "percentage"]
VALID_NM_TYPES = frozenset(["pips", "dollars"])
SIGNAL_TYPE_SET = frozenset(VALID_SIGNAL_TYPES)
# Command targets that map to the risky-gold NM type override (always metals).
NM_RISKY_TARGETS = frozenset(["risky", "risky-gold"])


def _extract_type_flag(args):
    """Pop a --type=X argument from args (if present). Returns (signal_type, remaining_args)."""
    signal_type = None
    remaining = []
    for a in args:
        if isinstance(a, str) and a.lower().startswith("--type="):
            signal_type = a.split("=", 1)[1].lower()
        else:
            remaining.append(a)
    return signal_type, remaining


class ThresholdsCog(BaseCog):
    """Runtime configuration commands for TP, alert distance, and near-miss thresholds"""

    def __init__(self, bot):
        super().__init__(bot)
        self.tp_config = TPConfig()
        self.alert_dist_config = AlertDistanceConfig()
        self.nm_config = NMConfig()

    # ── Take-Profit commands ───────────────────────────────────────────────────

    @commands.command(name="tp")
    async def tp_command(self, ctx: commands.Context, subcommand: Optional[str] = None, *args):
        """
        Take-profit configuration (per signal type: standard, scalp, swing, toll, pa, 1-1, risky).

          !tp config                                   — Show all type defaults
          !tp config <symbol> [--type=X]               — Show effective config for a symbol
          !tp config <type>                            — Show one type's full config
          !tp set <type> <asset_class> <value> [pips|dollars]   — Set type's asset-class default (admin)
          !tp set <symbol> <value> [pips|dollars] [--type=X]    — Set per-symbol override (admin)
          !tp remove <symbol> [--type=X]               — Remove per-symbol override (admin)

        See !help tp for full details.
        """
        if subcommand is None:
            await ctx.send(
                "Usage: `!tp config`, `!tp set`, `!tp remove` — see `!help tp` for details."
            )
            return

        sub = subcommand.lower()
        signal_type, args = _extract_type_flag(args)

        if sub == "config":
            target = args[0] if args else None
            await self._tp_show(ctx, target, signal_type)

        elif sub == "set":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            await self._tp_set(ctx, args, signal_type)

        elif sub == "remove":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if not args:
                await ctx.send("❌ Usage: `!tp remove <symbol> [--type=X]`")
                return
            await self._tp_remove(ctx, args[0], signal_type or "standard")

        else:
            await ctx.send(f"❌ Unknown subcommand `{subcommand}`. See `!help tp` for usage.")

    @staticmethod
    def _fmt_tp_entry(cfg):
        return (
            f"{cfg['value']:.1f} pips"
            if cfg.get("type") == "pips"
            else f"${cfg.get('value', 0):.2f}"
        )

    async def _tp_show(self, ctx: commands.Context, target: Optional[str] = None, signal_type: Optional[str] = None):
        try:
            info = self.tp_config.get_display_info()
            type_defaults = info["type_defaults"]
            type_overrides = info["type_overrides"]

            # No target → show every type's asset-class defaults.
            if target is None:
                embed = discord.Embed(
                    title="Auto Take-Profit Configuration",
                    color=discord.Color.blue(),
                    description="Per-type defaults. Use `!tp config <type>` for one type's details.",
                )
                for t in VALID_SIGNAL_TYPES:
                    lines = []
                    for ac, cfg in type_defaults.get(t, {}).items():
                        lines.append(f"**{ac}**: {self._fmt_tp_entry(cfg)}")
                    override_count = len(type_overrides.get(t, {}))
                    suffix = f" · {override_count} override(s)" if override_count else ""
                    embed.add_field(
                        name=f"{t}{suffix}",
                        value="\n".join(lines) if lines else "_inherits standard_",
                        inline=False,
                    )
                embed.set_footer(text="Use !tp set and !tp remove to manage thresholds")
                await ctx.send(embed=embed)
                return

            target_lower = target.lower()

            # Target is a signal type → show its full config.
            if target_lower in SIGNAL_TYPE_SET:
                embed = discord.Embed(
                    title=f"TP Config — type: {target_lower}",
                    color=discord.Color.blue(),
                )
                defaults_lines = [
                    f"**{ac}**: {self._fmt_tp_entry(cfg)}"
                    for ac, cfg in type_defaults.get(target_lower, {}).items()
                ]
                embed.add_field(
                    name="Asset-Class Defaults",
                    value="\n".join(defaults_lines) or "_inherits standard_",
                    inline=False,
                )
                overrides = type_overrides.get(target_lower, {})
                if overrides:
                    ov_lines = [
                        f"**{sym}**: {self._fmt_tp_entry(ov)} _(by {ov.get('set_by', '?')})_"
                        for sym, ov in overrides.items()
                    ]
                    embed.add_field(
                        name=f"Per-Symbol Overrides ({len(overrides)})",
                        value="\n".join(ov_lines),
                        inline=False,
                    )
                else:
                    embed.add_field(name="Per-Symbol Overrides", value="None", inline=False)
                await ctx.send(embed=embed)
                return

            # Otherwise the target is a symbol.
            resolved_type = (signal_type or "standard").lower()
            if resolved_type not in SIGNAL_TYPE_SET:
                await ctx.send(
                    f"❌ Unknown signal type `{resolved_type}`. Valid: {', '.join(VALID_SIGNAL_TYPES)}"
                )
                return

            symbol = target.upper()
            sym_info = self.tp_config.get_display_info(symbol, signal_type=resolved_type)
            value_str = self.tp_config.format_value(symbol, sym_info["value"])

            embed = discord.Embed(
                title=f"TP Config — {sym_info['symbol']} ({resolved_type})",
                color=discord.Color.blue(),
            )
            embed.add_field(name="Asset Class", value=sym_info["asset_class"], inline=True)
            embed.add_field(name="TP Threshold", value=value_str, inline=True)
            embed.add_field(
                name="Source",
                value="Override" if sym_info["is_override"] else "Default",
                inline=True,
            )

            if sym_info["is_override"]:
                embed.add_field(
                    name="Set By", value=sym_info.get("set_by", "Unknown"), inline=True
                )
                set_at = sym_info.get("set_at", "")
                if set_at:
                    try:
                        dt = datetime.fromisoformat(set_at.replace("Z", "+00:00"))
                        embed.add_field(
                            name="Set At", value=dt.strftime("%Y-%m-%d %H:%M UTC"), inline=True
                        )
                    except Exception:
                        embed.add_field(name="Set At", value=set_at[:19], inline=True)

            embed.set_footer(
                text="Auto-TP triggers when last limit hits threshold and earlier limits are combined breakeven"
            )
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in tp config: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching TP config: {e}")

    async def _tp_set(self, ctx: commands.Context, args, signal_type_flag: Optional[str] = None):
        """
        !tp set <type> <asset_class> <value> [pips|dollars]
        !tp set <asset_class> <value> [pips|dollars]                  # implies type=standard
        !tp set <symbol> <value> [pips|dollars] [--type=X]
        """
        try:
            if len(args) < 2:
                await ctx.send(
                    "❌ Usage: `!tp set <type> <asset_class> <value> [pips|dollars]` "
                    "or `!tp set <symbol> <value> [pips|dollars] [--type=X]`"
                )
                return

            first = args[0].lower()

            # First arg is a signal type → type-default form.
            if first in SIGNAL_TYPE_SET:
                if len(args) < 3:
                    await ctx.send(
                        "❌ Usage: `!tp set <type> <asset_class> <value> [pips|dollars]`"
                    )
                    return
                signal_type = first
                asset_class = args[1].lower()
                value_arg = args[2]
                tp_type_arg = args[3] if len(args) >= 4 else None

                if asset_class not in ASSET_CLASSES:
                    await ctx.send(
                        f"❌ Unknown asset class `{asset_class}`. Valid: {', '.join(sorted(ASSET_CLASSES))}"
                    )
                    return

                try:
                    float_value = float(value_arg)
                except ValueError:
                    await ctx.send(f"❌ Invalid value `{value_arg}` — must be a number.")
                    return
                if float_value <= 0:
                    await ctx.send("❌ TP value must be positive.")
                    return

                tp_type = (tp_type_arg or "").lower() if tp_type_arg else self.tp_config.ASSET_CLASS_TYPES.get(asset_class, "dollars")
                if tp_type not in VALID_TP_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{tp_type_arg}`. Valid types: {', '.join(VALID_TP_TYPES)}"
                    )
                    return

                history_key = f"{signal_type}/{asset_class}"
                old_value = self.tp_config.config.get("type_defaults", {}).get(signal_type, {}).get(asset_class)
                success = self.tp_config.set_default(
                    asset_class, float_value, tp_type, set_by=ctx.author.name, signal_type=signal_type
                )
                label = f"**{signal_type} / {asset_class}** (default)"
                val_display = (
                    f"{float_value:.1f} pips" if tp_type == "pips" else f"${float_value:.2f}"
                )

            # First arg is an asset class → implies signal_type=standard.
            elif first in ASSET_CLASSES:
                value_arg = args[1]
                tp_type_arg = args[2] if len(args) >= 3 else None
                try:
                    float_value = float(value_arg)
                except ValueError:
                    await ctx.send(f"❌ Invalid value `{value_arg}` — must be a number.")
                    return
                if float_value <= 0:
                    await ctx.send("❌ TP value must be positive.")
                    return
                signal_type = (signal_type_flag or "standard").lower()
                if signal_type not in SIGNAL_TYPE_SET:
                    await ctx.send(
                        f"❌ Unknown signal type `{signal_type}`. Valid: {', '.join(VALID_SIGNAL_TYPES)}"
                    )
                    return
                tp_type = (tp_type_arg or "").lower() if tp_type_arg else self.tp_config.ASSET_CLASS_TYPES.get(first, "dollars")
                if tp_type not in VALID_TP_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{tp_type_arg}`. Valid types: {', '.join(VALID_TP_TYPES)}"
                    )
                    return

                history_key = f"{signal_type}/{first}"
                old_value = self.tp_config.config.get("type_defaults", {}).get(signal_type, {}).get(first)
                success = self.tp_config.set_default(
                    first, float_value, tp_type, set_by=ctx.author.name, signal_type=signal_type
                )
                label = f"**{signal_type} / {first}** (default)"
                val_display = (
                    f"{float_value:.1f} pips" if tp_type == "pips" else f"${float_value:.2f}"
                )

            # First arg is a symbol → per-symbol override.
            else:
                symbol = first.upper()
                value_arg = args[1]
                tp_type_arg = args[2] if len(args) >= 3 else None
                try:
                    float_value = float(value_arg)
                except ValueError:
                    await ctx.send(f"❌ Invalid value `{value_arg}` — must be a number.")
                    return
                if float_value <= 0:
                    await ctx.send("❌ TP value must be positive.")
                    return
                signal_type = (signal_type_flag or "standard").lower()
                if signal_type not in SIGNAL_TYPE_SET:
                    await ctx.send(
                        f"❌ Unknown signal type `{signal_type}`. Valid: {', '.join(VALID_SIGNAL_TYPES)}"
                    )
                    return
                tp_type = (tp_type_arg or "").lower() if tp_type_arg else self.tp_config.get_tp_type(symbol, signal_type=signal_type)
                if tp_type not in VALID_TP_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{tp_type_arg}`. Valid types: {', '.join(VALID_TP_TYPES)}"
                    )
                    return

                history_key = f"{signal_type}/{symbol}"
                old_value = self.tp_config.config.get("type_overrides", {}).get(signal_type, {}).get(symbol)
                success = self.tp_config.set_override(
                    symbol, float_value, tp_type, set_by=ctx.author.name, signal_type=signal_type
                )
                label = f"**{symbol}** ({signal_type} override)"
                val_display = (
                    f"{float_value:.1f} pips" if tp_type == "pips" else f"${float_value:.2f}"
                )

            if not success:
                await ctx.send("❌ Failed to set TP. Check logs for details.")
                return

            await log_config_change(
                self.signal_db.db, "tp", history_key, old_value,
                {"value": float_value, "type": tp_type}, ctx.author.name,
            )

            if self.services.monitor:
                self.services.tp_config.reload_config()
                self.services.tp_monitor.tp_config = self.services.tp_config

            embed = discord.Embed(title="TP Configuration Updated", color=discord.Color.green())
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="New TP Threshold", value=val_display, inline=True)
            embed.set_footer(text=f"Set by {ctx.author.name}")
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in tp set: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting TP: {e}")

    async def _tp_remove(self, ctx: commands.Context, symbol: str, signal_type: str = "standard"):
        try:
            signal_type = signal_type.lower()
            if signal_type not in SIGNAL_TYPE_SET:
                await ctx.send(
                    f"❌ Unknown signal type `{signal_type}`. Valid: {', '.join(VALID_SIGNAL_TYPES)}"
                )
                return

            symbol_upper = symbol.upper()
            old_value = self.tp_config.config.get("type_overrides", {}).get(signal_type, {}).get(symbol_upper)
            removed = self.tp_config.remove_override(symbol_upper, signal_type=signal_type)

            if removed:
                await log_config_change(
                    self.signal_db.db, "tp", f"{signal_type}/{symbol_upper}",
                    old_value, None, ctx.author.name,
                )

            if self.services.monitor:
                self.services.tp_config.reload_config()
                self.services.tp_monitor.tp_config = self.services.tp_config

            if removed:
                fallback_val = self.tp_config.get_tp_value(symbol_upper, signal_type=signal_type)
                fallback_display = self.tp_config.format_value(symbol_upper, fallback_val)
                asset_class = self.tp_config.determine_asset_class(symbol_upper)

                embed = discord.Embed(title="TP Override Removed", color=discord.Color.green())
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(name="Type", value=signal_type, inline=True)
                embed.add_field(
                    name="Now Using",
                    value=f"{asset_class} default: {fallback_display}",
                    inline=True,
                )
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No `{signal_type}` override found for `{symbol_upper}`. It was already using the default."
                )

        except Exception as e:
            self.logger.error(f"Error in tp remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing TP override: {e}")

    # ── Alert Distance commands ────────────────────────────────────────────────

    @commands.command(
        name="alertdist",
        aliases=["alertdistance", "adist"],
        description="View or manage approaching-alert distance configuration",
    )
    async def alertdist_command(self, ctx: commands.Context, subcommand: Optional[str] = None, *args):
        """
        View and manage alert distance thresholds.

        Usage:
            !alertdist config [symbol]          — Show config (all, or for one symbol)
            !alertdist set <target> <value> [type]  — Set threshold (admin)
            !alertdist remove <symbol>          — Remove per-symbol override (admin)

        See !help alertdist for full details.
        """
        if not subcommand:
            await ctx.send(
                "Usage: `!alertdist config`, `!alertdist set`, `!alertdist remove` — see `!help alertdist` for details."
            )
            return

        sub = subcommand.lower()

        if sub == "config":
            symbol = args[0] if args else None
            await self._adist_show(ctx, symbol)

        elif sub == "set":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if len(args) < 2:
                await ctx.send(
                    "❌ Usage: `!alertdist set <target> <value> [pips|dollars|percentage]`"
                )
                return
            target = args[0]
            value = args[1]
            dist_type = args[2] if len(args) >= 3 else None
            await self._adist_set(ctx, target, value, dist_type)

        elif sub == "remove":
            if not self.is_admin(ctx.author):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            if not args:
                await ctx.send("❌ Usage: `!alertdist remove <symbol>`")
                return
            await self._adist_remove(ctx, args[0])

        else:
            await ctx.send(
                f"❌ Unknown subcommand `{subcommand}`. See `!help alertdist` for usage."
            )

    async def _adist_show(self, ctx: commands.Context, symbol: Optional[str] = None):
        try:
            if symbol:
                symbol_upper = symbol.upper()
                info = self.alert_dist_config.get_config_display(symbol_upper)
                asset_class = info["asset_class"]
                dist_type = info["type"]
                value = info["value"]
                is_override = info["is_override"]

                if dist_type == "dollars":
                    val_str = f"${value}"
                elif dist_type == "percentage":
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
                    embed.add_field(name="Set By", value=info.get("set_by", "Unknown"), inline=True)

                embed.set_footer(text="Use !alertdist set / remove to manage thresholds")
                await ctx.send(embed=embed)

            else:
                info = self.alert_dist_config.get_config_display()
                defaults = info["defaults"]
                overrides = info["overrides"]

                def _fmt_val(t, v):
                    if t == "dollars":
                        return f"${v}"
                    if t == "percentage":
                        return f"{v}%"
                    return f"{v} pips"

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

                PAGE_SIZE = 15
                ov_items = sorted(overrides.items())
                total_pages = (len(ov_items) + PAGE_SIZE - 1) // PAGE_SIZE

                for page_num, start in enumerate(range(0, len(ov_items), PAGE_SIZE), start=1):
                    chunk = ov_items[start : start + PAGE_SIZE]
                    lines = [
                        f"`{sym}` → {_fmt_val(cfg['type'], cfg['value'])}" for sym, cfg in chunk
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

    async def _adist_set(
        self, ctx: commands.Context, target: str, value: str, dist_type: Optional[str] = None
    ):
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

            if dist_type is not None:
                dist_type_lower = dist_type.lower()
                if dist_type_lower not in VALID_DIST_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{dist_type}`. Valid types: pips, dollars, percentage"
                    )
                    return
            elif target_lower in ASSET_CLASSES:
                existing = self.alert_dist_config.config["defaults"].get(target_lower, {})
                dist_type_lower = existing.get("type", "pips")
            else:
                existing_cfg = self.alert_dist_config._get_config_for_symbol(target_upper)
                dist_type_lower = existing_cfg.get("type", "pips")

            if target_lower in ASSET_CLASSES:
                if target_lower not in self.alert_dist_config.config["defaults"]:
                    await ctx.send(
                        f"❌ Unknown asset class `{target_lower}`. Valid: {', '.join(sorted(ASSET_CLASSES))}"
                    )
                    return
                old_value = dict(self.alert_dist_config.config["defaults"][target_lower])
                history_key = target_lower
                self.alert_dist_config.config["defaults"][target_lower]["value"] = float_value
                self.alert_dist_config.config["defaults"][target_lower]["type"] = dist_type_lower
                self.alert_dist_config._save_config()
                label = f"**{target_lower}** (default)"
            else:
                old_value = self.alert_dist_config.config.get("overrides", {}).get(target_upper)
                history_key = target_upper
                success = self.alert_dist_config.set_override(
                    target_upper, float_value, dist_type_lower, set_by=ctx.author.name
                )
                if not success:
                    await ctx.send(f"❌ Failed to set alert distance for `{target}`. Check logs.")
                    return
                label = f"**{target_upper}** (override)"

            await log_config_change(
                self.signal_db.db, "alertdist", history_key, old_value,
                {"value": float_value, "type": dist_type_lower}, ctx.author.name,
            )

            if dist_type_lower == "dollars":
                val_display = f"${float_value}"
            elif dist_type_lower == "percentage":
                val_display = f"{float_value}%"
            else:
                val_display = f"{float_value} pips"

            if self.services.monitor:
                self.services.alert_config.reload_config()

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
        try:
            symbol_upper = symbol.upper()
            old_value = self.alert_dist_config.config.get("overrides", {}).get(symbol_upper)
            removed = self.alert_dist_config.remove_override(symbol_upper)

            if self.services.monitor:
                self.services.alert_config.reload_config()

            if removed:
                await log_config_change(
                    self.signal_db.db, "alertdist", symbol_upper, old_value, None, ctx.author.name,
                )
                fallback_cfg = self.alert_dist_config._get_config_for_symbol(symbol_upper)
                t = fallback_cfg["type"]
                v = fallback_cfg["value"]
                if t == "dollars":
                    fallback_str = f"${v}"
                elif t == "percentage":
                    fallback_str = f"{v}%"
                else:
                    fallback_str = f"{v} pips"
                asset_class = self.alert_dist_config._determine_asset_class(symbol_upper)

                embed = discord.Embed(
                    title="Alert Distance Override Removed", color=discord.Color.green()
                )
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(
                    name="Now Using", value=f"{asset_class} default: {fallback_str}", inline=True
                )
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No override found for `{symbol_upper}`. It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in alertdist remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing alert distance override: {e}")

    # ── Near-Miss (NM) configuration commands ─────────────────────────────────

    @commands.command(name="nmconfig", aliases=["nmc", "nm_config"])
    async def nm_config_command(self, ctx: commands.Context, subcommand: Optional[str] = None, *args):
        """
        Near-miss auto-cancel configuration (linear bounce model).

          !nmconfig show [symbol|risky]                                   — Show NM config
          !nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]  — Set (admin)
          !nmconfig remove <symbol|risky>                                 — Remove override (admin)

        <target> is an asset class, a symbol, or `risky` (risky-gold signals).

        The required bounce scales linearly: required = closest_distance + base_bounce
        So price that got within 2 pips needs less bounce than one that stayed 6 pips away.

        Examples:
          !nmconfig show risky
          !nmconfig set XAUUSD 6 3 dollars      (within $6; bounce = closest + $3)
          !nmconfig set forex 7 4 pips          (within 7 pips; bounce = closest + 4 pips)
          !nmconfig set risky 8 1.5             (risky gold: within $8; bounce = closest + $1.5)
          !nmconfig remove risky
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
                await ctx.send(
                    "❌ Usage: `!nmconfig set <target> <proximity> <bounce> [pips|dollars]`"
                )
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

    async def _nm_show(self, ctx: commands.Context, symbol: Optional[str] = None):
        try:
            if symbol and symbol.lower() in NM_RISKY_TARGETS:
                info = self.nm_config.get_params_display("XAUUSD", signal_type="risky")
                is_override = "metals" in self.nm_config.get_type_overrides("risky")
                embed = discord.Embed(
                    title="NM Config — Risky Gold",
                    color=discord.Color.orange(),
                    description=(
                        "**Formula:** `required_bounce = closest_distance + base_bounce`\n"
                        "Applies to `type=risky` signals (risky-gold channel)."
                    ),
                )
                embed.add_field(name="Max Proximity", value=f"${info['max_proximity']}", inline=True)
                embed.add_field(name="Base Bounce", value=f"${info['base_bounce']}", inline=True)
                embed.add_field(
                    name="Source",
                    value="Type override" if is_override else "Metals default",
                    inline=True,
                )
                embed.add_field(
                    name="Curve Preview",
                    value=f"```\n{self.nm_config.describe_curve('XAUUSD', signal_type='risky')}\n```",
                    inline=False,
                )
                embed.set_footer(text="!nmconfig set risky <max_proximity> <base_bounce> to adjust")
                await ctx.send(embed=embed)
                return

            if symbol:
                symbol = symbol.upper()
                info = self.nm_config.get_params_display(symbol)
                nm_type = info["type"]
                unit = "pips" if nm_type == "pips" else "$"

                prox_str = (
                    f"{info['max_proximity']} {unit}"
                    if nm_type == "pips"
                    else f"${info['max_proximity']}"
                )
                base_str = (
                    f"{info['base_bounce']} {unit}"
                    if nm_type == "pips"
                    else f"${info['base_bounce']}"
                )

                is_override = symbol in self.nm_config.get_all_overrides()

                embed = discord.Embed(
                    title=f"NM Config — {symbol}",
                    color=discord.Color.orange(),
                    description=(
                        "**Formula:** `required_bounce = closest_distance + base_bounce`\n"
                        "Price must enter the proximity zone first; any bounce beyond this formula triggers an NM."
                    ),
                )
                embed.add_field(name="Max Proximity", value=prox_str, inline=True)
                embed.add_field(name="Base Bounce", value=base_str, inline=True)
                embed.add_field(
                    name="Source", value="Override" if is_override else "Default", inline=True
                )
                embed.add_field(
                    name="Curve Preview",
                    value=f"```\n{self.nm_config.describe_curve(symbol)}\n```",
                    inline=False,
                )
                if info.get("description"):
                    embed.add_field(name="Note", value=info["description"], inline=False)
                embed.set_footer(
                    text="!nmconfig set to adjust | closer approach = less bounce needed"
                )
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
                            override_lines.append(
                                f"**{sym}**: {p} pip / +{b} pip base _(by {set_by})_"
                            )
                        else:
                            override_lines.append(f"**{sym}**: ${p} / +${b} base _(by {set_by})_")
                    embed.add_field(
                        name=f"Per-Symbol Overrides ({len(overrides)})",
                        value="\n".join(override_lines),
                        inline=False,
                    )
                else:
                    embed.add_field(name="Per-Symbol Overrides", value="None", inline=False)

                risky = self.nm_config.get_type_overrides("risky")
                if risky:
                    risky_lines = []
                    for key, ov in risky.items():
                        p = ov.get("max_proximity", 0)
                        b = ov.get("base_bounce", 0)
                        risky_lines.append(f"**{key}**: ${p} / +${b} base")
                    embed.add_field(
                        name="Risky Gold", value="\n".join(risky_lines), inline=False
                    )

                embed.set_footer(
                    text="Use !nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]"
                )
                await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in nmconfig show: {e}", exc_info=True)
            await ctx.send(f"❌ Error fetching NM config: {e}")

    async def _nm_set(
        self, ctx, target: str, proximity_str: str, bounce_str: Optional[str] = None, nm_type: Optional[str] = None
    ):
        try:
            try:
                max_proximity = float(proximity_str)
            except (ValueError, TypeError):
                await ctx.send(f"❌ Invalid max_proximity `{proximity_str}` — must be a number.")
                return

            if bounce_str is None:
                await ctx.send(
                    "❌ Usage: `!nmconfig set <target> <max_proximity> <base_bounce> [pips|dollars]`"
                )
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
                if nm_type not in VALID_NM_TYPES:
                    await ctx.send(
                        f"❌ Invalid type `{nm_type}`. Valid: {', '.join(sorted(VALID_NM_TYPES))}"
                    )
                    return

            target_lower = target.lower()
            target_upper = target.upper()

            if target_lower in NM_RISKY_TARGETS:
                history_key = "risky/metals"
                old_value = self.nm_config.config.get("type_overrides", {}).get("risky", {}).get("metals")
                success = self.nm_config.set_type_override(
                    "risky", "metals", max_proximity, base_bounce, nm_type or "dollars",
                    set_by=ctx.author.name,
                )
                label = "**risky gold** (type override)"
            elif target_lower in ASSET_CLASSES:
                history_key = target_lower
                old_value = self.nm_config.config.get("defaults", {}).get(target_lower)
                success = self.nm_config.set_default(
                    target_lower, max_proximity, base_bounce, nm_type, set_by=ctx.author.name
                )
                label = f"**{target_lower}** (default)"
            else:
                history_key = target_upper
                old_value = self.nm_config.config.get("overrides", {}).get(target_upper)
                success = self.nm_config.set_override(
                    target_upper, max_proximity, base_bounce, nm_type, set_by=ctx.author.name
                )
                label = f"**{target_upper}** (override)"

            if success:
                await log_config_change(
                    self.signal_db.db, "nm", history_key, old_value,
                    {"max_proximity": max_proximity, "base_bounce": base_bounce, "type": nm_type},
                    ctx.author.name,
                )

            if self.services.monitor:
                self.services.nm_config = NMConfig()
                self.services.nm_monitor.nm_config = self.services.nm_config

            unit = nm_type if nm_type else "?"
            if target_lower in NM_RISKY_TARGETS:
                unit = nm_type or "dollars"
                curve = self.nm_config.describe_curve("XAUUSD", signal_type="risky")
            elif target_lower in ASSET_CLASSES:
                curve = self.nm_config.describe_curve("EURUSD")
            else:
                curve = self.nm_config.describe_curve(target_upper)
            embed = discord.Embed(title="NM Configuration Updated", color=discord.Color.green())
            embed.add_field(name="Target", value=label, inline=True)
            embed.add_field(name="Max Proximity", value=f"{max_proximity} {unit}", inline=True)
            embed.add_field(name="Base Bounce", value=f"{base_bounce} {unit}", inline=True)
            embed.add_field(
                name="Curve Preview",
                value=f"```\n{curve}\n```",
                inline=False,
            )
            embed.set_footer(
                text=f"Set by {ctx.author.name} | required_bounce = closest_distance + base_bounce"
            )
            await ctx.send(embed=embed)

        except Exception as e:
            self.logger.error(f"Error in nmconfig set: {e}", exc_info=True)
            await ctx.send(f"❌ Error setting NM config: {e}")

    async def _nm_remove(self, ctx, symbol: str):
        try:
            if symbol.lower() in NM_RISKY_TARGETS:
                old_value = self.nm_config.config.get("type_overrides", {}).get("risky", {}).get("metals")
                removed = self.nm_config.remove_type_override("risky", "metals")
                if removed:
                    await log_config_change(
                        self.signal_db.db, "nm", "risky/metals", old_value, None, ctx.author.name,
                    )
                if self.services.monitor:
                    self.services.nm_config = NMConfig()
                    self.services.nm_monitor.nm_config = self.services.nm_config
                if removed:
                    info = self.nm_config.get_params_display("XAUUSD", signal_type="risky")
                    embed = discord.Embed(
                        title="NM Override Removed", color=discord.Color.green()
                    )
                    embed.add_field(name="Target", value="risky gold", inline=True)
                    embed.add_field(
                        name="Now Using",
                        value=f"metals default: ${info['max_proximity']} proximity, +${info['base_bounce']} base",
                        inline=True,
                    )
                    await ctx.send(embed=embed)
                else:
                    await ctx.send(
                        "No risky-gold NM override found. It was already using the metals default."
                    )
                return

            symbol_upper = symbol.upper()
            old_value = self.nm_config.config.get("overrides", {}).get(symbol_upper)
            removed = self.nm_config.remove_override(symbol_upper)

            if self.services.monitor:
                self.services.nm_config = NMConfig()
                self.services.nm_monitor.nm_config = self.services.nm_config

            if removed:
                await log_config_change(
                    self.signal_db.db, "nm", symbol_upper, old_value, None, ctx.author.name,
                )
                info = self.nm_config.get_params_display(symbol_upper)
                t = info["type"]
                p, b = info["max_proximity"], info["base_bounce"]
                fallback_str = (
                    f"{p} pip proximity, +{b} pip base"
                    if t == "pips"
                    else f"${p} proximity, +${b} base"
                )
                asset_class = self.nm_config._get_asset_class(symbol_upper)

                embed = discord.Embed(title="NM Override Removed", color=discord.Color.green())
                embed.add_field(name="Symbol", value=symbol_upper, inline=True)
                embed.add_field(
                    name="Now Using", value=f"{asset_class} default: {fallback_str}", inline=True
                )
                await ctx.send(embed=embed)
            else:
                await ctx.send(
                    f"No NM override found for `{symbol_upper}`. It was already using the asset-class default."
                )

        except Exception as e:
            self.logger.error(f"Error in nmconfig remove: {e}", exc_info=True)
            await ctx.send(f"❌ Error removing NM override: {e}")
