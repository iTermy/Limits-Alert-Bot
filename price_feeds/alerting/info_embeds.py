"""
Persistent informational embeds for the alert channels.

Each alert channel gets one pinned embed explaining what the channel is, what
kinds of signals appear in it, how to take profit, validity rules, alert markers
and risk guidance. Message IDs are persisted to data/info_embeds.json so the
embed is created once and edited in place on subsequent startups instead of
being reposted.
"""

import json
from pathlib import Path
from typing import Callable, Optional

import discord

from utils.logger import get_logger

logger = get_logger("info_embeds")

_DATA_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "info_embeds.json"


def info_embed_message_ids() -> set[str]:
    """Return the set of persisted info-embed message IDs (as strings).

    Used by the weekly channel cleaner to preserve these permanent embeds.
    """
    if not _DATA_PATH.exists():
        return set()
    try:
        with open(_DATA_PATH) as f:
            return {str(v) for v in json.load(f).values()}
    except Exception as e:
        logger.error(f"Failed to read info_embeds.json: {e}")
        return set()

_FOOTER = "Discipline over everything — protect your account first. Small, consistent gains win long-term."


def _base_embed(title: str, description: str, color: int, footer: bool = True) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    if footer:
        embed.set_footer(text=_FOOTER)
    return embed


# ── Per-channel builders ─────────────────────────────────────────────────────


def _build_main_embed() -> discord.Embed:
    embed = _base_embed(
        "👑 Signal Alerts",
        "The main feed — **most signals land here**. Each one is a single live embed "
        "that updates as price approaches, hits and closes. Quiet just means nothing's "
        "active right now.",
        0x5865F2,
    )
    embed.add_field(
        name="What's included",
        value=(
            "**Forex** (scalp / daily / swing) & exotics, **indices**, **oil**, "
            "**stocks** and **crypto** — win rate **>90%**.\n"
            "*Expiry:* scalps & daily setups expire **end of day**; forex, exotics & "
            "indices valid **all week**; swings, stocks & crypto **all month**. Unfilled "
            "limits expire at the end of their window."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "Treat each signal as **one trade** — average the basket, risk **max 5%** "
            "across all limits, equal lots is simplest. Close lower entries first, hold "
            "runners.\n"
            "*Rough TPs:*\n"
            "• Scalp 5–10 pips · Daily 10–20 pips + runners · Swing 50–150 pips\n"
            "• Indices — SPX 5–15 pts · NASDAQ/US30/DAX 10–50 pts\n"
            "• Oil ~$0.5 · Crypto ~$500 (BTCUSDT; varies by symbol)\n"
            "• Stocks — ranges vary per name, **use discretion**\n"
            "*Prices:* indices & oil via **OANDA**, crypto via **Binance** — find the "
            "exact symbol on TradingView."
        ),
        inline=False,
    )
    embed.add_field(
        name="Reading the embeds",
        value=(
            "🟡 Approaching · 🎯 Hit · 🛑 Stop · ❌ Cancelled · ♻️ Reactivated · 💰 Profit"
        ),
        inline=False,
    )
    return embed


def _build_pa_embed() -> discord.Embed:
    embed = _base_embed(
        "🔮 Price Action Trades",
        "Higher-risk, higher-reward gold setups with wider stops.",
        0x9B59B6,
    )
    embed.add_field(
        name="What's included",
        value=(
            "Gold price-action calls only — roughly **60–70% win rate** with wider stops "
            "than the main feed. Fewer, more selective setups. Valid for the session "
            "they're called in."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "• Enter when called — expect spikes/dips around entry.\n"
            "• Keep total risk **under 3%** of your account.\n"
            "• You may add if it moves against you, but exit once risk hits 3%.\n"
            "• If price spikes hard right after entry, treat the move as done and step aside.\n"
            "*TPs:* Gold PA **10 / 15 / 25 $** — tp is more discretionary; exit sooner if "
            "the setup looks weak."
        ),
        inline=False,
    )
    return embed


def _build_gold_toll_embed() -> discord.Embed:
    embed = _base_embed(
        "🪙 Gold Tolls",
        "A collection of gold scalps mapped across a range of limits — one of our "
        "**fan favorites**.",
        0xF1C40F,
    )
    embed.add_field(
        name="What's included",
        value=(
            "Gold scalp limits with a default **$5 stop** (widens to $10–15 in extreme "
            "volatility — drop size to match). Valid **all week**. Win rate ~**90%**. "
            "Monitor the channel for updates, as levels may change."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "Average the basket across whatever limits hit — close lower entries first "
            "at breakeven/small profit, hold runners at your discretion. Equal lots is "
            "simplest.\n"
            "*TPs:* impulse limits **3–5 $** · runners **7–10 $**."
        ),
        inline=False,
    )
    return embed


def _build_general_toll_embed() -> discord.Embed:
    embed = _base_embed(
        "Tolls",
        "Non-gold toll maps — forex, indices and oil. One live embed per signal.",
        0xE67E22,
    )
    embed.add_field(
        name="What's included",
        value=(
            "Toll **maps** on forex, indices and oil. Only the limits price reaches get "
            "worked. Valid **all week**."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "• Always reconcile your broker's price with the reference chart — oil and "
            "indices usually need adjusting:\n"
            "  ◦ Oil → TradingView USOILSPOT\n"
            "  ◦ Indices → OANDA spot\n"
            "  ◦ Gold/forex → FXCM\n"
            "• Average the basket across whatever limits hit — close lower entries first, "
            "hold runners."
        ),
        inline=False,
    )
    return embed


def _build_legends_embed() -> discord.Embed:
    embed = _base_embed(
        "⭐ Legends Trades",
        "Trades shared by TM-trained community members. One live embed per signal.",
        0x1ABC9C,
    )
    embed.add_field(
        name="What's included",
        value=(
            "Calls from TM-trained members across a range of instruments and styles. "
            "Quality is high, but each caller runs their own plan."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "• Follow the **SL and targets on the individual call** — they vary by caller.\n"
            "• Size to your own risk tolerance and average the basket as usual."
        ),
        inline=False,
    )
    return embed


def _build_risky_embed() -> discord.Embed:
    embed = _base_embed(
        "🎲 Risky Gold",
        "Higher-risk gold scalps mapped across a range of limits. One live embed per signal.",
        0xE74C3C,
    )
    embed.add_field(
        name="What's included",
        value=(
            "Gold scalp limits with a default **$5 stop** measured from the last limit. "
            "Valid **all week**. Higher risk than the standard tolls — size down accordingly."
        ),
        inline=False,
    )
    embed.add_field(
        name="🎯 How to take it",
        value=(
            "Average the basket across whatever limits hit — close lower entries first "
            "at breakeven/small profit, hold runners at your discretion. Equal lots is "
            "simplest."
        ),
        inline=False,
    )
    return embed


# ── Monitored-channel reference notices ──────────────────────────────────────


def _build_oil_notice_embed() -> discord.Embed:
    return _base_embed(
        "Oil Trades",
        "All oil signals are based on this chart:\n"
        "[USOILSPOT on TradingView](https://www.tradingview.com/symbols/USOILSPOT/)",
        0x2C3E50,
        footer=False,
    )


def _build_indices_notice_embed() -> discord.Embed:
    return _base_embed(
        "Indices Trades",
        "All indices signals are based on **OANDA charts** unless specified "
        "otherwise. Reference charts:\n"
        "- [SPX](https://www.tradingview.com/symbols/SPX500USD/)\n"
        "- [NAS](https://www.tradingview.com/symbols/OANDA-NAS100USD/)\n"
        "- [DAX](https://www.tradingview.com/symbols/OANDA-DE30EUR/)\n"
        "- [US30](https://www.tradingview.com/symbols/OANDA-US30USD/)\n"
        "- [JP225](https://www.tradingview.com/symbols/OANDA-JP225USD/)",
        0x3498DB,
        footer=False,
    )


# Persistent reference embeds pinned in monitored (signal) channels. Keyed by
# the channels.json monitored-channel name.
_CHANNEL_NOTICES: dict[str, Callable[[], discord.Embed]] = {
    "oil-trades": _build_oil_notice_embed,
    "indices-trades": _build_indices_notice_embed,
}


class InfoEmbedManager:
    """Posts and maintains the pinned informational embed in each alert channel."""

    def __init__(self, bot, alert_system):
        self.bot = bot
        self.alert_system = alert_system

    def _channel_builders(self) -> dict[Optional[discord.TextChannel], Callable[[], discord.Embed]]:
        """Map each resolved alert channel to its embed builder, skipping any that
        are unconfigured or share an ID with one already mapped."""
        pairs = [
            (self.alert_system.alert_channel, _build_main_embed),
            (self.alert_system.pa_alert_channel, _build_pa_embed),
            (self.alert_system.toll_alert_channel, _build_gold_toll_embed),
            (self.alert_system.general_toll_alert_channel, _build_general_toll_embed),
            (self.alert_system.legends_alert_channel, _build_legends_embed),
            (self.alert_system.risky_alert_channel, _build_risky_embed),
        ]
        mapping: dict[discord.TextChannel, Callable[[], discord.Embed]] = {}
        seen = set()
        for channel, builder in pairs:
            if channel is None or channel.id in seen:
                continue
            seen.add(channel.id)
            mapping[channel] = builder
        return mapping

    def _load_ids(self) -> dict[str, int]:
        if not _DATA_PATH.exists():
            return {}
        try:
            with open(_DATA_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read info_embeds.json: {e}")
            return {}

    def _save_ids(self, ids: dict[str, int]) -> None:
        try:
            _DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_DATA_PATH, "w") as f:
                json.dump(ids, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write info_embeds.json: {e}")

    async def sync(self) -> None:
        """Create or refresh the informational embed in every alert channel and
        the reference notices pinned in the monitored signal channels."""
        ids = self._load_ids()
        changed = False

        for channel, builder in self._channel_builders().items():
            embed = builder()
            key = str(channel.id)
            stored_id = ids.get(key)

            if stored_id:
                try:
                    msg = await channel.fetch_message(int(stored_id))
                    await msg.edit(embed=embed)
                    logger.debug(f"Refreshed info embed in #{channel.name}")
                    continue
                except discord.NotFound:
                    logger.debug(f"Info embed in #{channel.name} was deleted — reposting")
                except Exception as e:
                    logger.warning(f"Could not refresh info embed in #{channel.name}: {e}")
                    continue

            new_id = await self._post(channel, embed)
            if new_id:
                ids[key] = new_id
                changed = True

        if await self._sync_notices(ids):
            changed = True

        if changed:
            self._save_ids(ids)

    async def _sync_notices(self, ids: dict[str, int]) -> bool:
        """Post or refresh the pinned reference embed in each configured channel.

        Returns True if any stored ID changed.
        """
        monitored = (self.bot.channels_config or {}).get("monitored_channels", {})
        changed = False

        for name, builder in _CHANNEL_NOTICES.items():
            channel_id = monitored.get(name)
            if not channel_id or not str(channel_id).isdigit():
                continue
            channel = self.bot.get_channel(int(channel_id))
            if channel is None:
                logger.warning(f"Notice channel #{name} ({channel_id}) not found")
                continue

            embed = builder()
            key = f"notice:{channel.id}"
            stored_id = ids.get(key)
            if stored_id:
                try:
                    msg = await channel.fetch_message(int(stored_id))
                    await msg.edit(content=None, embed=embed)
                    logger.debug(f"Refreshed notice in #{channel.name}")
                    continue
                except discord.NotFound:
                    logger.debug(f"Notice in #{channel.name} was deleted — reposting")
                except Exception as e:
                    logger.warning(f"Could not refresh notice in #{channel.name}: {e}")
                    continue

            new_id = await self._post_notice(channel, embed)
            if new_id:
                ids[key] = new_id
                changed = True

        return changed

    async def _post_notice(self, channel: discord.TextChannel, embed: discord.Embed) -> Optional[int]:
        try:
            msg = await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to post notice in #{channel.name}: {e}")
            return None
        try:
            await msg.pin()
        except Exception as e:
            logger.warning(f"Posted notice in #{channel.name} but could not pin it: {e}")
        logger.debug(f"Posted notice in #{channel.name}")
        return msg.id

    async def _post(self, channel: discord.TextChannel, embed: discord.Embed) -> Optional[int]:
        try:
            msg = await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to post info embed in #{channel.name}: {e}")
            return None
        try:
            await msg.pin()
        except Exception as e:
            logger.warning(f"Posted info embed in #{channel.name} but could not pin it: {e}")
        logger.debug(f"Posted info embed in #{channel.name}")
        return msg.id
