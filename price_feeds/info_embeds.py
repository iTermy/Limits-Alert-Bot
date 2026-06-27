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
from typing import Callable, Dict, Optional

import discord

from utils.logger import get_logger

logger = get_logger("info_embeds")

_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "info_embeds.json"

_FOOTER = "Discipline over everything — protect your account first. Small, consistent gains win long-term."

# ── Shared sections ──────────────────────────────────────────────────────────

_MARKERS_AND_TERMS = (
    "**Markers**\n"
    "🟡 Approaching  ·  🎯 Limit hit  ·  🛑 Stop loss\n"
    "❌ Cancelled  ·  ♻️ Reactivated  ·  💰 Profit\n\n"
    "**Terms**\n"
    "• **Approaching** — price is near a limit.\n"
    "• **Near-Miss (NM)** — price reverses a few pips before a limit and runs the "
    "other way; the trade is auto-cancelled.\n"
    "• **VTH (Valid Till Hit)** — stays active until filled.\n"
    "• **HOT / POC** — high-conviction limits to focus size on."
)

_RISK_MANAGEMENT = (
    "• Set your limits in advance — markets move fast.\n"
    "• Risk **max 5%** across *all* limits in a signal (5 limits ≈ 1% each). "
    "Equal lots is simplest for beginners.\n"
    "• Treat every signal as **one trade** — average the whole basket, not each limit alone.\n"
    "• Manage losing positions first; trailing stops (5–10 pips) lock profit without adding risk."
)

_VALIDITY = (
    "• Forex, exotics, indices and tolls — valid **all week**; unfilled limits are "
    "cancelled at week's end.\n"
    "• Swings & stocks — valid **all month**.\n"
    "• New week, new signals (swings & stocks carry over).\n"
    "• Not every limit fills — the ones that hit are worked, the rest expire."
)

_STAY_SAFE = (
    "Only follow official profiles in this server. We will **never** DM you offering "
    "services or help — ignore and report anyone who does."
)


def _base_embed(title: str, description: str, color: int) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text=_FOOTER)
    return embed


# ── Per-channel builders ─────────────────────────────────────────────────────


def _build_main_embed() -> discord.Embed:
    embed = _base_embed(
        "📊 Signal Alerts — How This Channel Works",
        "Every signal posted here becomes **one live embed** that updates itself as "
        "price approaches, hits limits and closes — so the channel stays clean. When "
        "it's quiet, there simply aren't any active trades right now. Here's how to "
        "use it. 👇",
        0x5865F2,
    )
    embed.add_field(
        name="📈 Trade Types",
        value=(
            "♎ **Scalp** — quick in-and-out, 3–10 pips. Don't hold.\n"
            "👑 **Daily** — clean setups, 10–50 pips, fast take-profits.\n"
            "🌂 **Swing** — longer holds, 50–500 pips; exit when structure changes.\n"
            "🔮 **Price Action** — higher risk (~60–70% win), wider stops (own channel)."
        ),
        inline=False,
    )
    embed.add_field(
        name="💸 Taking Profit",
        value=(
            "Overall profit across the basket is what matters — these are guides, not "
            "hard rules:\n"
            "• Scalps — 5–10 pips\n"
            "• Daily forex — 10–20 pips + runners\n"
            "• Forex swings — 50–100–150 pips\n"
            "• SPX — 5–10–15 pts  ·  NASDAQ / US30 / DAX — 10–20–50 pts\n"
            "• Stocks — TP when you see it (unless marked investment/swing)\n"
            "Close lower entries first at breakeven or small profit; protect equity "
            "over chasing pips."
        ),
        inline=False,
    )
    embed.add_field(name="🗓️ Validity & Expiry", value=_VALIDITY, inline=False)
    embed.add_field(name="⚠️ Risk Management", value=_RISK_MANAGEMENT, inline=False)
    embed.add_field(name="🔔 Alert Markers & Terms", value=_MARKERS_AND_TERMS, inline=False)
    embed.add_field(name="🛡️ Stay Safe", value=_STAY_SAFE, inline=False)
    return embed


def _build_pa_embed() -> discord.Embed:
    embed = _base_embed(
        "🔮 Price Action Trades — How This Channel Works",
        "Higher-risk, higher-reward setups (~60–70% win rate) with wider stops. Each "
        "signal is **one live embed** that updates as price moves and closes — a quiet "
        "channel just means nothing is active right now.",
        0x9B59B6,
    )
    embed.add_field(
        name="🎯 How to Take PA Trades",
        value=(
            "• Enter when called — expect spikes/dips around entry.\n"
            "• Keep total risk **under 3%** of your account.\n"
            "• You may add positions if it moves against you, but exit once risk hits 3%.\n"
            "• If price spikes hard right after entry, consider the move done and step aside."
        ),
        inline=False,
    )
    embed.add_field(
        name="💸 Taking Profit",
        value=(
            "Gold PA — **10 / 15 / 25 $**. These are guides: if the setup looks weak, "
            "exit sooner. With a ~60–70% win rate, size for the wider stops and let your "
            "winners cover the losers."
        ),
        inline=False,
    )
    embed.add_field(name="🗓️ Validity & Expiry", value=_VALIDITY, inline=False)
    embed.add_field(name="⚠️ Risk Management", value=_RISK_MANAGEMENT, inline=False)
    embed.add_field(name="🔔 Alert Markers & Terms", value=_MARKERS_AND_TERMS, inline=False)
    embed.add_field(name="🛡️ Stay Safe", value=_STAY_SAFE, inline=False)
    return embed


def _build_gold_toll_embed() -> discord.Embed:
    embed = _base_embed(
        "👑 Gold Tolls — How This Channel Works",
        "Gold toll-map signals — a ladder of limits with tight stops. Each signal is "
        "**one live embed** that updates as price approaches, hits and closes. A quiet "
        "channel just means nothing is active right now.",
        0xF1C40F,
    )
    embed.add_field(
        name="🛑 Stop Loss",
        value=(
            "Default stop is **$5**. In extreme volatility (e.g. $20 one-minute candles) "
            "it widens to **$10–15** — drop your position size to match. Never trade a "
            "$5 stop into a $20 candle."
        ),
        inline=False,
    )
    embed.add_field(
        name="💸 Taking Profit",
        value=(
            "• Impulse limits — **3–5 $**\n"
            "• Runners — **7–10 $**\n"
            "Profit is the basket average across whatever limits hit. Close lower entries "
            "first at breakeven/small profit; hold runners for the larger gains."
        ),
        inline=False,
    )
    embed.add_field(
        name="🔥 HOT / POC Limits",
        value=(
            "**HOT** trades and **POC** limits are the high-conviction levels — size up "
            "there. A toll map lays out a ladder of limits; only the ones price reaches "
            "are worked."
        ),
        inline=False,
    )
    embed.add_field(name="🗓️ Validity & Expiry", value=_VALIDITY, inline=False)
    embed.add_field(name="🔔 Alert Markers & Terms", value=_MARKERS_AND_TERMS, inline=False)
    embed.add_field(name="🛡️ Stay Safe", value=_STAY_SAFE, inline=False)
    return embed


def _build_general_toll_embed() -> discord.Embed:
    embed = _base_embed(
        "🛢️ Tolls — How This Channel Works",
        "Non-gold toll maps (forex, indices and oil). Each signal is **one live embed** "
        "that updates as price approaches, hits and closes. A quiet channel just means "
        "nothing is active right now.",
        0xE67E22,
    )
    embed.add_field(
        name="📐 Prices & Charts",
        value=(
            "Always reconcile your broker's price with the reference chart — oil and "
            "indices usually need adjusting:\n"
            "• Oil — https://www.tradingview.com/symbols/USOILSPOT/\n"
            "• Indices — OANDA spot\n"
            "• Gold / forex — FXCM chart"
        ),
        inline=False,
    )
    embed.add_field(
        name="💸 Taking Profit",
        value=(
            "Profit is the basket average across whatever limits hit. Close lower entries "
            "first at breakeven/small profit; hold runners for the larger gains. These "
            "are guides — exit sooner if the setup turns."
        ),
        inline=False,
    )
    embed.add_field(name="🗓️ Validity & Expiry", value=_VALIDITY, inline=False)
    embed.add_field(name="⚠️ Risk Management", value=_RISK_MANAGEMENT, inline=False)
    embed.add_field(name="🔔 Alert Markers & Terms", value=_MARKERS_AND_TERMS, inline=False)
    embed.add_field(name="🛡️ Stay Safe", value=_STAY_SAFE, inline=False)
    return embed


def _build_legends_embed() -> discord.Embed:
    embed = _base_embed(
        "⭐ Legends Trades — How This Channel Works",
        "Trades shared by TM-trained community members. Each signal is **one live embed** "
        "that updates as price approaches, hits and closes. A quiet channel just means "
        "nothing is active right now.",
        0x1ABC9C,
    )
    embed.add_field(
        name="🧭 What to Expect",
        value=(
            "These are trades from TM-trained community members, across a range of "
            "instruments and styles. Quality is high, but each caller runs their own "
            "plan — follow the **SL and targets on the individual call** and size to "
            "your own risk tolerance."
        ),
        inline=False,
    )
    embed.add_field(
        name="💸 Taking Profit",
        value=(
            "Take profit on overall basket average. As a rough guide: scalps 5–10 pips, "
            "daily forex 10–20 pips + runners, swings 50–150 pips, indices per the "
            "standard point targets. Protect equity over chasing pips."
        ),
        inline=False,
    )
    embed.add_field(name="🗓️ Validity & Expiry", value=_VALIDITY, inline=False)
    embed.add_field(name="⚠️ Risk Management", value=_RISK_MANAGEMENT, inline=False)
    embed.add_field(name="🔔 Alert Markers & Terms", value=_MARKERS_AND_TERMS, inline=False)
    embed.add_field(name="🛡️ Stay Safe", value=_STAY_SAFE, inline=False)
    return embed


class InfoEmbedManager:
    """Posts and maintains the pinned informational embed in each alert channel."""

    def __init__(self, bot, alert_system):
        self.bot = bot
        self.alert_system = alert_system

    def _channel_builders(self) -> Dict[Optional[discord.TextChannel], Callable[[], discord.Embed]]:
        """Map each resolved alert channel to its embed builder, skipping any that
        are unconfigured or share an ID with one already mapped."""
        pairs = [
            (self.alert_system.alert_channel, _build_main_embed),
            (self.alert_system.pa_alert_channel, _build_pa_embed),
            (self.alert_system.toll_alert_channel, _build_gold_toll_embed),
            (self.alert_system.general_toll_alert_channel, _build_general_toll_embed),
            (self.alert_system.legends_alert_channel, _build_legends_embed),
        ]
        mapping: Dict[discord.TextChannel, Callable[[], discord.Embed]] = {}
        seen = set()
        for channel, builder in pairs:
            if channel is None or channel.id in seen:
                continue
            seen.add(channel.id)
            mapping[channel] = builder
        return mapping

    def _load_ids(self) -> Dict[str, int]:
        if not _DATA_PATH.exists():
            return {}
        try:
            with open(_DATA_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read info_embeds.json: {e}")
            return {}

    def _save_ids(self, ids: Dict[str, int]) -> None:
        try:
            _DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_DATA_PATH, "w") as f:
                json.dump(ids, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write info_embeds.json: {e}")

    async def sync(self) -> None:
        """Create or refresh the informational embed in every alert channel."""
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
                    logger.info(f"Refreshed info embed in #{channel.name}")
                    continue
                except discord.NotFound:
                    logger.info(f"Info embed in #{channel.name} was deleted — reposting")
                except Exception as e:
                    logger.warning(f"Could not refresh info embed in #{channel.name}: {e}")
                    continue

            new_id = await self._post(channel, embed)
            if new_id:
                ids[key] = new_id
                changed = True

        if changed:
            self._save_ids(ids)

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
        logger.info(f"Posted info embed in #{channel.name}")
        return msg.id
