"""Volatility guard — informational alerts when a market moves sharply.

Self-contained: registers as a price-stream subscriber, samples mid-price into a
rolling window per subscribed symbol, and posts an embed when a currency (or the
whole board, via gold) becomes volatile. It has no effect on signal processing —
nothing is cancelled or paused; it only announces.

Detection: a symbol is "volatile" when its price range over the trailing
`lookback_seconds` is at least the configured threshold for its asset class
(20 pips for forex, $20 for gold by default). Thresholds are per asset class;
asset classes without a configured threshold are not monitored.

Guard lifecycle, per symbol:
  - First volatility detection arms a guard lasting `guard_minutes` (15 by default).
  - At expiry the volatility check runs again: still volatile -> re-arm another
    window; calm -> release.

Reference counting, per key:
  - A forex pair guards both of its currencies (EURUSD -> EUR, USD).
  - Gold (asset class "metals") guards the special key "ALL" (market-wide).
  - A key stays guarded while at least one contributing symbol is guarded, so EUR
    holds until both EURUSD and EURGBP have released. The activation embed fires
    on the empty -> guarded transition; the ended embed on guarded -> empty.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from datetime import time as dtime
from pathlib import Path
from time import monotonic
from typing import Deque, Dict, Optional, Set, Tuple

import discord
import pytz

from price_feeds.symbol_mapper import SymbolMapper
from utils.logger import get_logger

logger = get_logger("vol_guard")

_EST_TZ = pytz.timezone("America/New_York")

# Ticks whose broker timestamp is older than this (seconds) are ignored — a late
# rollover print could otherwise inject a spread-distorted price into the window.
_MAX_TICK_AGE_SECONDS = 5

# Pip size per asset class — forex/forex_jpy thresholds are expressed in pips,
# everything else in dollars (pip size 1.0).
_PIP_SIZES: Dict[str, float] = {
    "forex": 0.0001,
    "forex_jpy": 0.01,
    "metals": 1.0,
    "indices": 1.0,
    "stocks": 1.0,
    "crypto": 1.0,
    "oil": 1.0,
}

# How often the state machine runs (seconds). The per-tick path only samples;
# all arming/release decisions happen here, off the hot path.
_EVAL_INTERVAL_SECONDS = 5

# Edited "ended" embeds are removed after this delay to keep channels tidy.
_ENDED_DELETE_AFTER_SECONDS = 300

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "vol_guard.json"


def _load_config() -> Dict:
    """Load vol_guard.json, falling back to safe defaults if absent/malformed."""
    defaults = {
        "enabled": True,
        "lookback_seconds": 180,
        "guard_minutes": 15,
        "thresholds": {"forex": 20.0, "forex_jpy": 20.0, "metals": 20.0},
    }
    try:
        raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.info("vol_guard.json not found — using defaults")
        return defaults
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to read vol_guard.json ({e}) — using defaults")
        return defaults
    merged = {**defaults, **raw}
    merged["thresholds"] = raw.get("thresholds", defaults["thresholds"])
    return merged


class VolatilityGuard:
    """Watches subscribed-symbol ticks and announces volatility per currency."""

    def __init__(self, bot, stream_manager, channel: discord.abc.Messageable):
        self.bot = bot
        self.stream_manager = stream_manager
        self.channel = channel
        self.symbol_mapper: SymbolMapper = stream_manager.symbol_mapper

        config = _load_config()
        self.enabled: bool = bool(config.get("enabled", True))
        self.lookback_seconds: float = float(config.get("lookback_seconds", 180))
        self.guard_seconds: float = float(config.get("guard_minutes", 15)) * 60.0
        self.thresholds: Dict[str, float] = config.get("thresholds", {})

        # symbol -> rolling (monotonic_ts, mid_price) samples within the lookback
        self._samples: Dict[str, Deque[Tuple[float, float]]] = {}
        # symbol -> monotonic expiry of its current guard window (present == guarded)
        self._symbol_guards: Dict[str, float] = {}
        # key (currency or "ALL") -> set of guarded symbols contributing to it
        self._key_members: Dict[str, Set[str]] = {}
        # key -> the activation embed message, for editing to "ended"
        self._key_messages: Dict[str, discord.Message] = {}

        self._eval_task: Optional[asyncio.Task] = None

        # Cache for the spread-hour check so the per-tick path doesn't build a
        # tz-aware datetime on every print.
        self._spread_hour_cached: bool = False
        self._spread_hour_cache_expires: float = 0.0

    def start(self) -> None:
        """Register as a price subscriber and start the evaluation loop."""
        if not self.enabled:
            logger.info("Volatility guard disabled in config — not starting")
            return
        self.stream_manager.add_subscriber(self.on_price_update)
        self._eval_task = asyncio.ensure_future(self._eval_loop())
        logger.info(
            "Volatility guard started (lookback=%ss, window=%smin, thresholds=%s)",
            int(self.lookback_seconds),
            int(self.guard_seconds // 60),
            self.thresholds,
        )

    def stop(self) -> None:
        """Stop the evaluation loop (called on bot shutdown)."""
        if self._eval_task is not None and not self._eval_task.done():
            self._eval_task.cancel()
            self._eval_task = None

    # ------------------------------------------------------------------
    # Hot path — sampling only
    # ------------------------------------------------------------------

    async def on_price_update(self, symbol: str, price_data: Dict) -> None:
        """Record a mid-price sample. Keep this cheap — logic runs in the loop."""
        bid = price_data.get("bid")
        ask = price_data.get("ask")
        if bid is None or ask is None:
            return
        # Only sample asset classes we have a threshold for; skip the rest entirely.
        if self._threshold_price_units(symbol) is None:
            return

        # Drop stale ticks — a late rollover print timestamped before the spread
        # hour but delivered after it could carry a distorted price.
        tick_time = price_data.get("updated_at")
        if tick_time is not None:
            if tick_time.tzinfo is None:
                tick_time = tick_time.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - tick_time).total_seconds() > _MAX_TICK_AGE_SECONDS:
                return

        # Don't sample during the daily spread hour: wide, thin books produce
        # erratic prints that look like volatility but aren't real movement.
        if self._is_spread_hour():
            return

        mid = (bid + ask) / 2.0
        now = monotonic()
        samples = self._samples.get(symbol)
        if samples is None:
            samples = deque()
            self._samples[symbol] = samples
        samples.append((now, mid))
        self._prune(samples, now)

    def _prune(self, samples: Deque[Tuple[float, float]], now: float) -> None:
        cutoff = now - self.lookback_seconds
        while samples and samples[0][0] < cutoff:
            samples.popleft()

    def _is_spread_hour(self) -> bool:
        """True during the daily spread hour (17:00–18:00 America/New_York, weekdays).

        Cached for a few seconds and clamped at the hour boundaries so the cache
        never serves a stale value across the 17:00 / 18:00 transitions.
        """
        now_mono = monotonic()
        if now_mono < self._spread_hour_cache_expires:
            return self._spread_hour_cached

        now_est = datetime.now(_EST_TZ)
        if now_est.weekday() >= 5:
            result = False
        else:
            result = dtime(17, 0) <= now_est.time() < dtime(18, 0)

        cache_seconds = 5.0
        for hh in (17, 18):
            boundary = now_est.replace(hour=hh, minute=0, second=0, microsecond=0)
            if boundary > now_est:
                cache_seconds = min(cache_seconds, (boundary - now_est).total_seconds())

        self._spread_hour_cached = result
        self._spread_hour_cache_expires = now_mono + cache_seconds
        return result

    # ------------------------------------------------------------------
    # Evaluation loop — arm / re-arm / release
    # ------------------------------------------------------------------

    async def _eval_loop(self) -> None:
        while True:
            await asyncio.sleep(_EVAL_INTERVAL_SECONDS)
            try:
                await self._evaluate_all()
            except Exception as e:
                logger.error(f"Volatility guard eval loop error: {e}", exc_info=True)

    async def _evaluate_all(self) -> None:
        now = monotonic()
        for symbol in list(self._samples.keys()):
            samples = self._samples[symbol]
            self._prune(samples, now)

            threshold = self._threshold_price_units(symbol)
            volatile = threshold is not None and self._is_volatile(samples, threshold)
            guard_expiry = self._symbol_guards.get(symbol)

            if guard_expiry is None:
                if volatile:
                    await self._arm(symbol, now)
            elif now >= guard_expiry:
                if volatile:
                    self._symbol_guards[symbol] = now + self.guard_seconds
                    logger.info("Volatility guard re-armed for %s (still volatile)", symbol)
                else:
                    await self._release(symbol)

            # Drop idle samples to bound memory once a symbol stops ticking.
            if not samples and symbol not in self._symbol_guards:
                del self._samples[symbol]

    @staticmethod
    def _is_volatile(samples: Deque[Tuple[float, float]], threshold: float) -> bool:
        if len(samples) < 2:
            return False
        prices = [p for _, p in samples]
        return (max(prices) - min(prices)) >= threshold

    async def _arm(self, symbol: str, now: float) -> None:
        self._symbol_guards[symbol] = now + self.guard_seconds
        logger.info("Volatility guard armed for %s", symbol)
        for key in self._keys_for(symbol):
            members = self._key_members.setdefault(key, set())
            was_empty = not members
            members.add(symbol)
            if was_empty:
                await self._send_activation(key, symbol)

    async def _release(self, symbol: str) -> None:
        self._symbol_guards.pop(symbol, None)
        logger.info("Volatility guard released for %s", symbol)
        for key in self._keys_for(symbol):
            members = self._key_members.get(key)
            if not members:
                continue
            members.discard(symbol)
            if not members:
                del self._key_members[key]
                await self._send_ended(key)

    # ------------------------------------------------------------------
    # Threshold + key mapping
    # ------------------------------------------------------------------

    def _threshold_price_units(self, symbol: str) -> Optional[float]:
        """Threshold in absolute price units, or None if the asset class is unmonitored."""
        asset_class = self.symbol_mapper.determine_asset_class(symbol)
        configured = self.thresholds.get(asset_class)
        if configured is None:
            return None
        pip_size = _PIP_SIZES.get(asset_class, 1.0)
        return float(configured) * pip_size

    def _keys_for(self, symbol: str) -> Set[str]:
        """Guard keys a volatile symbol contributes to.

        Gold (metals) flags the whole board as "ALL"; a forex pair flags both of
        its currencies literally.
        """
        asset_class = self.symbol_mapper.determine_asset_class(symbol)
        if asset_class == "metals":
            return {"ALL"}
        if asset_class in ("forex", "forex_jpy"):
            clean = symbol.upper().replace("/", "")
            if len(clean) == 6:
                return {clean[:3], clean[3:]}
        return set()

    # ------------------------------------------------------------------
    # Embeds
    # ------------------------------------------------------------------

    async def _send_activation(self, key: str, symbol: str) -> None:
        if self.channel is None:
            return
        is_all = key == "ALL"
        title = "⚡ Volatility Guard — Market-Wide" if is_all else f"⚡ Volatility Guard — {key}"
        if is_all:
            description = (
                f"Sharp move detected on **{symbol}** — flagging the whole board as volatile."
            )
        else:
            description = f"Sharp move detected on **{key}**."
        embed = discord.Embed(
            title=title,
            description=description,
            color=0xE67E22,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="Informational only • signals are unaffected")
        try:
            msg = await self.channel.send(embed=embed)
            self._key_messages[key] = msg
        except Exception as e:
            logger.error(f"Failed to send volatility activation embed for {key}: {e}")

    async def _send_ended(self, key: str) -> None:
        msg = self._key_messages.pop(key, None)
        if msg is None:
            return
        ended_ts = int(datetime.now(timezone.utc).timestamp())
        label = "Market-Wide" if key == "ALL" else key
        embed = discord.Embed(
            title="⚡ Volatility Guard Ended",
            description=f"Volatility on **{label}** has settled.\n**Ended at <t:{ended_ts}:t>**",
            color=0x808080,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="This message will be deleted in 5 minutes")
        try:
            await msg.edit(embed=embed)
        except Exception as e:
            logger.warning(f"Could not edit volatility embed for {key}: {e}")
            return

        async def _delete_later():
            await asyncio.sleep(_ENDED_DELETE_AFTER_SECONDS)
            try:
                await msg.delete()
            except Exception:
                pass

        asyncio.ensure_future(_delete_later())
