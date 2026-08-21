"""Volatility guard — flags instruments that are moving sharply.

Self-contained: registers as a price-stream subscriber, samples mid-price into a
rolling window per subscribed symbol, and mirrors the active guard keys to
`bot_mode_status.vol_guard` plus a single live Discord embed.

The DB column is consumed by each user's execution bot, but only when that user
opts in (`config.volatility_guard`, off by default). When they do, a flagged
instrument has its pending limits cancelled and its filled positions force-closed,
so a false positive costs real money — thresholds are deliberately conservative.

Detection: a symbol is "volatile" when its price range over the trailing
`lookback_seconds` (180 = 3 min) is at least its threshold. Thresholds resolve
per symbol first (`symbol_thresholds`), then per asset class (`thresholds`);
an asset class with no configured threshold is not monitored at all.

Non-forex thresholds are ~2x the execution bot's proximity distance — the same
distance it uses to decide a limit is close enough to place — so an instrument
only flags on a move twice as large as the band its limits sit in. Forex (20
pips) and gold ($20) predate that formula and stay at their tuned values.

Guard lifecycle, per symbol:
  - First volatility detection arms a guard lasting `guard_minutes` (15 by default).
  - At expiry the volatility check runs again: still volatile -> re-arm another
    window; calm -> release.

Reference counting, per key:
  - Every non-metal symbol guards itself (EURUSD -> EURUSD, NAS100USD ->
    NAS100USD), so only that instrument is gated.
  - Gold (asset class "metals") guards the special key "ALL" (market-wide).
  - A key stays guarded while at least one contributing symbol is guarded ("ALL"
    holds while any metal is guarded). The embed lists every active key and is
    deleted once none remain.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from datetime import time as dtime
from pathlib import Path
from time import monotonic

import discord
import pytz

from price_feeds.config.symbol_mapper import SymbolMapper
from utils.logger import get_logger

logger = get_logger("vol_guard")

_EST_TZ = pytz.timezone("America/New_York")

# Ticks whose broker timestamp is older than this (seconds) are ignored — a late
# rollover print could otherwise inject a spread-distorted price into the window.
_MAX_TICK_AGE_SECONDS = 5

# Pip size per asset class — forex/forex_jpy thresholds are expressed in pips,
# everything else in dollars (pip size 1.0).
_PIP_SIZES: dict[str, float] = {
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

_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "vol_guard.json"

# Identifies our own embed in the channel, for the startup purge.
_EMBED_TITLE = "⚡ Volatility Guard"

_DEFAULT_THRESHOLDS = {
    "forex": 20.0,
    "forex_jpy": 20.0,
    "metals": 20.0,
    "indices": 200.0,
    "crypto": 2000.0,
    "oil": 2.0,
    "stocks": 10.0,
}

# Per-symbol thresholds for instruments whose proximity distance differs from their
# asset-class norm — every index (the class default suits the 100-point majority),
# ETH (the crypto default is BTC-priced), and the per-stock distances. Keyed by
# internal (DB) symbol. The shipped vol_guard.json carries the full set derived from
# the execution bot's proximity tables; these are the fallback if it can't be read.
_DEFAULT_SYMBOL_THRESHOLDS = {
    "SPX500USD": 80.0,
    "US500": 80.0,
    "NAS100USD": 200.0,
    "USTEC": 200.0,
    "DE30EUR": 200.0,
    "DE40": 200.0,
    "US30USD": 200.0,
    "US2000USD": 40.0,
    "UK100GBP": 100.0,
    "UK100": 100.0,
    "JP225": 400.0,
    "F40": 80.0,
    "HK50": 200.0,
    "CHINA50": 120.0,
    "CHINAH": 80.0,
    "AUS2000": 80.0,
    "ETHUSDT": 80.0,
}


def _load_config() -> dict:
    """Load vol_guard.json, falling back to safe defaults if absent/malformed."""
    defaults = {
        "enabled": True,
        "lookback_seconds": 180,
        "guard_minutes": 15,
        "thresholds": _DEFAULT_THRESHOLDS,
        "symbol_thresholds": _DEFAULT_SYMBOL_THRESHOLDS,
    }
    try:
        raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.debug("vol_guard.json not found — using defaults")
        return defaults
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to read vol_guard.json ({e}) — using defaults")
        return defaults
    merged = {**defaults, **raw}
    merged["thresholds"] = raw.get("thresholds", defaults["thresholds"])
    merged["symbol_thresholds"] = raw.get("symbol_thresholds", defaults["symbol_thresholds"])
    return merged


class VolatilityGuard:
    """Watches subscribed-symbol ticks and announces volatility per pair."""

    def __init__(self, bot, stream_manager, channel: discord.abc.Messageable, db=None):
        self.bot = bot
        self.stream_manager = stream_manager
        self.channel = channel
        self.db = db
        self.symbol_mapper: SymbolMapper = stream_manager.symbol_mapper

        config = _load_config()
        self.enabled: bool = bool(config.get("enabled", True))
        self.lookback_seconds: float = float(config.get("lookback_seconds", 180))
        self.guard_seconds: float = float(config.get("guard_minutes", 15)) * 60.0
        self.thresholds: dict[str, float] = config.get("thresholds", {})
        self.symbol_thresholds: dict[str, float] = {
            k.upper(): v for k, v in config.get("symbol_thresholds", {}).items()
        }

        # symbol -> rolling (monotonic_ts, mid_price) samples within the lookback
        self._samples: dict[str, deque[tuple[float, float]]] = {}
        # symbol -> monotonic expiry of its current guard window (present == guarded)
        self._symbol_guards: dict[str, float] = {}
        # key (instrument or "ALL") -> set of guarded symbols contributing to it
        self._key_members: dict[str, set[str]] = {}
        # The single live embed listing every active key, and the keys it shows.
        self._message: discord.Message | None = None
        self._displayed_keys: list[str] = []

        self._eval_task: asyncio.Task | None = None

        # Last value written to bot_mode_status.vol_guard. None before the first
        # write; the first reconcile always writes so a stale value left by a crash
        # is corrected.
        self._last_vol_guard_mode: str | None = None
        self._vol_guard_synced: bool = False

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
        logger.debug(
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

    async def on_price_update(self, symbol: str, price_data: dict) -> None:
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

    def _prune(self, samples: deque[tuple[float, float]], now: float) -> None:
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
        result = False if now_est.weekday() >= 5 else dtime(17, 0) <= now_est.time() < dtime(18, 0)

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
        await self._purge_stale_embeds()
        while True:
            await asyncio.sleep(_EVAL_INTERVAL_SECONDS)
            try:
                await self._evaluate_all()
            except Exception as e:
                logger.error(f"Volatility guard eval loop error: {e}", exc_info=True)

    async def _purge_stale_embeds(self) -> None:
        """Delete any guard embed a previous run left behind.

        Guard state is in-memory, so a restart mid-volatility would otherwise strand
        an embed listing symbols that are no longer flagged, with nothing left to
        edit or delete it. Mirrors the first _reconcile_vol_guard_mode write, which
        likewise always corrects a stale column.
        """
        if self.channel is None or self.bot is None or self.bot.user is None:
            return
        try:
            async for msg in self.channel.history(limit=50):
                if (
                    msg.author.id == self.bot.user.id
                    and msg.embeds
                    and msg.embeds[0].title == _EMBED_TITLE
                ):
                    await msg.delete()
                    logger.debug("Purged stale volatility embed from a previous run")
        except Exception as e:
            logger.warning(f"Could not purge stale volatility embeds: {e}")

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
                    self._arm(symbol, now)
            elif now >= guard_expiry:
                if volatile:
                    self._symbol_guards[symbol] = now + self.guard_seconds
                    logger.debug("Volatility guard re-armed for %s (still volatile)", symbol)
                else:
                    self._release(symbol)

            # Drop idle samples to bound memory once a symbol stops ticking.
            if not samples and symbol not in self._symbol_guards:
                del self._samples[symbol]

        await self._reconcile_vol_guard_mode()
        await self._refresh_embed()

    @staticmethod
    def _is_volatile(samples: deque[tuple[float, float]], threshold: float) -> bool:
        if len(samples) < 2:
            return False
        prices = [p for _, p in samples]
        return (max(prices) - min(prices)) >= threshold

    def _arm(self, symbol: str, now: float) -> None:
        self._symbol_guards[symbol] = now + self.guard_seconds
        logger.info("Volatility guard armed for %s", symbol)
        for key in self._keys_for(symbol):
            self._key_members.setdefault(key, set()).add(symbol)

    def _release(self, symbol: str) -> None:
        self._symbol_guards.pop(symbol, None)
        logger.info("Volatility guard released for %s", symbol)
        for key in self._keys_for(symbol):
            members = self._key_members.get(key)
            if not members:
                continue
            members.discard(symbol)
            if not members:
                del self._key_members[key]

    # ------------------------------------------------------------------
    # DB state — mirror active keys to bot_mode_status.vol_guard
    # ------------------------------------------------------------------

    def _compute_vol_guard_value(self) -> str | None:
        """Comma-separated active keys (pairs and/or 'ALL'), or None if calm."""
        keys = [k for k, members in self._key_members.items() if members]
        if not keys:
            return None
        return ", ".join(sorted(keys))

    async def _reconcile_vol_guard_mode(self) -> None:
        """Sync bot_mode_status.vol_guard to the active guard keys.

        Writes only when the value changes (the first call always writes, so a
        stale value left by a crash is corrected).
        """
        if self.db is None:
            return
        value = self._compute_vol_guard_value()
        if self._vol_guard_synced and value == self._last_vol_guard_mode:
            return
        try:
            await self.db.set_vol_guard_mode(value)
            self._last_vol_guard_mode = value
            self._vol_guard_synced = True
        except Exception as e:
            logger.error(f"Failed to reconcile vol_guard in DB: {e}")

    # ------------------------------------------------------------------
    # Threshold + key mapping
    # ------------------------------------------------------------------

    def _threshold_price_units(self, symbol: str) -> float | None:
        """Threshold in absolute price units, or None if the symbol is unmonitored.

        A per-symbol entry wins over its asset class; both are expressed in the
        class's own unit (pips for forex, price units elsewhere).
        """
        asset_class = self.symbol_mapper.determine_asset_class(symbol)
        configured = self.symbol_thresholds.get(symbol.upper())
        if configured is None:
            configured = self.thresholds.get(asset_class)
        if configured is None:
            return None
        return float(configured) * _PIP_SIZES.get(asset_class, 1.0)

    def _keys_for(self, symbol: str) -> set[str]:
        """Guard keys a volatile symbol contributes to.

        Gold (metals) flags the whole board as "ALL"; every other instrument flags
        itself, so only that instrument is gated (EURUSD volatility does not touch
        USDJPY, and NAS100USD does not touch SPX500USD). The key is the internal
        (DB) symbol, which is what each execution bot matches against.
        """
        if self.symbol_mapper.determine_asset_class(symbol) == "metals":
            return {"ALL"}
        return {symbol.upper().replace("/", "")}

    # ------------------------------------------------------------------
    # Embeds
    # ------------------------------------------------------------------

    @staticmethod
    def _build_embed(keys: list[str]) -> discord.Embed:
        listed = ", ".join("Market-wide (gold)" if k == "ALL" else k for k in keys)
        embed = discord.Embed(
            title=_EMBED_TITLE,
            description=f"Sharp moves detected on:\n**{listed}**",
            color=0xE67E22,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="Updates live • clears once markets settle")
        return embed

    async def _refresh_embed(self) -> None:
        """Drive the single live embed to match the active keys.

        One message for the whole guard: edited as instruments come and go, and
        deleted once nothing is volatile. Skipped entirely when the key list is
        unchanged, so a quiet market costs no API calls.
        """
        if self.channel is None:
            return
        keys = sorted(k for k, members in self._key_members.items() if members)
        if keys == self._displayed_keys:
            return

        if not keys:
            await self._clear_embed()
            self._displayed_keys = []
            return

        embed = self._build_embed(keys)
        if self._message is not None:
            try:
                await self._message.edit(embed=embed)
                self._displayed_keys = keys
                return
            except discord.NotFound:
                self._message = None  # deleted out from under us — repost below
            except Exception as e:
                logger.error(f"Failed to edit volatility embed: {e}")
                return

        try:
            self._message = await self.channel.send(embed=embed)
            self._displayed_keys = keys
        except Exception as e:
            logger.error(f"Failed to post volatility embed: {e}")

    async def _clear_embed(self) -> None:
        if self._message is None:
            return
        try:
            await self._message.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            logger.warning(f"Could not delete volatility embed: {e}")
        self._message = None
