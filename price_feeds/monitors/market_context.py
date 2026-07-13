"""
Market-context provider for signal analytics.

Pulls short OHLCV bar windows from the process-global MT5 terminal (the same
one the ICMarkets feed already drives) and derives a handful of features at
limit-hit time: ATR, RSI, EMA-distance (stretch), candle wick rejection, and
higher-timeframe trend alignment. Also exposes a lightweight per-minute volume
sample for time-series collection.

Bar fetches run in an executor so they never block the price-tick loop, and
nothing here touches alerts or signal status — it is pure data collection.

Oil (Exness-only) has no bars on this terminal and returns None; the excursion
MFE/MAE still works for it, only the bar-derived context is skipped.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from utils.logger import get_logger

logger = get_logger("market_context")

try:
    import MetaTrader5 as mt5

    _MT5_AVAILABLE = True
except Exception:  # pragma: no cover - Windows-only dependency
    mt5 = None
    _MT5_AVAILABLE = False

# Periods for the bar-derived features (all computed on closed bars).
_ATR_PERIOD = 14
_RSI_PERIOD = 14
_EMA_PERIOD = 20
_HTF_SLOPE_LOOKBACK = 5  # H1 bars back used to measure trend slope

# Bar counts requested per timeframe (closed-bar windows + the forming bar).
_M5_BARS = 80
_H1_BARS = 40
_M1_BARS = 20


class MarketContextProvider:
    """Derives ATR/RSI/EMA/wick/HTF context and volume samples from MT5 bars."""

    def __init__(self, symbol_mapper):
        self._mapper = symbol_mapper

    def _ic_symbol(self, internal_symbol: str) -> Optional[str]:
        """Resolve the MT5 (ICMarkets) symbol, or None if this terminal lacks it."""
        if not _MT5_AVAILABLE:
            return None
        # Oil routes to Exness, which this terminal doesn't serve.
        if self._mapper.determine_asset_class(internal_symbol) == "oil":
            return None
        return self._mapper.get_feed_symbol(internal_symbol, "icmarkets")

    async def _copy_rates(self, ic_symbol: str, timeframe, count: int) -> Optional[list[dict]]:
        """Fetch `count` recent bars as plain dicts, or None on failure."""
        loop = asyncio.get_event_loop()
        try:
            rates = await loop.run_in_executor(
                None, mt5.copy_rates_from_pos, ic_symbol, timeframe, 0, count
            )
        except Exception as e:
            logger.debug("copy_rates failed for %s: %s", ic_symbol, e)
            return None
        if rates is None or len(rates) < 3:
            return None
        return [
            {
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["tick_volume"]),
            }
            for r in rates
        ]

    async def snapshot(
        self, internal_symbol: str, direction: str, spread: Optional[float] = None
    ) -> Optional[dict]:
        """Context features at limit-hit. Returns None if no bars are available."""
        ic_symbol = self._ic_symbol(internal_symbol)
        if ic_symbol is None:
            return None

        m5 = await self._copy_rates(ic_symbol, mt5.TIMEFRAME_M5, _M5_BARS)
        if not m5:
            return None

        # Drop the forming bar so every feature is computed on closed candles.
        closed = m5[:-1]
        closes = [b["close"] for b in closed]

        atr = self._atr(closed, _ATR_PERIOD)
        rsi = self._rsi(closes, _RSI_PERIOD)
        ema = self._ema(closes, _EMA_PERIOD)
        last = closed[-1]

        ema_distance_atr = None
        if ema is not None and atr:
            ema_distance_atr = (last["close"] - ema) / atr

        wick = self._wick_rejection(last, direction)

        volume_at_hit = last["volume"]
        recent = [b["volume"] for b in closed[-20:]]
        avg_volume = sum(recent) / len(recent) if recent else None
        spike = (volume_at_hit / avg_volume) if avg_volume else None

        htf_trend, htf_aligned = await self._htf_trend(ic_symbol, direction)

        return {
            "atr_at_hit": atr,
            "rsi_at_hit": rsi,
            "ema_distance_atr": ema_distance_atr,
            "wick_rejection": wick,
            "htf_trend": htf_trend,
            "htf_aligned": htf_aligned,
            "volume_at_hit": volume_at_hit,
            "avg_volume": avg_volume,
            "volume_spike_ratio": spike,
            "spread_at_hit": spread,
            "session": self.current_session(),
        }

    async def sample_volume(self, internal_symbol: str) -> Optional[dict]:
        """Latest closed M1 bar price/volume + short M1 ATR, for the time series."""
        ic_symbol = self._ic_symbol(internal_symbol)
        if ic_symbol is None:
            return None
        m1 = await self._copy_rates(ic_symbol, mt5.TIMEFRAME_M1, _M1_BARS)
        if not m1:
            return None
        closed = m1[:-1]
        last = closed[-1]
        return {
            "price": last["close"],
            "volume": last["volume"],
            "atr": self._atr(closed, _ATR_PERIOD),
        }

    async def last_closed_m1(self, internal_symbol: str) -> Optional[dict]:
        """High/low/close of the last closed M1 bar; None when bars are unavailable."""
        ic_symbol = self._ic_symbol(internal_symbol)
        if ic_symbol is None:
            return None
        m1 = await self._copy_rates(ic_symbol, mt5.TIMEFRAME_M1, 3)
        if not m1:
            return None
        last = m1[-2]  # [-1] is the forming bar
        return {"high": last["high"], "low": last["low"], "close": last["close"]}

    async def _htf_trend(self, ic_symbol: str, direction: str):
        """H1 trend direction (-1/0/1) and whether it agrees with the signal."""
        h1 = await self._copy_rates(ic_symbol, mt5.TIMEFRAME_H1, _H1_BARS)
        if not h1:
            return None, None
        closed = h1[:-1]
        closes = [b["close"] for b in closed]
        ema_now = self._ema(closes, _EMA_PERIOD)
        ema_prev = self._ema(closes[:-_HTF_SLOPE_LOOKBACK], _EMA_PERIOD)
        if ema_now is None or ema_prev is None:
            return None, None

        last_close = closes[-1]
        if last_close > ema_now > ema_prev:
            trend = 1
        elif last_close < ema_now < ema_prev:
            trend = -1
        else:
            trend = 0

        aligned = trend > 0 if direction.lower() == "long" else trend < 0
        return trend, aligned

    @staticmethod
    def _wick_rejection(bar: dict, direction: str) -> Optional[float]:
        """Rejection-wick ratio of a candle, on the side the signal expects a bounce.

        Long bounces off support reject lower (long lower wick); shorts off
        resistance reject upper. Ratio is wick / full candle range.
        """
        rng = bar["high"] - bar["low"]
        if rng <= 0:
            return None
        body_top = max(bar["open"], bar["close"])
        body_bottom = min(bar["open"], bar["close"])
        wick = body_bottom - bar["low"] if direction.lower() == "long" else bar["high"] - body_top
        return wick / rng

    @staticmethod
    def _atr(bars: list[dict], period: int) -> Optional[float]:
        if len(bars) < period + 1:
            return None
        trs = []
        for i in range(1, len(bars)):
            high = bars[i]["high"]
            low = bars[i]["low"]
            prev_close = bars[i - 1]["close"]
            trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        return sum(trs[-period:]) / period

    @staticmethod
    def _rsi(closes: list[float], period: int) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        gains = 0.0
        losses = 0.0
        for i in range(len(closes) - period, len(closes)):
            change = closes[i] - closes[i - 1]
            if change >= 0:
                gains += change
            else:
                losses -= change
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _ema(values: list[float], period: int) -> Optional[float]:
        if len(values) < period:
            return None
        k = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        for v in values[period:]:
            ema = v * k + ema * (1 - k)
        return ema

    @staticmethod
    def current_session() -> str:
        """Rough FX session bucket from the current UTC hour."""
        hour = datetime.now(timezone.utc).hour
        if 7 <= hour < 12:
            return "london"
        if 12 <= hour < 16:
            return "london_ny"
        if 16 <= hour < 21:
            return "ny"
        if hour >= 22 or hour < 7:
            return "asia"
        return "off"
