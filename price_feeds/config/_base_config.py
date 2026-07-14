"""
Shared base class for threshold-style configuration files (TP, alert distance, NM).

Provides JSON load/save, path resolution, a shared SymbolMapper singleton,
asset-class detection with fallback, pip-size lookup, and reload.
"""

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class BaseThresholdConfig:
    """Base for configs that follow a defaults/overrides JSON structure."""

    _shared_mapper = None
    CONFIG_FILENAME: str = ""

    def __init__(self, config_path: Optional[str] = None):
        if config_path is not None:
            self.config_path = Path(config_path)
        else:
            self.config_path = (
                Path(__file__).resolve().parent.parent.parent / "config" / self.CONFIG_FILENAME
            )

        self.mapper = self._get_shared_mapper()
        self.config: dict = self._load_config()
        self._validate_config()

    # === Shared SymbolMapper ===

    @classmethod
    def _get_shared_mapper(cls):
        if BaseThresholdConfig._shared_mapper is None:
            try:
                from price_feeds.config.symbol_mapper import SymbolMapper

                mapper_path = (
                    Path(__file__).resolve().parent.parent.parent / "config" / "symbol_mappings.json"
                )
                BaseThresholdConfig._shared_mapper = SymbolMapper(str(mapper_path))
            except Exception as e:
                logger.warning(f"Could not init shared SymbolMapper: {e}")
        return BaseThresholdConfig._shared_mapper

    # === Config I/O ===

    def _load_config(self) -> dict:
        try:
            with open(self.config_path, encoding="utf-8") as f:
                raw = json.load(f)
            return self._post_load(raw)
        except FileNotFoundError:
            logger.warning(f"Config not found, creating default: {self.config_path}")
            config = self._create_default_config()
            self._save_config(config)
            return config
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {self.config_path}: {e}. Using defaults.")
            return self._create_default_config()

    def _post_load(self, raw: dict) -> dict:
        """Hook for subclass migration logic. Default: return as-is."""
        return raw

    def _create_default_config(self) -> dict:
        raise NotImplementedError

    def _validate_config(self):
        """Basic validation — ensure 'defaults' and 'overrides' keys exist."""
        for key in ("defaults", "overrides"):
            if key not in self.config:
                logger.error(f"Config missing key '{key}', resetting to defaults")
                self.config = self._create_default_config()
                return

    def _save_config(self, config: Optional[dict] = None):
        if config is None:
            config = self.config
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save config to {self.config_path}: {e}")
            raise

    def reload_config(self):
        """Reload configuration from disk."""
        self.config = self._load_config()
        self._validate_config()

    # === Asset-class detection ===

    def determine_asset_class(self, symbol: str) -> str:
        """Determine asset class via SymbolMapper, falling back to heuristics."""
        if self.mapper:
            try:
                return self.mapper.determine_asset_class(symbol)
            except Exception:
                pass
        return self._fallback_asset_class(symbol)

    @staticmethod
    def _fallback_asset_class(symbol: str) -> str:
        s = symbol.upper()

        # Stocks carry an exchange suffix; check before crypto/indices so a
        # ticker like ABNB.NAS (BNB) or GOOGL.NAS (NAS) is not misclassified.
        if "." in s:
            return "stocks"

        if (
            any(x in s for x in ["BTC", "ETH", "BNB", "XRP", "ADA", "DOGE", "SOL", "DOT"])
            or "USDT" in s
        ):
            return "crypto"

        if any(x in s for x in ["XAU", "XAG", "GOLD", "SILVER"]):
            return "metals"

        if any(x in s for x in ["WTI", "BRENT", "OIL", "USOIL"]):
            return "oil"

        if any(
            x in s
            for x in [
                "SPX",
                "NAS",
                "DOW",
                "DAX",
                "CHINA50",
                "US500",
                "USTEC",
                "US30",
                "US2000",
                "RUSSELL",
                "GER",
                "DE30",
                "DE40",
                "JP225",
                "NIKKEI",
                "FTSE",
            ]
        ):
            return "indices"

        forex_ccys = {"EUR", "USD", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}
        if len(s) == 6 and s[:3] in forex_ccys and s[3:] in forex_ccys:
            return "forex_jpy" if "JPY" in s else "forex"

        return "forex"

    # === Pip-size lookup ===

    @staticmethod
    def get_pip_size(symbol: str) -> float:
        """Return pip size in price units for the given symbol."""
        s = symbol.upper()
        if s.endswith((".NAS", ".NYSE")):
            return 0.01  # stocks: one cent — must precede the index keywords (".NAS" contains "NAS")
        if "JPY" in s:
            return 0.01
        if any(x in s for x in ["XAU", "GOLD"]) or s.startswith("GC"):
            return 0.01
        if any(x in s for x in ["XAG", "SILVER"]):
            return 0.001
        if "BTC" in s:
            return 1.0
        if any(x in s for x in ["SPX", "NAS", "DOW", "DAX", "US500", "USTEC", "US30"]):
            return 1.0
        if any(x in s for x in ["JP225", "NIKKEI", "DE30", "DE40", "GER", "US2000",
                                "RUSSELL", "UK100", "FTSE", "HK50", "CHINA50"]):
            return 1.0
        if any(x in s for x in ["OIL", "XTI", "WTI", "BRENT", "BCO"]):
            return 0.01
        if any(x in s for x in ["ETH", "SOL", "XRP", "ADA", "DOGE", "DOT", "BNB"]) or s.endswith(("USDT", "USDC")):
            return 0.1
        return 0.0001
