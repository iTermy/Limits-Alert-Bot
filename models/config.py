"""Pydantic models for configuration files."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SpreadBufferConfig(BaseModel):
    apply_to_approaching: bool = True
    apply_to_hit: bool = True
    apply_to_stop_loss: bool = False
    fallback_spread: float = 0.0


class NewsAutofetchConfig(BaseModel):
    """Auto-population of news windows from the ForexFactory weekly feed."""

    enabled: bool = True
    impact_levels: list[str] = Field(default_factory=lambda: ["High"])
    window_minutes: int = 10
    refresh_hours: int = 6
    currencies: list[str] = Field(
        default_factory=lambda: ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]
    )


class BotSettings(BaseModel):
    admin_ids: list[int] = Field(default_factory=list)
    signal_manager_role_ids: list[int] = Field(default_factory=list)
    signal_manager_user_ids: list[int] = Field(default_factory=list)
    health_alert_admin_id: int | None = None
    spread_buffer_enabled: bool = True
    spread_buffer_config: SpreadBufferConfig = Field(default_factory=SpreadBufferConfig)
    license_role_name: str = "Signal Subscriber"
    gold_tolls_sl_offset: float = 5.0
    risky_gold_sl_offset: float = 10.0
    us_market_holidays: list[str] = Field(default_factory=list)
    news_autofetch: NewsAutofetchConfig = Field(default_factory=NewsAutofetchConfig)

    model_config = {"extra": "allow"}


class ChannelSettings(BaseModel):
    default_expiry: str | None = None
    default_instrument: str | None = None

    model_config = {"extra": "allow"}


class ThresholdEntry(BaseModel):
    """Single threshold entry used by TP, alert distance, and NM configs."""

    type: str
    value: float | None = None
    max_proximity: float | None = None
    base_bounce: float | None = None
    description: str | None = None
    set_by: str | None = None
    set_at: str | None = None

    model_config = {"extra": "allow"}
