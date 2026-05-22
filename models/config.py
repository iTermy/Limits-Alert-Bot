"""Pydantic models for configuration files."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SpreadBufferConfig(BaseModel):
    apply_to_approaching: bool = True
    apply_to_hit: bool = True
    apply_to_stop_loss: bool = False
    fallback_spread: float = 0.0
    log_buffer_usage: bool = True


class BotSettings(BaseModel):
    bot_prefix: str = "!"
    enable_openai_fallback: bool = False
    max_retries: int = 3
    connection_timeout: int = 30
    alert_cooldown_minutes: int = 5
    cleanup_days: int = 30
    debug_mode: bool = False
    spread_buffer_enabled: bool = True
    spread_buffer_config: SpreadBufferConfig = Field(default_factory=SpreadBufferConfig)
    license_role_name: str = "Signal Subscriber"
    gold_tolls_sl_offset: float = 5.0
    admin_ids: list[int] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class ChannelSettings(BaseModel):
    default_expiry: Optional[str] = None
    default_instrument: Optional[str] = None

    model_config = {"extra": "allow"}


class ThresholdEntry(BaseModel):
    """Single threshold entry used by TP, alert distance, and NM configs."""

    type: str
    value: Optional[float] = None
    max_proximity: Optional[float] = None
    base_bounce: Optional[float] = None
    description: Optional[str] = None
    set_by: Optional[str] = None
    set_at: Optional[str] = None

    model_config = {"extra": "allow"}
