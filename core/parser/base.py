"""
Base classes and data structures for the signal parser
"""
from dataclasses import dataclass, field
from typing import List, Optional
from abc import ABC, abstractmethod


@dataclass
class ParsedSignal:
    """Represents a parsed trading signal"""
    instrument: str
    direction: str  # long/short
    limits: List[float]
    stop_loss: float
    expiry_type: str  # week_end, no_expiry, day_end, month_end
    raw_text: str
    parse_method: str  # high_confidence/moderate_confidence/ai
    keywords: List[str] = field(default_factory=list)  # hot, semi-swing, etc.
    channel_name: str = None


class ParsingStrategy(ABC):
    """Abstract base class for parsing strategies"""

    @abstractmethod
    def can_parse(self, message: str, channel_name: str = None) -> bool:
        """Check if this strategy can parse the message"""
        pass

    @abstractmethod
    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """Parse the message and return a ParsedSignal or None"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this parsing strategy"""
        pass