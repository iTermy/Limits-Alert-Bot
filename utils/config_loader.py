import json
import logging
from pathlib import Path
from typing import Any, Dict, Union

from models.config import BotSettings

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent.parent / "config"


class ConfigLoader:
    def __init__(self, config_dir: Path = _CONFIG_DIR):
        self.config_dir = config_dir
        self._configs: Dict[str, Any] = {}

    def load(self, filename: str, reload: bool = False) -> Dict[str, Any]:
        if not reload and filename in self._configs:
            return self._configs[filename]
        filepath = self.config_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Configuration file not found: {filepath}")
        try:
            with open(filepath) as f:
                data = json.load(f)
            self._configs[filename] = data
            return data
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {filename}: {e}")

    def save(self, filename: str, data: Dict[str, Any]):
        filepath = self.config_dir / filename
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        self._configs[filename] = data


config = ConfigLoader()


def load_settings() -> BotSettings:
    return BotSettings.model_validate(config.load("settings.json"))


def save_settings(settings: Union[BotSettings, dict]):
    data = settings.model_dump() if isinstance(settings, BotSettings) else settings
    config.save("settings.json", data)


def load_channels_config() -> dict:
    return config.load("channels.json")
