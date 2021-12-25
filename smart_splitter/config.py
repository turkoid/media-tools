import logging
import os
import re
from argparse import Namespace
from decimal import Decimal
from typing import Optional, Pattern, Any

from core.config import Config, ConfigAttr


class SmartSplitterConfig(Config):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.min_duration: Decimal = Decimal("3.0")
        self.black_min_duration: Decimal = Decimal("0.5")
        self.silence_min_duration: Decimal = Decimal("0.5")
        self.silence_noise_tolerance: Decimal = Decimal("-60")
        self._blackdetect_options: Optional[str] = None
        self._silencedetect_options: Optional[str] = None
        self.output_directory: Optional[str] = None
        self.input_pattern: Optional[Pattern] = None
        self.attrs: dict[str, ConfigAttr] = SmartSplitterConfig._create_config_entries()
        self._load_from_config()

    @staticmethod
    def _create_config_entries() -> dict[str, ConfigAttr]:
        attrs = [
            ConfigAttr("min_duration", type=Decimal),
            ConfigAttr("black_min_duration", type=Decimal),
            ConfigAttr("silence_min_duration", type=Decimal),
            ConfigAttr("silence_noise_tolerance", type=Decimal),
            ConfigAttr("_blackdetect_options", "blackdetect_options"),
            ConfigAttr("_blackdetect_options", "silencedetect_options"),
            ConfigAttr("output_directory"),
            ConfigAttr("input_pattern", type=Pattern),
        ]
        return {attr.name: attr for attr in attrs}

    def _setattr(self, attr: ConfigAttr, value: Any):
        if value is not None and attr.type and not isinstance(value, attr.type):
            if attr.type is Pattern:
                value = re.compile(value, re.IGNORECASE)
            else:
                value = attr.type(value)
        logging.debug(f"setting {attr.name}={value}")
        setattr(self, attr.name, value)

    def _load_from_config(self):
        logging.debug(f"overriding config values from config file {self.config_path}")
        for attr in self.attrs.values():
            if attr.config_name not in self.data["smart_splitter"]:
                continue
            config_value = self.data["smart_splitter"][attr.config_name]
            self._setattr(attr, config_value)

    def load_from_parsed_args(self, parsed_args: Namespace):
        logging.debug("overriding config values from parsed args")
        for attr in self.attrs.values():
            if attr.arg_name not in parsed_args:
                continue
            arg_value = getattr(parsed_args, attr.arg_name)
            self._setattr(attr, arg_value)

    @property
    def blackdetect_options(self):
        if self._blackdetect_options is None:
            return f"d={self.black_min_duration}"

    @blackdetect_options.setter
    def blackdetect_options(self, value):
        self._blackdetect_options = value

    @property
    def silencedetect_options(self):
        if self._silencedetect_options is None:
            return f"n={self.silence_noise_tolerance}dB:d={self.silence_min_duration}"

    @silencedetect_options.setter
    def silencedetect_options(self, value):
        self._silencedetect_options = value

    @property
    def handbrake_presets_import(self) -> str:
        return self.data["smart_splitter"]["handbrake_presets_import"]

    @property
    def handbrake_preset(self) -> str:
        return self.data["smart_splitter"]["handbrake_preset"]

    def validate(self):
        for path in [
            self.ffmpeg,
            self.ffprobe,
            self.handbrake_cli,
            self.handbrake_presets_import,
        ]:
            if not os.path.exists(path):
                raise FileNotFoundError(path)
        with open(self.handbrake_presets_import) as fh:
            if self.handbrake_preset not in fh.read():
                raise ValueError(f"handbrake preset not found: {self.handbrake_preset}")
