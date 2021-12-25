from dataclasses import dataclass
from typing import Optional, Type

import yaml


@dataclass
class ConfigAttr:
    name: str
    config_name: Optional[str] = None
    arg_name: Optional[str] = None
    type: Optional[Type] = None

    def __post_init__(self):
        self.config_name = self.config_name or self.name
        self.arg_name = self.arg_name or self.name


class Config:
    def __init__(self, config_path: str):
        self.config_path = config_path
        with open(config_path) as fh:
            self.data: dict = yaml.safe_load(fh)

    @property
    def executables(self) -> dict[str, str]:
        return self.data["executables"]

    @property
    def ffmpeg(self) -> str:
        return self.executables["ffmpeg"]

    @property
    def ffprobe(self) -> str:
        return self.executables["ffprobe"]

    @property
    def mkvmerge(self) -> str:
        return self.executables["mkvmerge"]

    @property
    def handbrake_cli(self) -> str:
        return self.executables["handbrake_cli"]
