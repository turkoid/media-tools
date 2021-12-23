import os.path

import yaml


class Config:
    def __init__(self):
        config_dir = os.path.dirname(__file__)
        config_path = os.path.join(config_dir, "config.yaml")
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

    @property
    def smart_splitter(self):
        return self.data["smart_splitter"]
