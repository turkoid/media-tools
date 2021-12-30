import logging
import os
from abc import ABC, abstractmethod

from core.utils import mimetype, is_video_mimetype, is_video_file


class Tool(ABC):
    def __init__(self, parsed_args):
        self.parsed_args = parsed_args
        self.input: str = parsed_args.input
        self.dry_run: bool = parsed_args.dry_run
        self.strict_mimetype: bool = parsed_args.strict_mimetype

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def create_parser(subparsers):
        raise NotImplementedError

    def build_media_files(self) -> list[str]:
        if os.path.isdir(self.input):
            media_files = self.find_media_files()
            if not media_files:
                logging.warning("no media files found in input directory")
        elif is_video_file(self.input, self.strict_mimetype):
            media_files = [self.input]
        else:
            raise ValueError(f"{self.input} is not a video file!")
        return media_files

    def find_media_files(self) -> list[str]:
        logging.debug(f"finding media files in {self.input}")
        media_files = []
        for entry in os.listdir(self.input):
            entry_path = os.path.join(self.input, entry)
            entry_mimetype = mimetype(entry_path, self.strict_mimetype)
            if not is_video_mimetype(entry_mimetype):
                logging.debug(
                    f"skipping non-video file: {entry_path} [{entry_mimetype}]"
                )
                continue
            media_files.append(entry_path)
        return media_files
