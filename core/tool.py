import logging
import os
from abc import ABC, abstractmethod

from core.utils import mime_type, is_video_file


class Tool(ABC):
    def __init__(self, parsed_args):
        self.parsed_args = parsed_args
        self.input: str = parsed_args.input

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
        else:
            media_type = mime_type(self.input)
            if not is_video_file(media_type):
                raise ValueError(f"{self.input} is not a video file!")
            media_files = [self.input]
        return media_files

    def find_media_files(self) -> list[str]:
        logging.debug(f"finding media files in {self.input}")
        media_files = []
        for entry in os.listdir(self.input):
            full_path = os.path.join(self.input, entry)
            if os.path.isdir(full_path):
                logging.debug(f"skipping directory {full_path}")
                continue
            media_type = mime_type(full_path)
            if not is_video_file(media_type):
                logging.debug(f"skipping non-video file: {full_path} [{media_type}]")
                continue
            media_files.append(full_path)
        return media_files
