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

    def build_media_files(self, recursive: bool = False) -> list[str]:
        if os.path.isdir(self.input):
            media_files = self.find_media_files(recursive)
            if not media_files:
                logging.warning("no media files found in input directory")
        elif is_video_file(self.input, self.strict_mimetype):
            media_files = [self.input]
        else:
            raise ValueError(f"{self.input} is not a video file!")
        return media_files

    def find_media_files(self, recursive: bool = False) -> list[str]:
        logging.debug(f"finding media files in {self.input}")
        media_files = []
        for root_dir, _, files in os.walk(self.input):
            for file in files:
                file_path = os.path.join(self.input, root_dir, file)
                file_mimetype = mimetype(file_path, self.strict_mimetype)
                if not is_video_mimetype(file_mimetype):
                    logging.debug(
                        f"skipping non-video file: {file_path} [{file_mimetype}]"
                    )
                    continue
                media_files.append(file_path)
        return media_files
