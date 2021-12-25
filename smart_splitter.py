import argparse
import logging
import os
import re
import sys
from functools import partial

import utils
from media import Media
from decimal import Decimal
from typing import Optional
from config import SmartSplitterConfig
from utils import mime_type, log_exception, initialize_logger


class SmartSplitter:
    def __init__(self):
        self.arg_parser = SmartSplitter.create_parser()
        self.config: Optional[SmartSplitterConfig] = None
        self.input: Optional[str] = None

    def run(self, args: list[str]):
        self.parse_args(args)
        if os.path.isdir(self.input):
            media_files = self.find_media_files()
            if not media_files:
                logging.warning("no media files found in input directory")
            return
        else:
            media_type = mime_type(self.input)
            if not utils.is_video_file(media_type):
                raise ValueError(f"{self.input} is not a video file!")
            media_files = [self.input]
        self.split_files(media_files)

    def find_media_files(self) -> list[str]:
        logging.debug(f"finding media files in {self.input}")
        media_files = []
        for entry in os.listdir(self.input):
            full_path = os.path.join(self.input, entry)
            if os.path.isdir(entry):
                logging.debug(f"skipping directory {full_path}")
                continue
            mime_type = utils.mime_type(full_path)
            if not utils.is_video_file(mime_type):
                logging.debug(f"skipping non-video file: {full_path} [{mime_type}]")
                continue
            media_files.append(full_path)
        return media_files

    def split_files(self, media_files: list[str]):
        for media_file in media_files:
            old_handlers = logging.getLogger().handlers[:]
            basename = os.path.basename(media_file)
            try:
                line = "*" * (len(basename) + 4)
                logging.info(f"\n{line}\n* {basename} *\n{line}")
                if not self.config.input_pattern:
                    output_folder = os.path.splitext(basename)[0]
                elif match := self.config.input_pattern.match(basename):
                    output_folder = "_".join(match.groups())
                else:
                    logging.warning(
                        f"input pattern was specified, but no match was found. skipping..."
                    )
                    continue
                media = Media(
                    media_file,
                    os.path.join(self.config.output_directory, output_folder),
                    self.config,
                )
                media.split()
            except Exception as exc:
                # catch errors here, so we can continue with the remaining files
                logging.error(f"!! An error was detected. Aborting...")
                logging.exception(exc)
            finally:
                remove_handlers = [
                    h for h in logging.getLogger().handlers if h not in old_handlers
                ]
                for handler in remove_handlers:
                    logging.getLogger().removeHandler(handler)

    @staticmethod
    def create_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="splits media files on black/silent frames"
        )
        parser.add_argument("--config", "-c", required=True, help="path to config file")
        parser.add_argument(
            "--min-duration",
            type=Decimal,
            default="3.0",
            help="minimum duration (secs) of output files (default: %(default)s)",
        )
        parser.add_argument(
            "--black-min-duration",
            type=Decimal,
            default="0.5",
            help="minimum duration (secs) for ffmpeg blackdetect (default: %(default)s)",
        )
        parser.add_argument(
            "--silence-min-duration",
            type=Decimal,
            default="0.5",
            help="minimum duration (secs) for ffmpeg silentdetect (default: %(default)s)",
        )
        parser.add_argument(
            "--silence-noise-tolerance",
            type=Decimal,
            default="-60",
            help="noise tolerance (dB) for ffmpeg silentdetect (default: %(default)s)",
        )
        parser.add_argument(
            "--input",
            "-i",
            required=True,
            help="path to directory or file. If directory, the script will handle only video files",
        )
        parser.add_argument(
            "--output-directory",
            "-o",
            help="base directory to store output files (defaults to path relative to input file)",
        )
        parser.add_argument(
            "--input-pattern",
            type=partial(re.compile, flags=re.IGNORECASE),
            help="regex used on the input file(s) to determine output directory relative to --output-directory (defaults to basename without extension)",
        )
        return parser

    def parse_args(self, args_without_script: list[str]):
        parsed_args = self.arg_parser.parse_args(args_without_script)
        self.config = SmartSplitterConfig(parsed_args.config)
        self.config.load_from_parsed_args(parsed_args)
        self.input = os.path.realpath(parsed_args.input)
        if not os.path.exists(self.input):
            raise FileNotFoundError(self.input)


def run():
    try:
        initialize_logger()
        runner = SmartSplitter()
        runner.run(sys.argv[1:])
    except FileNotFoundError as exc:
        log_exception(exc, f"File not found: {exc}")
    except Exception as exc:
        log_exception(exc)


if __name__ == "__main__":
    run()
