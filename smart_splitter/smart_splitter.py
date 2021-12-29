import logging
import os
import re
from functools import partial
from typing import Optional

import yaml

from core.tool import Tool
from smart_splitter.media import Media
from decimal import Decimal
from smart_splitter.config import SmartSplitterConfig
from core.utils import validate_paths, log_file_header


class SmartSplitter(Tool):
    def __init__(self, parsed_args):
        super().__init__(parsed_args)
        self.config = SmartSplitterConfig(parsed_args.config)
        self.config.load_from_parsed_args(parsed_args)
        self.validate()
        if not os.path.exists(self.input):
            raise FileNotFoundError(self.input)

    def run(self):
        media_files = self.build_media_files()
        self.split_files(media_files)

    def output_path(self, media_file: str, create: bool = True) -> Optional[str]:
        input_directory = os.path.dirname(media_file)
        basename = os.path.basename(media_file)
        if not self.config.input_pattern:
            media_output = os.path.splitext(basename)[0]
        elif match := self.config.input_pattern.match(basename):
            media_output = "_".join(match.groups()).strip()
            if not media_output:
                raise ValueError(f"input pattern matched returned zero length string")
        else:
            logging.warning(
                f"input pattern was specified, but no match was found. skipping..."
            )
            return
        output_path = os.path.join(
            input_directory, self.config.output_directory or "", media_output
        )
        if create:
            os.makedirs(output_path, exist_ok=True)
        return output_path

    def check_media_id(self, output_path: str, media_id: str):
        info_file = os.path.join(output_path, "info.yaml")
        if os.path.exists(info_file):
            with open(info_file) as fh:
                info = yaml.safe_load(fh)
                if "media" not in info:
                    raise KeyError(f"info.yaml file is missing the media key")
                file_media_id = info["media"]
            if file_media_id != media_id:
                raise ValueError(
                    f"'{output_path}' contains output for '{file_media_id}'"
                )
        else:
            if os.listdir(output_path):
                logging.warning(
                    f"{output_path} missing info.yaml file, but contains files."
                )

    def split_media(self, media_file: str):
        log_file_header(media_file)
        output_path = self.output_path(media_file)
        if not output_path:
            return
        self.check_media_id(output_path, os.path.basename(media_file))
        logging.info(f"\nOUTPUT: {output_path}")
        media = Media(
            media_file,
            output_path,
            self.config,
        )
        media.split()

    def split_files(self, media_files: list[str]):
        for media_file in media_files:
            old_handlers = logging.getLogger().handlers[:]
            try:
                self.split_media(media_file)
            except Exception as exc:
                # catch errors here, so we can continue with the remaining files
                if isinstance(Exception, FileNotFoundError):
                    logging.error(f"File not found: {exc}")
                else:
                    logging.error(exc)
                logging.error(f"!! An error was detected. Aborting...")
                logging.exception(exc)
            finally:
                remove_handlers = [
                    h for h in logging.getLogger().handlers if h not in old_handlers
                ]
                for handler in remove_handlers:
                    logging.getLogger().removeHandler(handler)

    def validate(self):
        validate_paths(
            self.config.ffmpeg,
            self.config.ffprobe,
            self.config.handbrake_cli,
            self.config.handbrake_presets_import,
        )
        with open(self.config.handbrake_presets_import) as fh:
            if self.config.handbrake_preset not in fh.read():
                raise ValueError(
                    f"handbrake preset not found: {self.config.handbrake_preset}"
                )

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "split", description="split media files at black/silent frames"
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
        parser.add_argument(
            "--dry-run",
            "-t",
            action="store_true",
            help="caches output, but does not run handbrake",
        )
