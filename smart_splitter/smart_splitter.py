import logging
import os
import re
from functools import partial
from typing import Optional

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

    def output_folder(self, media_file: str, create: bool = True) -> Optional[str]:
        if not self.config.output_directory:
            self.config.output_directory = os.path.dirname(media_file)
        basename = os.path.basename(media_file)
        if not self.config.input_pattern:
            output_folder = os.path.splitext(basename)[0]
        elif match := self.config.input_pattern.match(basename):
            output_folder = "_".join(match.groups())
        else:
            logging.warning(
                f"input pattern was specified, but no match was found. skipping..."
            )
            output_folder = None
        if create and output_folder:
            output_path = os.path.join(self.config.output_directory, output_folder)
            os.makedirs(output_path, exist_ok=True)
        return output_folder

    def check_media_id(self, output_path: str, media_id: str):
        media_id_file = os.path.join(output_path, ".media")
        if os.path.exists(media_id_file):
            with open(media_id_file) as fh:
                file_media_id = fh.read()
            if file_media_id != media_id:
                raise ValueError(
                    f"'{output_path}' contains output for '{file_media_id}'"
                )
        else:
            if os.listdir(output_path):
                logging.warning(
                    f"{output_path} missing .media file, but contains files."
                )
            with open(media_id_file, "w") as fh:
                fh.write(media_id)

    def split_media(self, media_file: str):
        log_file_header(media_file)
        output_folder = self.output_folder(media_file)
        if not output_folder:
            return
        output_path = os.path.join(self.config.output_directory, output_folder)
        self.check_media_id(output_path)
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
