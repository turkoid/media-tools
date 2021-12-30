import logging
import mimetypes
import os
import subprocess
import sys
import time
from decimal import Decimal
from typing import Optional

import magic


def initialize_logger(debug_file_path: str, debug_mode: bool = False):
    logger = logging.getLogger()
    if logger.hasHandlers():
        logging.debug(f"logger has already been initialized")
        return
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    if not debug_mode:
        sh.addFilter(lambda record: 0 if record.exc_info else 1)
    init_logging_handler(sh, logging.INFO)
    logger.addHandler(sh)
    fh = logging.FileHandler(filename=debug_file_path, mode="w")
    init_logging_handler(fh)
    logger.addHandler(fh)


def log_exception(e: Exception, log_file_path: str, msg: str = ""):
    logging.error(f"An error occurred, check {log_file_path} for more details...")
    if msg:
        logging.error(msg)
    logging.exception(e)
    sys.exit(1)


def init_logging_handler(handler: logging.Handler, level=logging.DEBUG):
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s - %(message)s")
    handler.setFormatter(formatter)


def format_timestamp(timestamp: Decimal) -> str:
    timestamp_parts = str(timestamp).split(".")
    seconds = timestamp_parts[0]
    fractional = timestamp_parts[1] if len(timestamp_parts) == 2 else ""
    partial_time = time.strftime("%H:%M:%S", time.gmtime(int(seconds)))
    return f"{partial_time}.{fractional:0<6}"


def parse_timestamp(timestamp: str) -> Decimal:
    hms, fractional = timestamp.split(".")
    h, m, s = hms.split(":")
    seconds = (int(h) * 3600) + (int(m) * 60) + int(s)
    return Decimal(f"{seconds}.{fractional}")


def fps_adjusted_frame(secs: Decimal, fps: Decimal) -> int:
    return round(fps * secs)


def log_multiline(level, header, message):
    logging.log(level, f"{header}\n{message}")


def mimetype(path: str, strict: bool = False) -> Optional[str]:
    if os.path.isdir(path):
        return "directory"
    path_mimetype = (
        magic.from_file(path, mime=True) if strict else mimetypes.guess_type(path)[0]
    )
    return path_mimetype


def is_video_mimetype(path_mimetype: Optional[str]) -> bool:
    return path_mimetype and path_mimetype.split("/")[0] == "video"


def is_video_file(path: str, strict: bool = False) -> bool:
    path_mimetype = mimetype(path, strict)
    logging.debug(f"{path} mimetype: {path_mimetype}")
    return is_video_mimetype(path_mimetype)


def validate_paths(*paths: str):
    for path in paths:
        if not os.path.exists(path):
            raise FileNotFoundError(path)


def run_process(args: list[str]) -> str:
    try:
        command = " ".join(args)
        logging.debug(f"\n+ BEGIN calling {command}")
        cp = subprocess.run(
            args, check=True, capture_output=True, universal_newlines=True
        )
        logging.debug(f"\n+ END calling {command}")
        return cp.stdout
    except subprocess.CalledProcessError as e:
        msg = [
            "Error calling process:",
            f"return_code: {e.returncode}",
            f"stdout:\n{e.stdout}",
            f"stderr:\n{e.stderr}",
        ]
        logging.error("\n".join(msg))
        raise


def log_file_header(header: str, level=logging.INFO):
    basename = os.path.basename(header)
    line = "*" * (len(basename) + 4)
    logging.log(level, f"\n{line}\n* {basename} *\n{line}")
