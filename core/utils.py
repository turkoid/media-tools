import logging
import sys
import time
from decimal import Decimal
import magic


def initialize_logger(debug_file_path: str):
    logger = logging.getLogger()
    if logger.hasHandlers():
        logging.debug(f"logger has already been initialized")
        return
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
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
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
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


def mime_type(path: str) -> str:
    mime = magic.from_file(path, mime=True)
    return mime


def is_video_file(mime: str) -> bool:
    return mime.split("/")[0] == "video"
