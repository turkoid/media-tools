import logging
import mimetypes
import os
import subprocess
import sys
import time
from decimal import Decimal
from typing import Optional, Callable, Union

import asyncio
import magic
from subprocess import CompletedProcess, CalledProcessError

from tqdm import tqdm


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


ENC_START = b"Encoding: task 1 of 1, "
OutputHandler = Callable[[bytes], None]


def monitor_handbrake_encode(buffer: bytes, progress_bar: tqdm, data: dict[str, bytes]):
    current_line = data["current_line"] + buffer
    if (perc_index := current_line.find(b"%")) != 1 and (
        enc_start := current_line.rfind(ENC_START, 0, perc_index)
    ) != -1:
        perc = current_line[enc_start + len(ENC_START) : perc_index]
        current_line = current_line[perc_index + 1 :]
        if perc:
            progress_bar.update(float(perc) - progress_bar.n)
    data["current_line"] = current_line


async def capture_output(
    stream: asyncio.StreamReader, handlers: list[OutputHandler]
) -> bytes:
    output = []
    while buffer := await stream.read(2 ** 16):
        output.append(buffer)
        for handler in handlers:
            handler(buffer)
    return b"".join(output)


def normalize_newlines(data: Union[bytes, str]) -> str:
    if isinstance(data, bytes):
        data = data.decode()
    return data.replace("\r\n", "\n").replace("\r", "\n")


async def async_run_process(
    args,
    stdout_handler: Optional[OutputHandler] = None,
    stderr_handler: Optional[OutputHandler] = None,
    check: bool = False,
    text: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
) -> CompletedProcess:
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout_handlers = [
            handler
            for handler in [print_stdout and sys.stdout.buffer.write, stdout_handler]
            if handler
        ]
        stderr_handlers = [
            handler
            for handler in [print_stderr and sys.stderr.buffer.write, stderr_handler]
            if handler
        ]
        if stdout_handlers:
            stdout_coroutine = capture_output(process.stdout, stdout_handlers)
        else:
            stdout_coroutine = process.stdout.read()
        if stderr_handlers:
            stderr_coroutine = capture_output(process.stderr, stderr_handlers)
        else:
            stderr_coroutine = process.stderr.read()
        stdout, stderr = await asyncio.gather(stdout_coroutine, stderr_coroutine)
        await process.wait()
        if text:
            stdout = normalize_newlines(stdout)
            stderr = normalize_newlines(stderr)
        if check and process.returncode:
            raise CalledProcessError(process.returncode, args, stdout, stderr)
        logging.debug(normalize_newlines(stdout))
        return CompletedProcess(args, process.returncode, stdout, stderr)
    except Exception:
        process.kill()
        raise


def run_process(args: list[str]) -> str:
    try:
        command = " ".join(args)
        logging.debug(f"\n+ BEGIN calling {command}")
        cp = asyncio.run(async_run_process(args, check=True, text=True))
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
