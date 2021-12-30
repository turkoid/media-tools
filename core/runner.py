import argparse
import os

from smart_splitter.smart_splitter import SmartSplitter
from core.utils import initialize_logger, log_exception
from stream_pruner.stream_pruner import StreamPruner


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="a collection to tools to handle media", allow_abbrev=False
    )
    parser.add_argument("--config", "-c", required=True, help="path to config file")
    parser.add_argument(
        "--debug", action="store_true", help="prints stack traces to stdout if enabled"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="runs the tool without making modifications",
    )
    parser.add_argument(
        "--strict-mimetype",
        action="store_true",
        help="if passed, detect the mimetype from the file instead of the extension",
    )
    subparsers = parser.add_subparsers(help="commands", dest="tool", required=True)
    for tool in [SmartSplitter, StreamPruner]:
        tool.create_parser(subparsers)
    return parser


def run(args_without_script: list[str]):
    parser = create_parser()
    parsed_args = parser.parse_args(args_without_script)
    debug_file_path = os.path.join(os.getcwd(), f"media_tools-{parsed_args.tool}.log")
    initialize_logger(debug_file_path, parsed_args.debug)
    try:
        if parsed_args.tool == "split":
            tool = SmartSplitter(parsed_args)
        else:
            raise ValueError(f"invalid tool: {parsed_args.tool}")
        tool.run()
    except FileNotFoundError as exc:
        log_exception(exc, debug_file_path, f"File not found: {exc}")
    except Exception as exc:
        log_exception(exc, debug_file_path)
