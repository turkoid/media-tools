import argparse
import os

from smart_splitter.tool import SmartSplitter
from core.utils import initialize_logger, log_exception


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="a collection to tools to handle media", allow_abbrev=False
    )
    parser.add_argument("--config", "-c", required=True, help="path to config file")
    subparsers = parser.add_subparsers(help="commands", dest="tool", required=True)
    for tool in [SmartSplitter]:
        tool.create_parser(subparsers)
    return parser


def run(args_without_script: list[str]):
    parser = create_parser()
    parsed_args = parser.parse_args(args_without_script)
    debug_file_path = os.path.join(os.getcwd(), f"media_tools-{parsed_args.tool}.log")
    initialize_logger(debug_file_path)
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
