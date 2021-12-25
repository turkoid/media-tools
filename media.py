import json
import logging
import os
import re
import subprocess
from decimal import Decimal
from typing import Any, Optional

from config import SmartSplitterConfig
from models import (
    FrameInfo,
    FrameMetadata,
    DetectMetadata,
    DetectInterval,
    SplitMetadata,
)
from utils import init_logging_handler, log_multiline, parse_timestamp


class Media:
    FFMPEG_FRAME_LINE = r"frame:(\d+)\s+pts:(\d+)\s+pts_time:(-?\d+\.?\d*)"
    FFMPEG_KEY_LINE = r"(.+)\.(.+?)(?:_([^_]+?))?=(-?\d+\.?\d*)"

    def __init__(self, path: str, output_folder: str, config: SmartSplitterConfig):
        self.config = config
        self.path: str = os.path.realpath(path)
        self.cache: dict[str, Any] = {}
        self.detection_duration: float = 0.5
        self._output_folder: str = os.path.realpath(output_folder)
        self.extension: str = os.path.splitext(path)[1]
        self.init_logger()
        self.log_basic_info()

    def log_basic_info(self):
        for stream_type in ["video", "audio"]:
            logging.info(
                f"{stream_type} frame count: {self.stream_frame_count(stream_type)}"
            )
            logging.info(f"{stream_type} duration: {self.stream_duration(stream_type)}")
            logging.info(f"{stream_type} fps: {self.stream_fps(stream_type)}")

    def init_logger(self):
        log_file = os.path.join(self.output_folder, "output.log")
        fh = logging.FileHandler(log_file, mode="w")
        init_logging_handler(fh)
        logging.getLogger().addHandler(fh)

    @property
    def cache_directory(self) -> str:
        cache_dir = os.path.join(self.output_folder, "cache")
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir

    def run_process(self, args: list[str], cache_key: Optional[str] = None) -> str:
        logging.debug(f"running {args}")
        stdout = None
        cache_file = os.path.join(self.cache_directory, f"{cache_key}.txt")
        if cache_key:
            if os.path.exists(cache_file):
                logging.info(f"reading from cached output: {cache_file}")
                with open(cache_file) as fh:
                    stdout = fh.read()
                if not stdout:
                    logging.warning(f"cache file found, but no output found!")
            else:
                logging.debug("cache file not found")
        if not stdout:
            try:
                cp = subprocess.run(
                    args, check=True, capture_output=True, universal_newlines=True
                )
                stdout = cp.stdout
            except subprocess.CalledProcessError as e:
                msg = [
                    "Error calling process:",
                    f"return_code: {e.returncode}",
                    f"stdout:\n{e.stdout}",
                    f"stderr:\n{e.stderr}",
                ]
                logging.error("\n".join(msg))
                raise

            if cache_key:
                with open(cache_file, mode="w") as fh:
                    fh.write(stdout)
        log_multiline(logging.DEBUG, "process stdout:", stdout)
        return stdout

    @property
    def info_json(self) -> dict:
        cache_key = "info_json"
        if cache_key not in self.cache:
            args = [
                self.config.ffprobe,
                "-of",
                "json",
                "-hide_banner",
                "-v",
                "error",
                "-show_streams",
                "-count_packets",
                self.path,
            ]
            stdout = self.run_process(args, cache_key)
            self.cache[cache_key] = json.loads(stdout)
        return self.cache[cache_key]

    def parse_streams(self, stream_type: str) -> list[dict]:
        streams = []
        for stream in self.info_json["streams"]:
            if stream["codec_type"] == stream_type:
                streams.append(stream)
        return streams

    @property
    def video_streams(self) -> list[dict]:
        cache_key = "video_streams"
        if cache_key not in self.cache:
            self.cache[cache_key] = self.parse_streams("video")
        return self.cache[cache_key]

    @property
    def audio_streams(self) -> list[dict]:
        cache_key = "audio_streams"
        if cache_key not in self.cache:
            self.cache[cache_key] = self.parse_streams("audio")
        return self.cache[cache_key]

    def stream_duration(self, stream_type: str) -> Decimal:
        tags: dict[str, Any] = getattr(self, f"{stream_type}_streams")[0]["tags"]
        for k, v in tags.items():
            if "duration" in k.lower():
                return parse_timestamp(v)
        raise ValueError("no duration tag found!!")

    def stream_frame_count(self, stream_type: str) -> int:
        frame_count = getattr(self, f"{stream_type}_streams")[0]["nb_read_packets"]
        return int(frame_count)

    def stream_fps(self, stream_type: str) -> Decimal:
        fps = self.stream_frame_count(stream_type) / self.stream_duration(stream_type)
        return fps

    @property
    def video_duration(self) -> Decimal:
        return self.stream_duration("video")

    @property
    def video_frame_count(self) -> int:
        return self.stream_frame_count("video")

    @property
    def video_fps(self) -> Decimal:
        return self.stream_fps("video")

    @property
    def audio_duration(self) -> Decimal:
        return self.stream_duration("audio")

    @property
    def audio_frame_count(self) -> int:
        return self.stream_frame_count("audio")

    @property
    def audio_fps(self) -> Decimal:
        return self.stream_fps("audio")

    @property
    def ffmpeg_output(self) -> str:
        cache_key = "ffmpeg_output"
        if cache_key not in self.cache:
            args = [
                self.config.ffmpeg,
                "-v",
                "warning",
                "-i",
                self.path,
                "-af",
                f"silencedetect={self.config.blackdetect_options},ametadata=mode=print:file=-",
                "-vf",
                f"blackdetect={self.config.silencedetect_options},metadata=mode=print:file=-",
                "-sn",
                "-f",
                "null",
                "-y",
                "-",
            ]
            stdout = self.run_process(args, cache_key)
            self.cache[cache_key] = stdout
        return self.cache[cache_key]

    @property
    def frames(self) -> list[FrameInfo]:
        cache_key = "frames"
        if cache_key not in self.cache:
            ffmpeg_lines = [line.strip() for line in self.ffmpeg_output.splitlines()]
            frames: list[FrameInfo] = []
            frame: Optional[FrameInfo] = None
            for line in ffmpeg_lines:
                if match := re.match(Media.FFMPEG_FRAME_LINE, line):
                    logging.debug(f"ffmpeg frame line: {line}")
                    if frame:
                        frames.append(frame)
                    frame = FrameInfo(line, *match.groups())
                elif match := re.match(Media.FFMPEG_KEY_LINE, line):
                    logging.debug(f"ffmpeg metadata line: {line}")
                    if not frame:
                        raise ValueError(f"frame metadata found, but no frame: {line}")
                    metadata = FrameMetadata(line, *match.groups())
                    if metadata.key in frame.metadata:
                        raise ValueError(
                            f"metadata already exists [{frame.raw}]: {line}"
                        )
                    frame.metadata[metadata.key] = metadata
                else:
                    raise ValueError(f"ffmpeg parsing error: {line}")
            if frame:
                frames.append(frame)
            self.cache[cache_key] = frames
        return self.cache[cache_key]

    def detect_frames(self, cache_key: str, metadata_keys: list[str]):
        if cache_key not in self.cache:
            detect_frames = []
            for frame in self.frames:
                for key, metadata in frame.metadata.items():
                    if key not in metadata_keys:
                        continue
                    detect_frame = DetectMetadata(frame, metadata)
                    detect_frames.append(detect_frame)
            self.cache[cache_key] = detect_frames
        return self.cache[cache_key]

    @property
    def black_frames(self) -> list[DetectMetadata]:
        return self.detect_frames(
            "black_frames", ["lavfi.black_start", "lavfi.black_end"]
        )

    @property
    def silent_frames(self) -> list[DetectMetadata]:
        return self.detect_frames(
            "silent_frames", ["lavfi.silence_start", "lavfi.silence_end"]
        )

    def intervals(
        self, cache_key: str, frames: list[DetectMetadata]
    ) -> list[DetectInterval]:
        if cache_key not in self.cache:
            intervals = []
            i = 0
            while i + 1 < len(frames):
                start_frame = frames[i]
                end_frame = frames[i + 1]
                if start_frame.sub_type != "start" or end_frame.sub_type != "end":
                    raise ValueError(
                        f"expected start and end frames, got {start_frame} and {end_frame}"
                    )
                interval = DetectInterval(start_frame, end_frame)
                intervals.append(interval)
                i += 2
            self.cache[cache_key] = intervals
        return self.cache[cache_key]

    @property
    def black_intervals(self) -> list[DetectInterval]:
        return self.intervals("black_intervals", self.black_frames)

    @property
    def silent_intervals(self) -> list[DetectInterval]:
        return self.intervals("silent_intervals", self.silent_frames)

    @property
    def split_frames(self) -> list[SplitMetadata]:
        cache_key = "split_frames"
        if cache_key not in self.cache:
            split_frames = []
            _black_intervals = self.black_intervals[:]
            _silent_intervals = self.silent_intervals[:]
            log_multiline(
                logging.DEBUG,
                f"black intervals:",
                "\n".join(str(frame) for frame in _black_intervals),
            )
            log_multiline(
                logging.DEBUG,
                f"silent intervals:",
                "\n".join(str(frame) for frame in _silent_intervals),
            )
            while _black_intervals:
                black_interval = _black_intervals.pop(0)
                for i, silent_interval in enumerate(_silent_intervals):
                    if black_interval.overlaps(silent_interval):
                        split_frames.append(
                            SplitMetadata(black_interval, silent_interval)
                        )
                        _silent_intervals.pop(i)
                        break
            log_multiline(
                logging.INFO,
                "split_frames:",
                f"\n|\n".join(str(split) for split in split_frames),
            )
            self.cache[cache_key] = split_frames
        return self.cache[cache_key]

    def split(self):
        index = 0
        while index < len(self.split_frames):
            split_frame = self.split_frames[index]
            frame_start = split_frame.adjusted_start_frame(self.video_fps)
            if index + 1 < len(self.split_frames):
                next_split_frame = self.split_frames[index + 1]
                frame_end = next_split_frame.adjusted_start_frame(self.video_fps)
            else:
                frame_end = self.video_frame_count
            frame_duration = frame_end - frame_start
            output_path = os.path.join(
                self.output_folder, f"{index:0>3}.{self.extension}"
            )
            incomplete_output_path = f"{output_path}.incomplete"
            logging.info(
                f"Encoding {frame_start} -> {frame_end} = {frame_duration} -> {output_path}"
            )
            index += 1
            if os.path.exists(output_path):
                logging.warning(f"skipping {output_path}")
                continue
            if os.path.exists(incomplete_output_path):
                logging.debug(f"removing incomplete: {incomplete_output_path}")
                os.remove(incomplete_output_path)
            args = [
                self.config.handbrake_cli,
                "--preset-import-file",
                self.config.handbrake_presets_import,
                "--preset",
                self.config.handbrake_preset,
                "--no-markers",
                "--start-at",
                f"frames:{frame_start}",
                "--stop-at",
                f"frames:{frame_duration}",
                "-i",
                self.path,
                "-o",
                incomplete_output_path,
            ]
            self.run_process(args)
            logging.debug(
                f"renaming {os.path.basename(incomplete_output_path)} -> {os.path.basename(output_path)}"
            )
            os.rename(incomplete_output_path, output_path)
