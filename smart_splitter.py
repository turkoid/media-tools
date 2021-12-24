import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Union

from config import Config

config = Config()


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


@dataclass
class FrameMetadata:
    raw: str
    filter: str
    type: str
    sub_type: Optional[str]
    value: str

    @property
    def key(self):
        sub_type = f"_{self.sub_type}" if self.sub_type else ""
        return f"{self.filter}.{self.type}{sub_type}"

    def __str__(self):
        return f"{self.key}={self.value}"

    def __repr__(self):
        return str(self)


@dataclass
class FrameInfo:
    raw: str
    frame: int
    pts: int
    pts_time: Decimal
    metadata: dict[str, FrameMetadata]

    def __init__(
        self,
        raw: str,
        frame: Union[str, int],
        pts: Union[str, int],
        pts_time: Union[str, Decimal],
    ):
        self.raw = raw
        self.frame = frame if isinstance(frame, int) else int(frame)
        self.pts = pts if isinstance(pts, int) else int(pts)
        self.pts_time = pts_time if isinstance(pts_time, Decimal) else Decimal(pts_time)
        self.metadata = {}

    def __str__(self):
        return f"frame: {self.frame} pts: {self.pts} pts_time: {self.pts_time}"

    def __repr__(self):
        return str(self)


@dataclass
class DetectMetadata:
    type: str
    sub_type: str
    frame: int
    pts: int
    pts_time: Decimal
    timestamp: Decimal

    def __init__(self, frame_info: FrameInfo, frame_metadata: FrameMetadata):
        self.type = frame_metadata.type
        self.sub_type = frame_metadata.sub_type
        self.frame = frame_info.frame
        self.pts = frame_info.pts
        self.pts_time = frame_info.pts_time
        self.timestamp = Decimal(frame_metadata.value)

    @property
    def short_type(self) -> str:
        return "B" if self.type == "black" else "S"

    @property
    def fps(self) -> Decimal:
        if self.pts_time.is_zero():
            return Decimal("-1")
        return self.frame / self.pts_time

    def output(self, include_type: bool = True, include_frame: bool = True) -> str:
        sub_type = f"-{self.sub_type.upper()}" if self.sub_type else ""
        type_part = f"[{self.short_type}{sub_type}] " if include_type else ""
        frame_part = f" ({self.frame})" if include_frame else ""
        return f"{type_part}{format_timestamp(self.timestamp)}{frame_part}"

    def __str__(self):
        return self.output()

    def __repr__(self):
        return str(self)


@dataclass
class DetectInterval:
    start: DetectMetadata
    end: DetectMetadata

    def overlaps(self, other: "DetectInterval", tolerance: Decimal = Decimal("0.5")):
        st, et = self.start.timestamp, self.end.timestamp
        other_st, other_et = other.start.timestamp, other.end.timestamp
        if st >= other_st and et <= other_et:
            return True
        if st > other_et or et < other_st:
            return False
        if abs(st - other_st) <= tolerance or abs(et - other_et) <= tolerance:
            return True
        return False

    @property
    def type(self) -> str:
        interval_type = self.start.short_type
        if interval_type != self.end.short_type:
            interval_type += self.end.short_type
        return interval_type

    @property
    def timestamp_range(self) -> str:
        return f"{self.start.output(False, False)} - {self.end.output(False, False)}"

    @property
    def frames(self):
        return f"{self.start.frame}:{self.end.frame}"

    @property
    def fps(self) -> Decimal:
        return (self.start.fps + self.end.fps) / 2

    def output(self, include_types: bool = True, include_frames: bool = True):
        type_part = f"[{self.type}] " if include_types else ""
        frame_part = f" {self.frames}" if include_frames else ""
        return f"{type_part}{self.timestamp_range}{frame_part}"

    def __str__(self):
        return self.output()

    def __repr__(self):
        return str(self)


def fps_adjusted_frame(secs: Decimal, fps: Decimal) -> int:
    return round(fps * secs)


@dataclass
class SplitMetadata:
    black_frame: DetectInterval
    silent_frame: DetectInterval

    def adjusted_silent_start_frame(self, video_fps: Decimal) -> int:
        return fps_adjusted_frame(self.silent_frame.start.timestamp, video_fps)

    def adjusted_silent_end_frame(self, video_fps: Decimal) -> int:
        return fps_adjusted_frame(self.silent_frame.end.timestamp, video_fps)

    def adjusted_start_frame(self, video_fps: Decimal) -> int:
        start_frame = self.adjusted_silent_start_frame(video_fps)
        end_frame = self.adjusted_silent_end_frame(video_fps)
        return int((start_frame + end_frame) / 2)

    def average_start_timestamp(self) -> Decimal:
        return (self.silent_frame.start.timestamp + self.silent_frame.end.timestamp) / 2

    def output(self, video_fps: Optional[Decimal] = None):
        video_fps = video_fps or self.black_frame.fps
        start_frame = self.adjusted_silent_start_frame(video_fps)
        end_frame = self.adjusted_silent_end_frame(video_fps)
        return f"{self.black_frame}\n{self.silent_frame} | {start_frame}:{end_frame}"

    def __str__(self):
        return self.output()


def log_multiline(level, header, message):
    logging.log(level, f"{header}\n{message}")


class Media:
    REGEX = r"^.+(S\d+E\d+)\.(.+)\.(.+)$"
    FFMPEG_FRAME_LINE = r"frame:(\d+)\s+pts:(\d+)\s+pts_time:(-?\d+\.?\d*)"
    FFMPEG_KEY_LINE = r"(.+)\.(.+?)(?:_([^_]+?))?=(-?\d+\.?\d*)"

    def __init__(self, path: str):
        self.path: str = path
        self.cache: dict[str, Any] = {}
        self.detection_duration: float = 0.5
        if not (match := re.match(Media.REGEX, self.basename)):
            raise ValueError(self.basename)
        self.episode: str = match.group(1)
        self.titles: list[str] = match.group(2).replace(".", " ").split(" - ")
        self.extension: str = match.group(3)
        self.setup_logging()
        self.log_basic_info()

    def log_basic_info(self):
        for stream_type in ["video", "audio"]:
            logging.info(
                f"{stream_type} frame count: {self.stream_frame_count(stream_type)}"
            )
            logging.info(f"{stream_type} duration: {self.stream_duration(stream_type)}")
            logging.info(f"{stream_type} fps: {self.stream_fps(stream_type)}")

    @property
    def directory(self):
        return os.path.dirname(self.path)

    @property
    def basename(self):
        return os.path.basename(self.path)

    def setup_logging(self):
        log_dir = os.path.join(self.directory, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{self.episode}.log")
        fh = logging.FileHandler(log_file, mode="w")
        setup_logging_handler(fh)
        logging.getLogger().addHandler(fh)

    def run_process(self, args: list[str], cache_key: Optional[str] = None) -> str:
        logging.debug(f"running {args}")
        stdout = None
        cache_file = os.path.join(
            self.cache_directory, f"{self.episode}.{cache_key}.txt"
        )
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
                config.ffprobe,
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
    def cache_directory(self) -> str:
        cache_dir = os.path.join(self.directory, "cache")
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir

    @property
    def ffmpeg_output(self) -> str:
        cache_key = "ffmpeg_output"
        if cache_key not in self.cache:
            args = [
                config.ffmpeg,
                "-v",
                "warning",
                "-i",
                self.path,
                "-af",
                f"silencedetect=noise=-60dB:d={self.detection_duration},ametadata=mode=print:file=-",
                "-vf",
                f"blackdetect=d={self.detection_duration},metadata=mode=print:file=-",
                # "-vn",
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

    @property
    def output_directory(self):
        output_dir = os.path.join(self.directory, "output", self.episode)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def split_ffmpeg(self):
        output_path = os.path.join(self.output_directory, f"%03d.{self.extension}")
        silence_start_timestamps = [
            str(sf.average_start_timestamp()) for sf in self.split_frames
        ]
        args = [
            config.ffmpeg,
            "-v",
            "warning",
            "-i",
            self.path,
            "-c",
            "copy",
            "-map",
            "0",
            "-f",
            "segment",
            "-segment_times",
            ",".join(silence_start_timestamps),
            "-y",
            output_path,
        ]
        self.run_process(args)

    def split_handbrake(self):
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
                self.output_directory, f"{index:0>3}.{self.extension}"
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
            if self.episode == "S0201":
                print("here")
            args = [
                config.handbrake_cli,
                "--preset-import-file",
                config.smart_splitter["handbrake_presets_import"],
                "--preset",
                config.smart_splitter["handbrake_preset"],
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


def split_by_silence(path: str):
    logging.info(f"finding files in {path}")
    for episode in sorted(os.listdir(path)):
        input_path = os.path.join(path, episode)
        if os.path.isdir(input_path):
            logging.warning(f"skipping {input_path}")
            continue
        old_handlers = logging.getLogger().handlers[:]
        try:
            line = "*" * (len(episode) + 4)
            logging.info(f"\n{line}\n* {episode} *\n{line}")
            media = Media(input_path)
            media.split_handbrake()
        except Exception as e:
            logging.error(f"!! An error was detected. Aborting {input_path}")
            logging.exception(e, exc_info=True)
        finally:
            remove_handlers = [
                h for h in logging.getLogger().handlers if h not in old_handlers
            ]
            for handler in remove_handlers:
                logging.getLogger().removeHandler(handler)


def split_all(path: str):
    logging.info(f"finding seasons in {path}")
    for season in sorted(os.listdir(path)):
        if season == "13":
            continue
        season_path = os.path.join(path, season)
        if not os.path.isdir(season_path):
            logging.warning(f"skipping {season_path}")
            continue
        split_by_silence(season_path)


def setup_logging_handler(handler: logging.Handler, level=logging.DEBUG):
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)


def setup_logging(base_logging_dir: str):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    setup_logging_handler(sh, logging.INFO)
    logger.addHandler(sh)
    fh = logging.FileHandler(
        filename=os.path.join(base_logging_dir, "master.log"), mode="w"
    )
    setup_logging_handler(fh)
    logger.addHandler(fh)


def run(args: list[str]):
    for path in [config.ffmpeg, config.ffprobe, config.handbrake_cli]:
        assert os.path.exists(path), path
    with open(config.smart_splitter["handbrake_presets_import"]) as fh:
        preset = config.smart_splitter["handbrake_preset"]
        assert preset in fh.read(), preset
    base_dir = args[1]
    setup_logging(base_dir)
    split_all(base_dir)


if __name__ == "__main__":
    run(sys.argv)
