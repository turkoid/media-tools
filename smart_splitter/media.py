import asyncio
import json
import logging
import os
import re
from decimal import Decimal
from functools import partial
from subprocess import CompletedProcess
from typing import Any, Optional

from tqdm import tqdm
import yaml

from smart_splitter.config import SmartSplitterConfig
from smart_splitter.models import (
    FrameInfo,
    FrameMetadata,
    DetectMetadata,
    DetectInterval,
    SplitMetadata,
    Clip,
)
from core.utils import (
    init_logging_handler,
    log_multiline,
    format_timestamp,
    run_process,
    async_run_process,
    monitor_handbrake_encode,
    parse_duration,
)


class Media:
    FFMPEG_FRAME_LINE = r"frame:(\d+)\s+pts:(\d+)\s+pts_time:(-?\d+\.?\d*)"
    FFMPEG_KEY_LINE = r"(?:([^.]+?)\.)?(.+?)(?:_([^_]+?))?=(.+)"

    def __init__(self, path: str, output_folder: str, config: SmartSplitterConfig):
        self.path: str = os.path.abspath(path)
        self.output_folder: str = output_folder
        self.config: SmartSplitterConfig = config
        self.cache: dict[str, Any] = {}
        self.extension: str = os.path.splitext(path)[1]
        self.init_logger()
        self.log_basic_info()

    def log_basic_info(self):
        for stream_type in ["video", "audio"]:
            logging.debug(
                f"{stream_type} frame count: {self.stream_frame_count(stream_type)}"
            )
            logging.debug(
                f"{stream_type} duration: {self.stream_duration(stream_type)}"
            )
            logging.debug(f"{stream_type} fps: {self.stream_fps(stream_type)}")

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
        command = " ".join(args)
        logging.debug(f"running {command}")
        stdout = None
        cache_file = os.path.join(self.cache_directory, f"{cache_key}.txt")
        if cache_key:
            if os.path.exists(cache_file):
                logging.debug(f"reading from cached output: {cache_file}")
                with open(cache_file) as fh:
                    stdout = fh.read()
                if not stdout:
                    logging.warning(f"cache file found, but no output found!")
            else:
                logging.debug("cache file not found")
        if not stdout:
            if cache_key:
                logging.info(f"\nBuilding cached output: {command}")
            stdout = run_process(args)
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
        stream_info: dict[str, Any] = getattr(self, f"{stream_type}_streams")[0]
        tags: dict[str, Any] = stream_info["tags"]
        for k, v in tags.items():
            if "duration" in k.lower():
                return parse_duration(v)
        if "duration" in stream_info:
            return parse_duration(stream_info["duration"])
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
                f"silencedetect={self.config.silencedetect_options},ametadata=mode=print:file=-",
                "-vf",
                f"blackdetect={self.config.blackdetect_options},metadata=mode=print:file=-",
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
    def split_points(self) -> list[SplitMetadata]:
        cache_key = "split_points"
        if cache_key not in self.cache:
            split_points = []
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
                        split_points.append(
                            SplitMetadata(black_interval, silent_interval)
                        )
                        _silent_intervals.pop(i)
                        break
            log_multiline(
                logging.DEBUG,
                "split_points:",
                f"\n-----\n".join(sp.output() for sp in split_points),
            )
            self.cache[cache_key] = split_points
        return self.cache[cache_key]

    def clip(
        self, start: Optional[SplitMetadata], end: Optional[SplitMetadata]
    ) -> Clip:
        fps = self.video_fps
        frame_start = start.frame(fps) if start else 0
        clip_start = start.time() if start else Decimal(0)
        frame_end = end.frame(fps) if end else self.video_frame_count
        clip_end = end.time() if end else self.video_duration
        return Clip(frame_start, frame_end, clip_start, clip_end)

    def clips(self) -> list[Clip]:
        min_duration = 3
        split_points = []
        if self.split_points:
            first = self.split_points[0]
            clip_duration = first.time_start()
            if clip_duration >= min_duration:
                logging.debug(
                    f"adding split point at the beginning of the video, duration={format_timestamp(clip_duration)}"
                )
                split_points.append(None)
            split_points.extend(self.split_points)
            last = self.split_points[-1]
            clip_duration = self.video_duration - last.time_end()
            if clip_duration >= min_duration:
                logging.debug(
                    f"adding split point at the end of the video, duration={format_timestamp(clip_duration)}"
                )
                split_points.append(None)
        if len(split_points) <= 1:
            logging.debug(f"clipping whole video")
            split_points = [None, None]
        clips = []
        for index in range(len(split_points) - 1):
            start, end = split_points[index : index + 2]
            clip = self.clip(start, end)
            clips.append(clip)
        return clips

    def _save_info(self, info: dict[str, Clip]):
        info_dict = {"media": os.path.basename(self.path)}
        for file, clip in info.items():
            clip_info = dict()
            clip_info["frame_start"] = clip.frame_start
            clip_info["frame_end"] = clip.frame_end
            clip_info["frames"] = clip.frames
            clip_info["time_start"] = str(clip.time_start)
            clip_info["time_end"] = str(clip.time_end)
            clip_info["duration"] = str(clip.duration)
            clip_info["formatted"] = {}
            clip_info["formatted"]["time_start"] = format_timestamp(clip.time_start)
            clip_info["formatted"]["time_end"] = format_timestamp(clip.time_end)
            clip_info["formatted"]["duration"] = format_timestamp(clip.duration)
            info_dict[file] = clip_info
        with open(os.path.join(self.output_folder, "info.yaml"), "w") as fh:
            yaml.dump(info_dict, fh, sort_keys=False)

    def _split(self, args: list[str]):
        with tqdm(
            total=100,
            desc="Encoding",
            miniters=1,
            delay=1,
            bar_format="{l_bar}{bar:20}| [{elapsed}] ETA: {remaining}",
        ) as pbar:
            handler = partial(
                monitor_handbrake_encode, progress_bar=pbar, data={"current_line": b""}
            )
            cp: CompletedProcess = asyncio.run(
                async_run_process(
                    args,
                    stdout_handler=handler,
                    check=True,
                    text=True,
                )
            )
            if cp.returncode == 0:
                pbar.update(100 - pbar.n)

    def split(self):
        clips = self.clips()
        info: dict[str, Clip] = {}
        for clip_index, clip in enumerate(clips):
            clip_file = f"{clip_index:0>3}{self.extension}"
            clip_path = os.path.join(self.output_folder, clip_file)
            incomplete_clip_path = f"{clip_path}.incomplete"
            clip_index += 1
            logging.info(
                f"Encoding {clip.frames} frames ({clip.frame_start}-{clip.frame_end}) -> {clip_file} [{format_timestamp(clip.duration)}]"
            )
            info[clip_file] = clip
            if os.path.exists(clip_path):
                logging.warning(f"{clip_file} already exists. skipping...")
                continue
            if os.path.exists(incomplete_clip_path):
                logging.debug(f"removing incomplete: {incomplete_clip_path}")
                os.remove(incomplete_clip_path)
            if len(clips) == 1:
                logging.info(f"clip is whole file, creating link...")
                os.link(self.path, clip_path)
                continue
            args = [
                self.config.handbrake_cli,
                "--preset-import-file",
                self.config.handbrake_presets_import,
                "--preset",
                self.config.handbrake_preset,
                "--no-markers",
                "--start-at",
                f"seconds:{clip.time_start}",
                "--stop-at",
                f"seconds:{clip.duration}",
                "-i",
                self.path,
                "-o",
                incomplete_clip_path,
            ]
            if self.config.dry_run:
                logging.info("...dry run")
            else:
                self._split(args)
                logging.debug(
                    f"renaming {os.path.basename(incomplete_clip_path)} -> {clip_file}"
                )
                os.rename(incomplete_clip_path, clip_path)
                logging.info("...done!")
        self._save_info(info)
