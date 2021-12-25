from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Union

from core.utils import format_timestamp, fps_adjusted_frame


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
        return f"{self.start.frame}-{self.end.frame}"

    @property
    def fps(self) -> Decimal:
        return (self.start.fps + self.end.fps) / 2

    def output(self, include_types: bool = True, include_frames: bool = True):
        type_part = f"[{self.type}] " if include_types else ""
        frame_part = f" | {self.frames}" if include_frames else ""
        return f"{type_part}{self.timestamp_range}{frame_part}"

    def __str__(self):
        return self.output()

    def __repr__(self):
        return str(self)


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

    def output(self, video_fps: Optional[Decimal] = None, prefix=""):
        video_fps = video_fps or self.black_frame.fps
        start_frame = self.adjusted_silent_start_frame(video_fps)
        end_frame = self.adjusted_silent_end_frame(video_fps)
        return f"{prefix}{self.black_frame}\n{prefix}{self.silent_frame} ({start_frame}-{end_frame})"

    def __str__(self):
        return self.output()


@dataclass
class Clip:
    frame_start: int
    frame_end: int
    time_start: Decimal
    time_end: Decimal

    @property
    def frames(self):
        return self.frame_end - self.frame_start

    @property
    def duration(self):
        return self.time_end - self.time_start
