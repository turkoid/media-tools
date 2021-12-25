import logging
import os
from typing import Optional

from core.config import Config
from core.tool import Tool
from core.utils import run_process, log_file_header, validate_paths
from stream_pruner.models import SubtitleTrack, MkvData, AudioTrack, VideoTrack


class StreamPruner(Tool):
    def __init__(self, parsed_args):
        super().__init__(parsed_args)
        self.config = Config(parsed_args.config)
        self.input = os.path.abspath(parsed_args.input)
        if os.path.isdir(self.input):
            self.input_directory = self.input
        else:
            self.input_directory = os.path.dirname(self.input)
        self.output_directory: Optional[str] = parsed_args.output_directory
        output_path = os.path.join(self.input_directory, self.output_directory)
        if self.output_directory is None:
            output_path = os.path.join(output_path, "pruned")
        self.output_path = os.path.abspath(output_path)
        self.validate()

    def validate(self):
        validate_paths(self.config.mkvmerge, self.input)
        if os.path.samefile(self.input_directory, self.output_path):
            raise ValueError(
                f"output directory must be different then the input directory"
            )

    def run(self):
        media_files = self.build_media_files()
        os.makedirs(self.output_path, exist_ok=True)
        self.prune_files(media_files)

    def _filter_video_tracks(self, data: MkvData) -> list[VideoTrack]:
        tracks = []
        for track in data.video_tracks:
            tracks.append(track)
            logging.info(f"++V {track}")
        return tracks

    def _filter_audio_tracks(self, data: MkvData) -> list[AudioTrack]:
        first_track = data.audio_tracks[0]
        tracks = [first_track]
        logging.info(f"++A {first_track}")
        for track in data.audio_tracks[1:]:
            logging.info(f"--A {track}")
        return tracks

    def _filter_subtitle_tracks(
        self, data: MkvData, default_lang: str
    ) -> list[SubtitleTrack]:
        subtitles: dict[str, dict[str, Optional[SubtitleTrack]]] = {}
        for lang in ["eng", "kor"]:
            lang_subs = subtitles.setdefault(lang, {"forced": None, "full": None})
            for track in data.subtitle_tracks.get(lang, []):
                if not lang_subs["forced"] or not lang_subs["full"]:
                    st_type: str = "forced" if track.forced else "full"
                    if not lang_subs[st_type]:
                        logging.info(f"++S {track}")
                        lang_subs[st_type] = track
                        continue
                logging.info(f"--S {track}")
        if default_lang == "eng":
            track_order = [
                subtitles["eng"]["forced"],
                subtitles["eng"]["full"],
                subtitles["kor"]["forced"],
                subtitles["kor"]["full"],
            ]
        else:
            track_order = [
                subtitles["eng"]["full"],
                subtitles["eng"]["forced"],
                subtitles["kor"]["full"],
                subtitles["kor"]["forced"],
            ]
        tracks = [t for t in track_order if t]
        return tracks

    def prune_media(self, media_file: str):
        data = MkvData(media_file, self.config)
        log_file_header(media_file)
        if not data.is_valid():
            track_info = [
                f"V:{bool(data.video_tracks)}",
                f"A:{bool(data.audio_tracks)}",
                f"S:{'eng' in data.subtitle_tracks}",
            ]
            logging.warning(f"missing tracks {' '.join(track_info)}")
            return
        audio_tracks = self._filter_audio_tracks(data)
        subtitle_tracks = self._filter_subtitle_tracks(data, audio_tracks[0].language)
        pruned_file = os.path.join(self.output_path, os.path.basename(media_file))
        args = [
            self.config.mkvmerge,
            "-o",
            pruned_file,
            "--audio-tracks",
            ",".join(str(t.id) for t in audio_tracks),
            "--default-track",
            f"{audio_tracks[0].id}:1",
            "--subtitle-tracks",
            ",".join(str(t.id) for t in subtitle_tracks),
        ]
        for i, track in enumerate(subtitle_tracks):
            default_flag = 1 if i == 0 else 0
            forced_flag = 1 if track.forced else 0
            args.extend(["--default-track", f"{track.id}:{default_flag}"])
            args.extend(["--forced-track", f"{track.id}:{forced_flag}"])
            if track.forced:
                args.extend(["--track-episode", f"{track.id}:Forced"])
        args.extend(
            [
                "--track-order",
                ",".join(f"0:{track.id}" for track in subtitle_tracks),
            ]
        )
        args.append(media_file)
        run_process(args)

    def prune_files(self, media_files: list[str]):
        for media_file in media_files:
            try:
                self.prune_media(media_file)
            except Exception as exc:
                if isinstance(Exception, FileNotFoundError):
                    logging.error(f"File not found: {exc}")
                else:
                    logging.error(exc)
                logging.error(f"!! An error was detected. Aborting...")
                logging.exception(exc)

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "prune", description="prune unneeded tracks from media"
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
            help="directory to store pruned files (defaults to path relative to input file/directory)",
        )
