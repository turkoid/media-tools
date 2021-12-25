import logging
import os
from typing import Optional

from core.config import Config
from core.tool import Tool
from core.utils import run_process, log_file_header
from stream_pruner.models import SubtitleTrack, MkvData, AudioTrack, VideoTrack


class StreamPruner(Tool):
    def __init__(self, parsed_args):
        super().__init__(parsed_args)
        self.config = Config(parsed_args.config)
        self.validate()
        self.input = os.path.realpath(parsed_args.input)
        if not os.path.exists(self.input):
            raise FileNotFoundError(self.input)
        self._output_path: Optional[str] = None

    def validate(self):
        pass

    def run(self):
        media_files = self.build_media_files()
        self.prune_files(media_files)

    def output_path(self, media_file: str) -> str:
        if not self._output_path:
            self._output_path = os.path.join(os.path.dirname(media_file), "pruned")
        return self._output_path

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
        pruned_file = os.path.join(
            self.output_path(media_file), os.path.basename(media_file)
        )
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
            help="directory to store pruned files (defaults to path relative to input file)",
        )
