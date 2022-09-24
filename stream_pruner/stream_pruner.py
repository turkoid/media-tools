import itertools
import logging
import os
import time
from typing import Optional, TypeVar

from core.config import Config
from core.tool import Tool
from core.utils import run_process, log_file_header, validate_paths, identify
from stream_pruner.models import SubtitleTrack, MkvData, AudioTrack, VideoTrack, Track

T = TypeVar("T", bound=Track)


class StreamPruner(Tool):
    def __init__(self, parsed_args):
        super().__init__(parsed_args)
        self.dry_run = parsed_args.dry_run
        self.config = Config(parsed_args.config)
        self.input = os.path.abspath(parsed_args.input)
        self.recursive = parsed_args.recursive
        if os.path.isdir(self.input):
            self.input_directory = self.input
        else:
            self.input_directory = os.path.dirname(self.input)
        self.video_lang_codes = self._normalize_lang_codes(parsed_args.video_lang or [])
        self.audio_lang_codes = self._normalize_lang_codes(parsed_args.audio_lang or [])
        self.subtitle_lang_codes = self._normalize_lang_codes(
            parsed_args.subtitle_lang or []
        )
        self.prefer_text_subtitles: bool = parsed_args.prefer_text_subtitles
        self.prefer_sdh_subtitles: bool = parsed_args.prefer_sdh_subtitles
        self.undefined_lang: str = parsed_args.undefined_lang.lower() or "und"
        self.output_directory: Optional[str] = parsed_args.output_directory
        output_path = os.path.join(
            self.input_directory, self.output_directory or "pruned"
        )
        self.output_path = os.path.abspath(output_path)
        self.validate()

    def _normalize_lang_codes(self, lang_codes: list[str]) -> list[str]:
        return list(dict.fromkeys(lang.lower() for lang in lang_codes))

    def validate(self):
        validate_paths(self.config.mkvmerge, self.input)
        if os.path.realpath(self.input_directory) == os.path.realpath(self.output_path):
            raise ValueError(
                "output directory must be different then the input directory"
            )

    def run(self):
        media_files = self.build_media_files(self.recursive, [self.output_path])
        os.makedirs(self.output_path, exist_ok=True)
        self.prune_files(media_files)

    def _filter_tracks(self, tracks: list[T], lang_codes: list[str]) -> list[T]:
        filtered_tracks = [
            track
            for track in tracks
            if not lang_codes or track.lang_code(self.undefined_lang) in lang_codes
        ]
        return filtered_tracks

    def _sort_tracks(self, tracks: list[T], lang_codes: list[str]) -> list[T]:
        track_lang_codes = [t.lang_code(self.undefined_lang) for t in tracks]
        lang_codes = list(dict.fromkeys(lang_codes + track_lang_codes))
        sorted_tracks = sorted(
            tracks, key=lambda t: lang_codes.index(t.lang_code(self.undefined_lang))
        )
        return sorted_tracks

    def _filter_video_tracks(self, data: MkvData) -> list[VideoTrack]:
        tracks = self._filter_tracks(data.video_tracks, self.video_lang_codes)
        tracks = self._sort_tracks(tracks, self.video_lang_codes)
        return tracks

    def _filter_audio_tracks(self, data: MkvData) -> list[AudioTrack]:
        tracks = self._filter_tracks(data.audio_tracks, self.audio_lang_codes)
        tracks = sorted(
            tracks,
            key=lambda t: (t.enabled, t.original, t.default),
            reverse=True,
        )
        tracks = self._sort_tracks(tracks, self.audio_lang_codes)
        return tracks

    def _filter_subtitle_tracks(
        self, data: MkvData, default_lang: str
    ) -> list[SubtitleTrack]:
        tracks = self._filter_tracks(data.subtitle_tracks, self.subtitle_lang_codes)
        lang_codes = self.subtitle_lang_codes or list(
            dict.fromkeys(t.lang_code(self.undefined_lang) for t in tracks)
        )
        prefer_forced = lang_codes[0] == default_lang
        tracks = sorted(
            tracks,
            key=lambda t: (
                t.enabled,
                t.original,
                t.forced == prefer_forced,
                t.default,
                t.vobsub == self.prefer_text_subtitles,
                t.hearing_impaired == self.prefer_sdh_subtitles,
            ),
            reverse=True,
        )
        tracks = self._sort_tracks(tracks, self.subtitle_lang_codes)
        return tracks

    def _output_track_operations(
        self, old_tracks: list[Track], new_tracks: list[Track]
    ) -> bool:
        new_track_order = {t.id: i for i, t in enumerate(new_tracks)}
        old_track_order = {
            t.id: i
            for i, t in enumerate(ot for ot in old_tracks if ot.id in new_track_order)
        }
        for new_index, track in enumerate(new_tracks):
            old_index = old_track_order[track.id]
            if new_index < old_index:
                move = "↑"
            elif new_index > old_index:
                move = "↓"
            else:
                move = "-"
            logging.info(f"++{move}{track.short_type} {track}")
        for track in old_tracks:
            if track.id in new_track_order:
                continue
            logging.info(f"---{track.short_type} {track}")
        if len(new_tracks) == len(old_tracks) and list(new_track_order.keys()) == list(
            old_track_order.keys()
        ):
            logging.warning("All tracks will be the same")
            return False
        return True

    def prune_media(self, media_file: str):
        prune_start = time.perf_counter()
        data = MkvData(media_file, identify(self.config.mkvmerge, media_file))
        log_file_header(media_file)
        if not data.is_valid():
            track_info = [
                f"V:{bool(data.video_tracks)}",
                f"A:{bool(data.audio_tracks)}",
                f"S:{bool(data.subtitle_tracks)}",
            ]
            logging.warning(f"missing tracks {' '.join(track_info)}")
            return
        video_tracks = self._filter_video_tracks(data)
        audio_tracks = self._filter_audio_tracks(data)
        default_lang = (
            self.audio_lang_codes[0]
            if self.audio_lang_codes
            else audio_tracks[0].language
        )
        subtitle_tracks = self._filter_subtitle_tracks(data, default_lang)
        if not video_tracks or not audio_tracks:
            logging.error(f"No video and/or audio tracks found")
            return
        new_tracks = list(itertools.chain(video_tracks, audio_tracks, subtitle_tracks))
        tracks_changed = self._output_track_operations(data.tracks, new_tracks)
        if not tracks_changed:
            logging.info("...skipped")
            return
        pruned_file = os.path.join(
            self.output_path, os.path.relpath(media_file, start=self.input_directory)
        )
        args = [
            self.config.mkvmerge,
            "-o",
            pruned_file,
            "--video-tracks",
            ",".join(str(t.id) for t in video_tracks),
            "--audio-tracks",
            ",".join(str(t.id) for t in audio_tracks),
        ]
        if subtitle_tracks:
            args.extend(
                [
                    "--subtitle-tracks",
                    ",".join(str(t.id) for t in subtitle_tracks),
                ]
            )
        short_type = None
        for track in new_tracks:
            default_flag = 1 if short_type != track.short_type else 0
            short_type = track.short_type
            args.extend(["--default-track", f"{track.id}:{default_flag}"])
        args.extend(
            [
                "--track-order",
                ",".join(f"0:{t.id}" for t in new_tracks),
            ]
        )
        args.append(media_file)
        if self.dry_run:
            command = " ".join(args)
            logging.debug(f"running command={command}")
            logging.info(f"dry run: pruning...")
        else:
            logging.info("pruning...")
            run_process(args)
            logging.info("...done!")
        prune_stop = time.perf_counter()
        elapsed = prune_stop - prune_start
        logging.info(f"took {elapsed:.3f}s")
        logging.info(f"output: {pruned_file}")

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
            "prune", description="prunes and reorders tracks from media"
        )
        parser.add_argument(
            "--input",
            "-i",
            required=True,
            help="path to directory or file. If directory, the script will handle only video files",
        )
        parser.add_argument(
            "--recursive", "-r", action="store_true", help="recursively find all files"
        )
        parser.add_argument(
            "-v",
            "--video-lang",
            nargs="*",
            help="list of language codes to keep and in which order. (uses 3 letter ISO 639-3 codes)",
        )
        parser.add_argument(
            "-a",
            "--audio-lang",
            nargs="*",
            help="list of language codes to keep and in which order. (uses 3 letter ISO 639-3 codes)",
        )
        parser.add_argument(
            "-s",
            "--subtitle-lang",
            nargs="*",
            help="list of language codes to keep and in which order. (uses 3 letter ISO 639-3 codes)",
        )
        parser.add_argument(
            "--prefer-text-subtitles",
            action="store_true",
            help="text based subtitles will be preferred over image based subtitles",
        )
        parser.add_argument(
            "--prefer-sdh-subtitles",
            action="store_true",
            help="hearing imparied subtitles will be preferred",
        )
        parser.add_argument(
            "--undefined-lang",
            default="eng",
            help="language code to use if undefined (default: %(default)s)",
        )
        parser.add_argument(
            "--output-directory",
            "-o",
            help="directory to store pruned files (defaults to path relative to input file/directory)",
        )
