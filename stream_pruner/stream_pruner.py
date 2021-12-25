import json
import os
import subprocess
import sys
from typing import Optional


def parse_mkv(data: str):
    parsed_data = json.loads(data)
    for track in parsed_data["tracks"]:
        if track["type"] == "video":
            print(track)


def identify(mkv_path: str) -> dict:
    args = [config.mkvmerge, "--identify", "--identification-format", "json", mkv_path]
    cp = subprocess.run(args, check=True, capture_output=True)
    json_dict = json.loads(cp.stdout)
    return json_dict


class Track:
    def __init__(self, track_json: dict):
        self.raw: dict = track_json
        self.id: int = track_json["id"]
        self.type: str = track_json["type"]
        self.properties: dict = track_json["properties"]
        self.default: bool = self.properties.get("default_track", False)
        self.enabled: bool = self.properties.get("enabled_track", False)
        self.original: bool = self.properties.get("flag_original", False)
        self.forced_prop: bool = self.properties.get("forced_track", False)
        self.language: str = self.properties.get("language", "und")
        self.track_name: str = self.properties.get("track_name", "")
        self.codec = self.properties.get("codec_id", "")[2:]

    @property
    def forced(self) -> bool:
        return self.forced_prop or "forced" in self.track_name.lower()


class VideoTrack(Track):
    pass


class AudioTrack(Track):
    def __str__(self):
        track_name = self.track_name or "<und>"
        return f"[{self.id}][{self.codec}].{self.language}: {track_name} (Enabled: {self.enabled}, Original: {self.original}), Default: {self.default}, Forced: {self.forced}"

    def __repr__(self):
        return str(self)


class SubtitleTrack(Track):
    def __init__(self, track_json: dict):
        super().__init__(track_json)
        self.hearing_impaired_prop: bool = self.properties.get(
            "flag_hearing_impaired", False
        )

    @property
    def hearing_impaired(self) -> bool:
        return self.hearing_impaired_prop or "SDH" in self.track_name.lower()

    def __str__(self):
        track_name = self.track_name or "<und>"
        return f"[{self.id}].{self.language}: {track_name} (Enabled: {self.enabled}, Original: {self.original}, Default: {self.default}, Forced: {self.forced})"

    def __repr__(self):
        return str(self)


class MkvData:
    def __init__(self, path: str):
        self.path: str = path
        self.raw: dict = identify(path)
        self.base: str = os.path.dirname(path)
        self.file: str = os.path.basename(path)
        self.video_tracks: list[VideoTrack] = []
        self.audio_tracks: list[AudioTrack] = []
        self.subtitle_tracks: dict[str, list[SubtitleTrack]] = {}
        self.other_tracks: list[Track] = []
        for track in sorted(self.raw["tracks"], key=lambda t: t["id"]):
            if track["type"] == "video":
                self.video_tracks.append(VideoTrack(track))
            elif track["type"] == "audio":
                self.audio_tracks.append(AudioTrack(track))
            elif track["type"] == "subtitles":
                sub_track = SubtitleTrack(track)
                self.subtitle_tracks.setdefault(sub_track.language, []).append(
                    sub_track
                )
            else:
                self.other_tracks.append(Track(track))
        self.audio_tracks.sort(
            key=lambda at: (at.enabled, at.original, at.default), reverse=True
        )

        default_lang = self.audio_tracks[0].language if self.audio_tracks else "eng"
        for lang in ["eng", "kor"]:
            if lang not in self.subtitle_tracks:
                continue
            self.subtitle_tracks[lang].sort(
                key=lambda st: (st.enabled, st.original, not st.forced, st.default),
                reverse=True,
            )


def prune_files(path: str):
    mkv_data = []
    for mkv_path in sorted(
        entry for entry in os.listdir(path) if entry.endswith(".mkv")
    ):
        mkv_data.append(MkvData(os.path.join(path, mkv_path)))
    for data in mkv_data:
        print(f"** {data.file}")
        if (
            not data.video_tracks
            or not data.audio_tracks
            or "eng" not in data.subtitle_tracks
        ):
            print("!! Manual intervention required")
            continue

        for i, at in enumerate(data.audio_tracks):
            prefix = "++" if i == 0 else "--"
            print(f"{prefix}A {at}")
        audio_lang = data.audio_tracks[0].language
        subtitles: dict[str, dict[str, Optional[SubtitleTrack]]] = {}
        for lang in ["eng", "kor"]:
            lang_subs = subtitles.setdefault(lang, {"forced": None, "full": None})
            for st in data.subtitle_tracks.get(lang, []):
                if not lang_subs["forced"] or not lang_subs["full"]:
                    st_type: str = "forced" if st.forced else "full"
                    if not lang_subs[st_type]:
                        print(f"++S {st}")
                        lang_subs[st_type] = st
                        continue
                print(f"--S {st}")
        if audio_lang == "eng":
            st_order = [
                subtitles["eng"]["forced"],
                subtitles["eng"]["full"],
                subtitles["kor"]["forced"],
                subtitles["kor"]["full"],
            ]
        else:
            st_order = [
                subtitles["eng"]["full"],
                subtitles["eng"]["forced"],
                subtitles["kor"]["full"],
                subtitles["kor"]["forced"],
            ]
        st_order = [st for st in st_order if st]
        a_tid = data.audio_tracks[0].id
        args = [config.mkvmerge, "-o", os.path.join(data.base, "fixed", data.file)]
        args.extend(["--audio-tracks", str(a_tid), "--default-track", f"{a_tid}:1"])
        args.extend(["--subtitle-tracks", ",".join(str(t.id) for t in st_order)])
        for i, st in enumerate(st_order):
            default_flag = 1 if i == 0 else 0
            forced_flag = 1 if st.forced else 0
            args.extend(["--default-track", f"{st.id}:{default_flag}"])
            args.extend(["--forced-track", f"{st.id}:{forced_flag}"])
            if st.forced:
                args.extend(["--track-episode", f"{st.id}:Forced"])
        args.extend(["--track-order", ",".join(f"0:{st.id}" for st in st_order)])
        args.append(data.path)
        print(" ".join(args))
        cp = subprocess.run(args, check=True, capture_output=True)


if __name__ == "__main__":
    prune_files(sys.argv[1])
