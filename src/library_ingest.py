"""Conservatively link downloaded music into SoundCloud playlist folders."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from mutagen import File as MutagenFile
from rapidfuzz import fuzz

LIKED_SOURCE = "liked"
DEFAULT_ROOT = Path.home() / "Documents/media/dj"
TRACKS_PATH = Path("data/tracks.parquet")
MANIFEST_PATH = Path("data/library_manifest.json")
AUDIO_SUFFIXES = {".aif", ".aiff", ".flac", ".m4a", ".mp3", ".ogg", ".wav"}
VERSION_WORDS = {"acapella", "bootleg", "dub", "edit", "extended", "instrumental", "mix", "original", "radio", "remix", "vip"}


@dataclass(frozen=True)
class AudioMetadata:
    title: str
    artist: str
    isrc: str
    duration_seconds: float | None
    bpm: float | None
    key: str


@dataclass(frozen=True)
class Candidate:
    row: dict[str, Any]
    score: float


def normalized(value: object) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.casefold())).strip()


def filename_metadata(path: Path) -> tuple[str, str]:
    stem = re.sub(r"\s+", " ", path.stem.replace("_", " ")).strip()
    parts = re.split(r"\s+-\s+", stem, maxsplit=1)
    return (parts[1], parts[0]) if len(parts) == 2 else (stem, "")


def first_tag(tags: Any, *keys: str) -> str:
    if not tags:
        return ""
    for key in keys:
        value = tags.get(key)
        if value:
            return str(value[0] if isinstance(value, list) else value).strip()
    return ""


def read_metadata(path: Path) -> AudioMetadata:
    filename_title, filename_artist = filename_metadata(path)
    try:
        audio = MutagenFile(path, easy=True)
        tags = getattr(audio, "tags", None)
        duration = getattr(getattr(audio, "info", None), "length", None)
    except Exception:
        tags, duration = None, None
    return AudioMetadata(
        title=first_tag(tags, "title") or filename_title,
        artist=first_tag(tags, "artist", "albumartist") or filename_artist,
        isrc=first_tag(tags, "isrc"),
        duration_seconds=float(duration) if duration else None,
        bpm=parse_number(first_tag(tags, "bpm")),
        key=first_tag(tags, "initialkey", "key"),
    )


def parse_number(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def version_words(value: str) -> set[str]:
    return set(normalized(value).split()) & VERSION_WORDS


def score_track(metadata: AudioMetadata, row: dict[str, Any]) -> float:
    if metadata.isrc and normalized(metadata.isrc) == normalized(row.get("isrc")):
        return 1.0
    title_score = fuzz.token_set_ratio(normalized(metadata.title), normalized(row.get("title"))) / 100
    artist_score = fuzz.token_set_ratio(normalized(metadata.artist), normalized(row.get("artist"))) / 100 if metadata.artist else 0.0
    combined_score = fuzz.token_set_ratio(
        normalized(f"{metadata.artist} {metadata.title}"),
        normalized(f"{row.get('artist', '')} {row.get('title', '')}"),
    ) / 100
    score = 0.55 * title_score + 0.30 * artist_score + 0.15 * combined_score
    remote_duration = row.get("duration_ms")
    if metadata.duration_seconds and remote_duration:
        difference = abs(metadata.duration_seconds - float(remote_duration) / 1000)
        score += 0.08 * max(0.0, 1.0 - difference / 12)
    if version_words(metadata.title) ^ version_words(str(row.get("title", ""))):
        score -= 0.15
    return max(0.0, min(1.0, score))


def candidates(metadata: AudioMetadata, tracks: pl.DataFrame) -> list[Candidate]:
    ranked = [Candidate(row, score_track(metadata, row)) for row in tracks.iter_rows(named=True)]
    return sorted(ranked, key=lambda candidate: candidate.score, reverse=True)[:5]


def high_confidence(ranked: list[Candidate]) -> bool:
    return bool(ranked) and ranked[0].score >= 0.94 and (len(ranked) == 1 or ranked[0].score - ranked[1].score >= 0.08)


def safe_name(value: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "-", value).strip(" .") or "Untitled playlist"


def load_manifest() -> dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return {"files": {}}
    with MANIFEST_PATH.open() as manifest_file:
        return json.load(manifest_file)


def save_manifest(manifest: dict[str, Any]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("w") as manifest_file:
        json.dump(manifest, manifest_file, indent=2, sort_keys=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as music_file:
        for chunk in iter(lambda: music_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize(path: Path, candidate: Candidate, root: Path, manifest: dict[str, Any], dry_run: bool) -> None:
    file_hash = sha256(path)
    if file_hash in manifest["files"]:
        print(f"Already ingested: {path.name}")
        return
    library = root / "_library"
    library.mkdir(parents=True, exist_ok=True)
    target = library / path.name
    if target.exists():
        target = library / f"{candidate.row['urn'].rsplit(':', 1)[-1]} - {path.name}"
    playlists = [name for name in candidate.row.get("playlists", []) if name != LIKED_SOURCE]
    print(f"{path.name} -> {candidate.row['artist']} - {candidate.row['title']} ({candidate.score:.0%})")
    if dry_run:
        return
    shutil.move(str(path), target)
    for playlist in playlists:
        folder = root / safe_name(playlist)
        folder.mkdir(parents=True, exist_ok=True)
        link = folder / target.name
        if not link.exists():
            os.link(target, link)
    manifest["files"][file_hash] = {"urn": candidate.row["urn"], "library_path": str(target)}
    save_manifest(manifest)


def choose_candidate(path: Path, ranked: list[Candidate]) -> Candidate | None:
    print(f"\nReview required: {path.name}")
    for number, candidate in enumerate(ranked, start=1):
        row = candidate.row
        print(f"  {number}. {candidate.score:.0%}  {row.get('artist', '')} - {row.get('title', '')}\n     {row.get('soundcloud_url', '')}")
    choice = input("Choose 1-5, or Enter to leave in inbox: ").strip()
    return ranked[int(choice) - 1] if choice.isdigit() and 1 <= int(choice) <= len(ranked) else None


def ingest(root: Path, review: bool, dry_run: bool) -> None:
    if not TRACKS_PATH.exists():
        raise SystemExit("Missing data/tracks.parquet. Run the SoundCloud sync first.")
    inbox = root / "_inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    tracks = pl.read_parquet(TRACKS_PATH)
    if "urn" not in tracks.columns:
        tracks = tracks.with_columns((pl.lit("soundcloud:tracks:") + pl.col("id").cast(pl.Utf8)).alias("urn"))
    manifest = load_manifest()
    for path in sorted(inbox.iterdir()):
        if not path.is_file() or path.suffix.lower() not in AUDIO_SUFFIXES:
            continue
        ranked = candidates(read_metadata(path), tracks)
        candidate = ranked[0] if high_confidence(ranked) else (choose_candidate(path, ranked) if review else None)
        if candidate:
            materialize(path, candidate, root, manifest, dry_run)
        else:
            print(f"Left for review: {path.name}")


def initialise(root: Path) -> None:
    if not TRACKS_PATH.exists():
        raise SystemExit("Missing data/tracks.parquet. Run the SoundCloud sync first.")
    root.mkdir(parents=True, exist_ok=True)
    (root / "_inbox").mkdir(exist_ok=True)
    (root / "_library").mkdir(exist_ok=True)
    tracks = pl.read_parquet(TRACKS_PATH)
    playlists = {playlist for row in tracks.iter_rows(named=True) for playlist in row.get("playlists", []) if playlist != LIKED_SOURCE}
    for playlist in playlists:
        (root / safe_name(playlist)).mkdir(exist_ok=True)
    print(f"Created library folders in {root}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "ingest", "watch"))
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--review", action="store_true", help="prompt for uncertain matches")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args()
    if args.command == "init":
        initialise(args.root)
    elif args.command == "ingest":
        ingest(args.root, args.review, args.dry_run)
    else:
        print("Watching inbox; only high-confidence matches are ingested. Press Ctrl-C to stop.")
        while True:
            ingest(args.root, review=False, dry_run=False)
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
