from __future__ import annotations

import webbrowser
from pathlib import Path

import polars as pl
from rapidfuzz import fuzz
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import Button, DataTable, Input, Label, SelectionList

try:  # Support both `python src/djapp.py` and `python -m src.djapp`.
    from .library_ingest import (
        AUDIO_SUFFIXES,
        DEFAULT_ROOT,
        Candidate,
        candidates,
        high_confidence,
        initialise,
        load_manifest,
        materialize,
        materialize_unmatched,
        normalized,
        playlist_names,
        read_metadata,
    )
except ImportError:
    from library_ingest import (
        AUDIO_SUFFIXES,
        DEFAULT_ROOT,
        Candidate,
        candidates,
        high_confidence,
        initialise,
        load_manifest,
        materialize,
        materialize_unmatched,
        normalized,
        playlist_names,
        read_metadata,
    )


class IngestionScreen(Screen[None]):
    """Review inbox files and explicitly link each one to a SoundCloud track."""

    CSS = """
    #ingestion-actions, #ingestion-candidates {
        height: 3;
        padding: 0 2;
        align: left middle;
    }

    #ingestion-actions Button, #ingestion-candidates Button {
        margin-right: 1;
    }

    #ingestion-table {
        height: 1fr;
    }

    #manual-results {
        height: 10;
    }

    #manual-playlists {
        height: 6;
        margin: 0 2;
    }

    #ingestion-detail, #ingestion-status {
        padding: 0 2;
        min-height: 1;
    }

    #ingestion-detail {
        color: $text-muted;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self.items: dict[Path, tuple[object, list[Candidate]]] = {}
        self.selected_path: Path | None = None
        self.manual_candidates: dict[str, Candidate] = {}
        self.manual_selected: Candidate | None = None

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Button("Back to tracks", id="ingestion-back"),
            Button("Refresh inbox", id="ingestion-refresh", variant="primary"),
            Button("Open best match", id="ingestion-open-match"),
            id="ingestion-actions",
        )
        yield DataTable(id="ingestion-table")
        yield Label("Select an inbox file to review its candidate matches.", id="ingestion-detail")
        yield Horizontal(
            *(Button(f"Ingest #{number}", id=f"ingest-{number}", disabled=True) for number in range(1, 6)),
            id="ingestion-candidates",
        )
        yield Horizontal(
            Label("Manual search:"),
            Input(placeholder="Search title or artist in your SoundCloud library", id="manual-search"),
            Button("Ingest manual selection", id="ingest-manual", disabled=True),
            id="manual-search-actions",
        )
        yield DataTable(id="manual-results")
        yield Label("No SoundCloud match? Assign the selected file to playlists:")
        yield SelectionList[str](id="manual-playlists", compact=True)
        yield Button("Ingest selected playlists", id="ingest-playlists", disabled=True)
        yield Label("", id="ingestion-status")

    def _tracks(self) -> pl.DataFrame:
        tracks = self.app.df
        if "urn" not in tracks.columns:
            return tracks.with_columns(
                (pl.lit("soundcloud:tracks:") + pl.col("id").cast(pl.Utf8)).alias("urn")
            )
        return tracks

    def _refresh_items(self) -> None:
        inbox = DEFAULT_ROOT / "_inbox"
        inbox.mkdir(parents=True, exist_ok=True)
        self.items = {}
        table = self.query_one("#ingestion-table", DataTable)
        table.clear(columns=True)
        for column, width in (("File", 36), ("Status", 12), ("Best match", 44), ("Score", 8)):
            table.add_column(column, width=width)

        for path in sorted(inbox.iterdir()):
            if not path.is_file() or path.suffix.lower() not in AUDIO_SUFFIXES:
                continue
            metadata = read_metadata(path)
            ranked = candidates(metadata, self._tracks())
            self.items[path] = (metadata, ranked)
            best = ranked[0] if ranked else None
            status = "Ready" if high_confidence(ranked) else "Review"
            match = "" if best is None else f"{best.row.get('artist', '')} - {best.row.get('title', '')}"
            score = "" if best is None else f"{best.score:.0%}"
            table.add_row(path.name, status, match, score, key=str(path))

        self.selected_path = None
        self.manual_selected = None
        self._refresh_manual_results("")
        self._refresh_playlist_choices()
        self._set_selected_details()
        self.query_one("#ingestion-status", Label).update(f"{len(self.items)} audio file(s) in inbox")

    def _set_selected_details(self) -> None:
        detail = self.query_one("#ingestion-detail", Label)
        ranked: list[Candidate] = []
        if self.selected_path is not None:
            metadata, ranked = self.items[self.selected_path]
            lines = [f"{self.selected_path.name}  |  detected: {metadata.artist} - {metadata.title}"]
            for number, candidate in enumerate(ranked, start=1):
                row = candidate.row
                lines.append(f"#{number} {candidate.score:.0%}: {row.get('artist', '')} - {row.get('title', '')}  {row.get('soundcloud_url', '')}")
            detail.update("\n".join(lines))
        else:
            detail.update("Select an inbox file to review its candidate matches.")
        for number in range(1, 6):
            self.query_one(f"#ingest-{number}", Button).disabled = number > len(ranked)
        self.query_one("#ingest-manual", Button).disabled = self.manual_selected is None
        self._update_playlist_ingest_button()

    def _refresh_playlist_choices(self) -> None:
        choices = self.query_one("#manual-playlists", SelectionList)
        choices.clear_options()
        playlists = {
            playlist
            for row in self._tracks().iter_rows(named=True)
            for playlist in playlist_names(row.get("playlists"))
        }
        choices.add_options((playlist, playlist) for playlist in sorted(playlists, key=str.casefold))

    def _update_playlist_ingest_button(self) -> None:
        choices = self.query_one("#manual-playlists", SelectionList)
        self.query_one("#ingest-playlists", Button).disabled = (
            self.selected_path is None or not choices.selected
        )

    def _refresh_manual_results(self, query: str) -> None:
        table = self.query_one("#manual-results", DataTable)
        table.clear(columns=True)
        for column, width in (("Artist", 28), ("Title", 48), ("Playlists", 32), ("Match", 8)):
            table.add_column(column, width=width)
        self.manual_candidates = {}
        self.manual_selected = None
        search = normalized(query)
        if len(search) < 2:
            self.query_one("#ingest-manual", Button).disabled = True
            return
        matches: list[Candidate] = []
        for row in self._tracks().iter_rows(named=True):
            haystack = normalized(f"{row.get('artist', '')} {row.get('title', '')}")
            score = fuzz.WRatio(search, haystack) / 100
            if search in haystack or score >= 0.55:
                matches.append(Candidate(row, score))
        for candidate in sorted(matches, key=lambda item: item.score, reverse=True)[:25]:
            row = candidate.row
            urn = str(row["urn"])
            self.manual_candidates[urn] = candidate
            playlists = ", ".join(str(name) for name in row.get("playlists", []))
            table.add_row(str(row.get("artist", "")), str(row.get("title", "")), playlists, f"{candidate.score:.0%}", key=urn)
        self.query_one("#ingest-manual", Button).disabled = True

    def on_mount(self) -> None:
        table = self.query_one("#ingestion-table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        manual_table = self.query_one("#manual-results", DataTable)
        manual_table.cursor_type = "row"
        manual_table.zebra_stripes = True
        self._refresh_items()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "ingestion-table":
            self.selected_path = Path(str(getattr(event.row_key, "value", event.row_key)))
            self._set_selected_details()
        elif event.data_table.id == "manual-results":
            urn = str(getattr(event.row_key, "value", event.row_key))
            self.manual_selected = self.manual_candidates.get(urn)
            self.query_one("#ingest-manual", Button).disabled = self.manual_selected is None

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "manual-search":
            self._refresh_manual_results(event.value)

    def on_selection_list_selected_changed(
        self, event: SelectionList.SelectedChanged[str]
    ) -> None:
        if event.selection_list.id == "manual-playlists":
            self._update_playlist_ingest_button()

    def _ingest(self, candidate: Candidate) -> None:
        if self.selected_path is None:
            self.query_one("#ingestion-status", Label).update("Select an inbox file first.")
            return
        try:
            initialise(DEFAULT_ROOT)
            materialize(self.selected_path, candidate, DEFAULT_ROOT, load_manifest(), dry_run=False)
        except OSError as error:
            self.query_one("#ingestion-status", Label).update(f"Ingestion failed: {error}")
            return
        self.app.df = pl.read_parquet(self.app.TRACKS_PATH)
        self._refresh_items()

    def _ingest_playlists(self) -> None:
        if self.selected_path is None:
            self.query_one("#ingestion-status", Label).update("Select an inbox file first.")
            return
        playlists = self.query_one("#manual-playlists", SelectionList).selected
        if not playlists:
            return
        try:
            initialise(DEFAULT_ROOT)
            materialize_unmatched(
                self.selected_path, playlists, DEFAULT_ROOT, load_manifest(), dry_run=False
            )
        except OSError as error:
            self.query_one("#ingestion-status", Label).update(f"Ingestion failed: {error}")
            return
        self._refresh_items()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "ingestion-back":
            self.app.pop_screen()
            self.app.call_after_refresh(self.app._reload_tracks_from_disk)
            return
        if button_id == "ingestion-refresh":
            self._refresh_items()
            return
        if button_id == "ingest-manual":
            if self.manual_selected is not None:
                self._ingest(self.manual_selected)
            return
        if button_id == "ingest-playlists":
            self._ingest_playlists()
            return
        if self.selected_path is None:
            return
        _, ranked = self.items[self.selected_path]
        if button_id == "ingestion-open-match" and ranked:
            url = ranked[0].row.get("soundcloud_url")
            if url:
                webbrowser.open(str(url))
            return
        if not button_id.startswith("ingest-"):
            return
        candidate_index = int(button_id.removeprefix("ingest-")) - 1
        if not 0 <= candidate_index < len(ranked):
            return
        self._ingest(ranked[candidate_index])
