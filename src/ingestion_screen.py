from __future__ import annotations

import webbrowser
from pathlib import Path

import polars as pl
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Label

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

    def on_mount(self) -> None:
        table = self.query_one("#ingestion-table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        self._refresh_items()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "ingestion-table":
            return
        self.selected_path = Path(str(getattr(event.row_key, "value", event.row_key)))
        self._set_selected_details()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "ingestion-back":
            self.app.pop_screen()
            return
        if button_id == "ingestion-refresh":
            self._refresh_items()
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
        try:
            initialise(DEFAULT_ROOT)
            materialize(self.selected_path, ranked[candidate_index], DEFAULT_ROOT, load_manifest(), dry_run=False)
        except OSError as error:
            self.query_one("#ingestion-status", Label).update(f"Ingestion failed: {error}")
            return
        self._refresh_items()
