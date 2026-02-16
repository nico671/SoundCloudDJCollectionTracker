import subprocess
import sys
import webbrowser
from pathlib import Path

import polars as pl
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Header,
    Input,
    Label,
    Select,
    Static,
    Switch,
)


class EditTrackModal(ModalScreen[tuple[bool, float | None, str | None] | None]):
    CSS = """
    #edit-dialog {
        width: 60;
        height: auto;
        padding: 1 2;
        background: $surface;
        border: round $primary;
    }

    #edit-title {
        margin-bottom: 1;
        text-style: bold;
    }

    .edit-row {
        height: 3;
        align: left middle;
    }

    .edit-label {
        width: 12;
        color: $text-muted;
    }

    #edit-price {
        width: 1fr;
    }

    #edit-buttons {
        height: 3;
        align: right middle;
        margin-top: 1;
    }

    #edit-error {
        color: $error;
        margin-top: 1;
        min-height: 1;
    }
    """

    def __init__(
        self,
        track_id: str,
        purchased: bool,
        price: float | None,
        download_url: str | None,
        track_title: str | None = None,
    ) -> None:
        super().__init__()
        self.track_id = track_id
        self.track_title = track_title or f"Track {track_id}"
        self.initial_purchased = purchased
        self.initial_price = price
        self.initial_download_url = download_url

    def compose(self) -> ComposeResult:
        initial_price = "" if self.initial_price is None else str(self.initial_price)
        initial_download_url = (
            "" if self.initial_download_url is None else self.initial_download_url
        )
        yield Vertical(
            Static(f"Edit track: {self.track_title}", id="edit-title"),
            Horizontal(
                Label("Purchased", classes="edit-label"),
                Switch(value=self.initial_purchased, id="edit-purchased"),
                classes="edit-row",
            ),
            Horizontal(
                Label("Price", classes="edit-label"),
                Input(
                    value=initial_price,
                    placeholder="Leave blank for no price",
                    id="edit-price",
                ),
                classes="edit-row",
            ),
            Horizontal(
                Label("Download URL", classes="edit-label"),
                Input(
                    value=initial_download_url,
                    placeholder="Paste download/purchase URL",
                    id="edit-download-url",
                ),
                classes="edit-row",
            ),
            Static("", id="edit-error"),
            Horizontal(
                Button("Cancel", id="edit-cancel"),
                Button("Save", variant="primary", id="edit-save"),
                id="edit-buttons",
            ),
            id="edit-dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "edit-cancel":
            self.dismiss(None)
            return

        if event.button.id != "edit-save":
            return

        purchased = self.query_one("#edit-purchased", Switch).value
        price_text = self.query_one("#edit-price", Input).value.strip()
        download_url_text = self.query_one("#edit-download-url", Input).value.strip()
        download_url = download_url_text if download_url_text else None
        if price_text == "":
            self.dismiss((purchased, None, download_url))
            return

        try:
            parsed_price = float(price_text)
        except ValueError:
            self.query_one("#edit-error", Static).update(
                "Price must be a valid number."
            )
            self.app.bell()
            return

        self.dismiss((purchased, parsed_price, download_url))


class DJApp(App[None]):
    ALL_PLAYLISTS = "__all__"
    ALL_PURCHASED = "__all_purchased__"
    PURCHASED_TRUE = "true"
    PURCHASED_FALSE = "false"
    ALL_PROCESSED = "__all_processed__"
    PROCESSED_TRUE = "processed_true"
    PROCESSED_FALSE = "processed_false"
    URL_COLUMNS = {"purchase_url", "soundcloud_url", "url", "permalink_url"}
    TRACKS_PATH = Path("data/tracks.parquet")
    FLOW_SCRIPT_PATH = Path(__file__).with_name("soundcloud_flow.py")
    COLUMN_WIDTH = 24
    CSS = """
    #filter-bar {
        height: 3;
        padding: 0 2;
        align: left middle;
        background: $surface;
        border-bottom: tall $primary 30%;
    }

    #playlist-label {
        margin-right: 1;
        color: $text-muted;
        text-style: bold;
    }

    #playlist-filter {
        width: 40;
        min-width: 24;
        margin-right: 2;
    }

    #purchased-label {
        margin-right: 1;
        color: $text-muted;
        text-style: bold;
    }

    #purchased-filter {
        width: 20;
        min-width: 16;
        margin-right: 2;
    }

    #processed-label {
        margin-right: 1;
        color: $text-muted;
        text-style: bold;
    }

    #processed-filter {
        width: 20;
        min-width: 16;
        margin-right: 2;
    }

    #search-label {
        margin-right: 1;
        color: $text-muted;
        text-style: bold;
    }

    #track-search {
        width: 28;
        min-width: 20;
        margin-right: 2;
    }

    #result-count {
        width: 1fr;
        content-align: right middle;
        color: $text-muted;
    }

    #refresh-soundcloud {
        margin-right: 2;
    }

    #stats-bar {
        height: 3;
        padding: 0 2;
        align: left middle;
        background: $surface-darken-1;
        border-bottom: tall $primary 15%;
    }

    #downloaded-value {
        width: 1fr;
        color: $success;
        text-style: bold;
    }

    #refresh-status {
        width: 1fr;
        content-align: center middle;
        color: $text-muted;
    }

    #remaining-value {
        width: 1fr;
        content-align: right middle;
        color: $warning;
        text-style: bold;
    }

    #tracks-table {
        height: 1fr;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self.df = pl.read_parquet("data/tracks.parquet")
        self._ensure_processed_column(persist_if_missing=True)
        self.visible_columns = [column for column in self.df.columns if column != "id"]
        self.column_widths = self._build_column_widths()
        self.current_playlist = self.ALL_PLAYLISTS
        self.current_purchased = self.ALL_PURCHASED
        self.current_processed = self.ALL_PROCESSED
        self.track_name_query = ""

    def _ensure_processed_column(self, *, persist_if_missing: bool = False) -> None:
        """Ensure a boolean `processed` column exists and is correctly derived.

        Rule: a track is processed only when both price and purchase_url are
        present (non-default).
        """

        had_processed = "processed" in self.df.columns

        price_expr: pl.Expr
        if "price" in self.df.columns:
            price_expr = pl.col("price")
        else:
            price_expr = pl.lit(None)

        purchase_url_expr: pl.Expr
        if "purchase_url" in self.df.columns:
            purchase_url_expr = (
                pl.col("purchase_url")
                .cast(pl.Utf8)
                .fill_null("")
                .str.strip_chars()
                .str.len_chars()
                > 0
            )
        else:
            purchase_url_expr = pl.lit(False, dtype=pl.Boolean)

        processed_expr = (price_expr.is_not_null() & purchase_url_expr).cast(pl.Boolean)

        if had_processed:
            # Always re-derive to guarantee the rule holds (and to backfill nulls).
            self.df = self.df.with_columns(processed_expr.alias("processed"))
        else:
            self.df = self.df.with_columns(processed_expr.alias("processed"))

        if persist_if_missing and not had_processed:
            # Persist the new column so subsequent loads (and other tools) see it.
            self.df.write_parquet(self.TRACKS_PATH)

    def _build_column_widths(self) -> dict[str, int]:
        widths = {column: self.COLUMN_WIDTH for column in self.visible_columns}

        # Keep title fully visible by sizing to the longest track title.
        title_max_len = max(
            [len("title")]
            + [
                len(str(value))
                for value in self.df.get_column("title").to_list()
                if value is not None
            ]
        )
        widths["title"] = title_max_len + 2

        # Compact columns for boolean / small numeric values.
        widths["purchased"] = 10
        widths["price"] = 8
        if "processed" in widths:
            widths["processed"] = 11
        return widths

    def _get_playlist_options(self) -> list[tuple[str, str]]:
        playlists: set[str] = set()
        for row in self.df.iter_rows(named=True):
            row_playlists = row.get("playlists")
            if row_playlists is None:
                continue
            for name in str(row_playlists).split(","):
                normalized = name.strip()
                if normalized and normalized.lower() != "liked":
                    playlists.add(normalized)

        options = [("All playlists", self.ALL_PLAYLISTS)]
        options.extend((name, name) for name in sorted(playlists, key=str.lower))
        return options

    @staticmethod
    def _in_playlist(playlists_value: object, selected_playlist: str) -> bool:
        if selected_playlist == DJApp.ALL_PLAYLISTS:
            return True
        if playlists_value is None:
            return False
        return selected_playlist in {
            name.strip() for name in str(playlists_value).split(",") if name.strip()
        }

    @staticmethod
    def _matches_purchased(purchased_value: object, selected_purchased: str) -> bool:
        if selected_purchased == DJApp.ALL_PURCHASED:
            return True

        is_purchased = bool(purchased_value)
        if selected_purchased == DJApp.PURCHASED_TRUE:
            return is_purchased
        if selected_purchased == DJApp.PURCHASED_FALSE:
            return not is_purchased
        return True

    @staticmethod
    def _matches_processed(processed_value: object, selected_processed: str) -> bool:
        if selected_processed == DJApp.ALL_PROCESSED:
            return True

        is_processed = bool(processed_value)
        if selected_processed == DJApp.PROCESSED_TRUE:
            return is_processed
        if selected_processed == DJApp.PROCESSED_FALSE:
            return not is_processed
        return True

    @staticmethod
    def _matches_track_name(track_title: object, query: str) -> bool:
        normalized_query = query.strip().lower()
        if not normalized_query:
            return True
        if track_title is None:
            return False
        return normalized_query in str(track_title).lower()

    @staticmethod
    def _truncate(value: object, width: int) -> str:
        text = "" if value is None else str(value)
        if width <= 1:
            return text[:width]
        return text if len(text) <= width else text[: width - 1] + "…"

    def _display_cell(self, column: str, value: object) -> str:
        text = "" if value is None else str(value)
        if column == "title":
            return text
        return self._truncate(text, self.column_widths.get(column, self.COLUMN_WIDTH))

    def _get_track_row(self, track_id: str) -> dict[str, object] | None:
        matching = self.df.filter(pl.col("id").cast(pl.Utf8) == track_id)
        if matching.height == 0:
            return None
        return matching.row(0, named=True)

    def _selected_column_name(self, table: DataTable) -> str | None:
        column_index = table.cursor_column
        if column_index is None:
            return None
        if not 0 <= column_index < len(self.visible_columns):
            return None
        return self.visible_columns[column_index]

    def compose(self) -> ComposeResult:
        yield Header(name="SoundCloud DJ Track Manager")
        yield Horizontal(
            Label("Playlist:", id="playlist-label"),
            Select(
                options=self._get_playlist_options(),
                value=self.ALL_PLAYLISTS,
                allow_blank=False,
                id="playlist-filter",
            ),
            Label("Purchased:", id="purchased-label"),
            Select(
                options=[
                    ("All", self.ALL_PURCHASED),
                    ("Purchased", self.PURCHASED_TRUE),
                    ("Not purchased", self.PURCHASED_FALSE),
                ],
                value=self.ALL_PURCHASED,
                allow_blank=False,
                id="purchased-filter",
            ),
            Label("Processed:", id="processed-label"),
            Select(
                options=[
                    ("All", self.ALL_PROCESSED),
                    ("Processed", self.PROCESSED_TRUE),
                    ("Unprocessed", self.PROCESSED_FALSE),
                ],
                value=self.ALL_PROCESSED,
                allow_blank=False,
                id="processed-filter",
            ),
            Label("Track:", id="search-label"),
            Input(placeholder="Search title...", id="track-search"),
            Button("Refresh SoundCloud", id="refresh-soundcloud", variant="primary"),
            Label("", id="result-count"),
            id="filter-bar",
        )
        yield Horizontal(
            Label("", id="downloaded-value"),
            Label("", id="refresh-status"),
            Label("", id="remaining-value"),
            id="stats-bar",
        )
        yield DataTable(id="tracks-table")

    def _set_refresh_status(self, message: str) -> None:
        self.query_one("#refresh-status", Label).update(message)

    def _reload_tracks_from_disk(self) -> None:
        self.df = pl.read_parquet(self.TRACKS_PATH)
        self._ensure_processed_column(persist_if_missing=True)
        self.visible_columns = [column for column in self.df.columns if column != "id"]
        self.column_widths = self._build_column_widths()

        table = self.query_one("#tracks-table", DataTable)
        table.clear(columns=True)
        for column in self.visible_columns:
            table.add_column(
                column, width=self.column_widths.get(column, self.COLUMN_WIDTH)
            )

        playlist_select = self.query_one("#playlist-filter", Select)
        playlist_options = self._get_playlist_options()
        playlist_option_values = {value for _, value in playlist_options}
        if hasattr(playlist_select, "set_options"):
            playlist_select.set_options(playlist_options)
        if self.current_playlist not in playlist_option_values:
            self.current_playlist = self.ALL_PLAYLISTS
            playlist_select.value = self.ALL_PLAYLISTS

        self._populate_table()
        self._update_value_counters()

    @work(thread=True, exclusive=True)
    def _refresh_soundcloud_data(self) -> None:
        self.call_from_thread(
            self._set_refresh_status,
            "Refreshing…",
        )

        result = subprocess.run(
            [sys.executable, str(self.FLOW_SCRIPT_PATH)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent),
        )

        if result.returncode != 0:
            stderr_tail = (
                result.stderr.strip().splitlines()[-1]
                if result.stderr.strip()
                else "Unknown error"
            )
            self.call_from_thread(
                self._set_refresh_status,
                f"Refresh failed: {stderr_tail}",
            )
            return

        self.call_from_thread(self._reload_tracks_from_disk)
        self.call_from_thread(self._set_refresh_status, "Refresh complete")

    def _update_value_counters(self) -> None:
        downloaded_label = self.query_one("#downloaded-value", Label)
        remaining_label = self.query_one("#remaining-value", Label)

        downloaded_total = 0.0
        remaining_total = 0.0
        for row in self.df.iter_rows(named=True):
            price_raw = row.get("price")
            price = 0.0 if price_raw is None else float(price_raw)
            if bool(row.get("purchased")):
                downloaded_total += price
            else:
                remaining_total += price

        downloaded_label.update(f"Downloaded value: ${downloaded_total:,.2f}")
        remaining_label.update(f"Left to spend: ${remaining_total:,.2f}")

    @staticmethod
    def _row_key_to_track_id(row_key: object) -> str:
        return str(getattr(row_key, "value", row_key))

    def _open_editor_for_track(self, track_id: str) -> None:
        row = self._get_track_row(track_id)
        if row is None:
            return
        purchased = bool(row.get("purchased"))
        price_value = row.get("price")
        price = None if price_value is None else float(price_value)
        download_url_value = row.get("purchase_url")
        download_url = None if download_url_value is None else str(download_url_value)
        track_title_value = row.get("title")
        track_title = None if track_title_value is None else str(track_title_value)
        self.push_screen(
            EditTrackModal(
                track_id=track_id,
                purchased=purchased,
                price=price,
                download_url=download_url,
                track_title=track_title,
            ),
            lambda result, selected_track_id=track_id: self._apply_track_edits(
                selected_track_id, result
            ),
        )

    def _apply_track_edits(
        self, track_id: str, result: tuple[bool, float | None, str | None] | None
    ) -> None:
        if result is None:
            return

        purchased, price, download_url = result
        price_dtype = self.df.schema.get("price", pl.Float64)
        if price_dtype == pl.Null:
            price_dtype = pl.Float64
            self.df = self.df.with_columns(
                pl.col("price").cast(price_dtype).alias("price")
            )

        if "purchase_url" not in self.df.columns:
            self.df = self.df.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("purchase_url")
            )

        if "processed" not in self.df.columns:
            self.df = self.df.with_columns(
                pl.lit(False, dtype=pl.Boolean).alias("processed")
            )

        purchase_url_dtype = self.df.schema.get("purchase_url", pl.Utf8)
        if purchase_url_dtype == pl.Null:
            purchase_url_dtype = pl.Utf8
            self.df = self.df.with_columns(
                pl.col("purchase_url").cast(purchase_url_dtype).alias("purchase_url")
            )

        price_value = (
            pl.lit(None, dtype=price_dtype)
            if price is None
            else pl.lit(price, dtype=price_dtype)
        )
        purchase_url_value = pl.lit(download_url, dtype=purchase_url_dtype)
        processed_value = pl.lit(
            (price is not None) and (download_url is not None),
            dtype=pl.Boolean,
        )

        self.df = self.df.with_columns(
            pl.when(pl.col("id").cast(pl.Utf8) == track_id)
            .then(pl.lit(purchased, dtype=pl.Boolean))
            .otherwise(pl.col("purchased"))
            .alias("purchased"),
            pl.when(pl.col("id").cast(pl.Utf8) == track_id)
            .then(price_value)
            .otherwise(pl.col("price"))
            .alias("price"),
            pl.when(pl.col("id").cast(pl.Utf8) == track_id)
            .then(purchase_url_value)
            .otherwise(pl.col("purchase_url"))
            .alias("purchase_url"),
            pl.when(pl.col("id").cast(pl.Utf8) == track_id)
            .then(processed_value)
            .otherwise(pl.col("processed").fill_null(False).cast(pl.Boolean))
            .alias("processed"),
        )

        self.df.write_parquet(self.TRACKS_PATH)
        self._populate_table()
        self._update_value_counters()

    def _populate_table(self) -> None:
        table = self.query_one("#tracks-table", DataTable)
        count_label = self.query_one("#result-count", Label)
        table.clear(columns=False)

        matched_count = 0

        for row in self.df.iter_rows(named=True):
            if not self._in_playlist(row.get("playlists"), self.current_playlist):
                continue
            if not self._matches_purchased(
                row.get("purchased"), self.current_purchased
            ):
                continue
            if not self._matches_processed(
                row.get("processed"), self.current_processed
            ):
                continue
            if not self._matches_track_name(row.get("title"), self.track_name_query):
                continue
            matched_count += 1
            table.add_row(
                *[
                    self._display_cell(column, row[column])
                    for column in self.visible_columns
                ],
                key=str(row["id"]),
            )

        count_label.update(f"Showing {matched_count} tracks")

    def on_mount(self) -> None:
        table = self.query_one("#tracks-table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        for column in self.visible_columns:
            table.add_column(
                column, width=self.column_widths.get(column, self.COLUMN_WIDTH)
            )
        self._populate_table()
        self._update_value_counters()
        # Automatically sync from SoundCloud when the app first loads.
        self._refresh_soundcloud_data()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "tracks-table":
            return

        track_id = self._row_key_to_track_id(event.row_key)
        selected_column = self._selected_column_name(event.data_table)

        if selected_column in self.URL_COLUMNS:
            row = self._get_track_row(track_id)
            url_value = None if row is None else row.get(selected_column)
            if url_value:
                webbrowser.open(str(url_value))
            return

        self._open_editor_for_track(track_id)

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "playlist-filter":
            self.current_playlist = str(event.value)
        elif event.select.id == "purchased-filter":
            self.current_purchased = str(event.value)
        elif event.select.id == "processed-filter":
            self.current_processed = str(event.value)
        else:
            return
        self._populate_table()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "track-search":
            return
        self.track_name_query = event.value
        self._populate_table()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "refresh-soundcloud":
            return
        self._refresh_soundcloud_data()


if __name__ == "__main__":
    app = DJApp()
    app.run()
