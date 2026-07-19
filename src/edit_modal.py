from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Input,
    Label,
    Static,
    Switch,
)


class EditTrackModal(
    ModalScreen[tuple[bool, float | None, str | None, bool, str | None] | None]
):
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
        do_not_download: bool,
        notes: str | None,
        track_title: str | None = None,
    ) -> None:
        super().__init__()
        self.track_id = track_id
        self.track_title = track_title or f"Track {track_id}"
        self.initial_purchased = purchased
        self.initial_price = price
        self.initial_download_url = download_url
        self.initial_do_not_download = do_not_download
        self.initial_notes = notes

    def compose(self) -> ComposeResult:
        initial_price = "" if self.initial_price is None else str(self.initial_price)
        initial_download_url = (
            "" if self.initial_download_url is None else self.initial_download_url
        )
        initial_notes = "" if self.initial_notes is None else self.initial_notes
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
            Horizontal(
                Label("Skip DJ", classes="edit-label"),
                Switch(value=self.initial_do_not_download, id="edit-do-not-download"),
                classes="edit-row",
            ),
            Horizontal(
                Label("Notes", classes="edit-label"),
                Input(
                    value=initial_notes,
                    placeholder="Optional notes",
                    id="edit-notes",
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
        do_not_download = self.query_one("#edit-do-not-download", Switch).value
        notes_text = self.query_one("#edit-notes", Input).value.strip()
        download_url = download_url_text if download_url_text else None
        notes = notes_text if notes_text else None
        if price_text == "":
            self.dismiss((purchased, None, download_url, do_not_download, notes))
            return

        try:
            parsed_price = float(price_text)
        except ValueError:
            self.query_one("#edit-error", Static).update(
                "Price must be a valid number."
            )
            self.app.bell()
            return

        self.dismiss((purchased, parsed_price, download_url, do_not_download, notes))
