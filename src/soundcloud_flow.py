import base64
import hashlib
import os
import urllib
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv

OUTPUT_FILE = "data/tracks.parquet"
LIKED_SOURCE = "liked"


def generate_pkce_pair():
    # Generate code_verifier
    code_verifier = base64.urlsafe_b64encode(os.urandom(40)).decode("utf-8").rstrip("=")

    # Generate code_challenge
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode("utf-8")).digest())
        .decode("utf-8")
        .rstrip("=")
    )

    return code_verifier, code_challenge


def get_client_id_secret():
    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("CLIENT_ID and CLIENT_SECRET must be set in the .env file")
    return client_id, client_secret


def has_non_default_purchase_url(purchase_url):
    return purchase_url is not None and str(purchase_url).strip() != ""


def is_track_processed(price, purchase_url):
    return price is not None and has_non_default_purchase_url(purchase_url)


def track_urn(track: dict[str, Any]) -> str:
    """Return the current SoundCloud identifier, with legacy API fallback."""

    urn = track.get("urn")
    if urn:
        return str(urn)
    return f"soundcloud:tracks:{track['id']}"


def build_track_record(track: dict[str, Any], old_tracks: dict[str, dict[str, Any]]):
    track_id = track.get("id")
    urn = track_urn(track)
    track_purchase_url = track.get("purchase_url")
    track_title = track.get("title")
    soundcloud_url = track.get("permalink_url")
    artist_name = track.get("user", {}).get("username")
    track_genre = track.get("genre")

    persisted_state = old_tracks.get(urn)
    if persisted_state:
        track_purchased = persisted_state["purchased"]
        track_price = persisted_state["price"]
        track_purchase_url = persisted_state["purchase_url"]
    else:
        track_purchased = False
        track_price = None

    return {
        "title": track_title,
        "id": track_id,
        "urn": urn,
        "purchase_url": track_purchase_url,
        "purchased": track_purchased,
        "price": track_price,
        "processed": is_track_processed(track_price, track_purchase_url),
        "soundcloud_url": soundcloud_url,
        "playlist_sources": set(),
        "artist": artist_name,
        "genre": track_genre,
        "isrc": track.get("isrc"),
        "duration_ms": track.get("duration"),
        "bpm": track.get("bpm"),
        "key_signature": track.get("key_signature"),
        "tag_list": track.get("tag_list"),
        "label_name": track.get("label_name"),
        "release_year": track.get("release_year"),
        "release_month": track.get("release_month"),
        "release_day": track.get("release_day"),
        "access": track.get("access"),
        "downloadable": track.get("downloadable"),
        "download_url": track.get("download_url"),
        "playback_count": track.get("playback_count"),
        "favoritings_count": track.get("favoritings_count"),
        "reposts_count": track.get("reposts_count"),
        "comment_count": track.get("comment_count"),
        "download_count": track.get("download_count"),
    }


def add_track(
    track: dict[str, Any],
    all_tracks: dict[str, dict[str, Any]],
    old_tracks: dict[str, dict[str, Any]],
    source_name: str,
):
    if track.get("kind") != "track":
        return

    urn = track_urn(track)
    if urn not in all_tracks:
        all_tracks[urn] = build_track_record(track, old_tracks)

    all_tracks[urn]["playlist_sources"].add(source_name)


def fetch_paginated_collection(url, headers, limit):
    payload = requests.get(
        url,
        headers=headers,
        params={"linked_partitioning": True, "limit": limit},
    ).json()

    while True:
        yield payload.get("collection", [])
        next_href = payload.get("next_href")
        if not next_href:
            break
        payload = requests.get(
            next_href,
            headers=headers,
            params={"linked_partitioning": True, "limit": limit},
        ).json()


def create_new_df(headers):
    old_tracks = {}
    # get old dataframe, if exists
    old_len = 0
    if os.path.exists(OUTPUT_FILE):
        old_df = pl.read_parquet(OUTPUT_FILE)
        for row in old_df.iter_rows(named=True):
            old_len += 1
            track_purchase_url = row["purchase_url"]
            urn = str(row.get("urn") or f"soundcloud:tracks:{row['id']}")
            track_price = row["price"]
            track_downloaded = row["purchased"]
            # if all tracked fields are defaults, we don't need to store them
            if (
                track_price is None
                and track_downloaded is False
                and not has_non_default_purchase_url(track_purchase_url)
            ):
                continue
            old_tracks[urn] = {
                "price": track_price,
                "purchased": track_downloaded,
                "purchase_url": track_purchase_url,
            }
    print(f"Loaded {old_len} tracks from existing dataframe.")

    all_tracks = {}
    # process liked tracks
    liked_tracks_url = "https://api.soundcloud.com/me/likes/tracks"
    print("Processing liked tracks...")
    for track_page in fetch_paginated_collection(liked_tracks_url, headers, limit=1000):
        for track in track_page:
            add_track(track, all_tracks, old_tracks, source_name=LIKED_SOURCE)

    # process playlists
    playlists_url = "https://api.soundcloud.com/me/playlists"
    print("Processing playlists...")
    for playlist_page in fetch_paginated_collection(playlists_url, headers, limit=100):
        for playlist in playlist_page:
            playlist_name = playlist["title"]
            for track in playlist["tracks"]:
                add_track(
                    track,
                    all_tracks,
                    old_tracks,
                    source_name=playlist_name,
                )

    for track in all_tracks.values():
        track["playlists"] = sorted(track["playlist_sources"])
        del track["playlist_sources"]

    new_len = len(all_tracks)
    print(f"Added {new_len - old_len} new tracks, total is now {new_len} tracks.")
    # print(list(all_tracks.values())[:5])
    df = pl.DataFrame(list(all_tracks.values()), infer_schema_length=None)
    df.write_parquet(OUTPUT_FILE)

    return


if __name__ == "__main__":
    code_verifier, code_challenge = generate_pkce_pair()

    client_id, client_secret = get_client_id_secret()
    random_state = os.urandom(16).hex()
    redirect_uri = "http://localhost:8000/callback"

    auth_params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": random_state,
    }

    auth_url = "https://secure.soundcloud.com/authorize?" + urllib.parse.urlencode(
        auth_params
    )

    webbrowser.open(auth_url)
    print(f"Go to this URL if browser doesn’t open: {auth_url}")

    auth_code = None

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            global auth_code
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            auth_code = params.get("code", [None])[0]

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authorization successful! You can close this window.")

    # Start server (must match your redirect URI port)
    httpd = HTTPServer(("localhost", 8000), CallbackHandler)
    print(httpd.server_name, httpd.server_port)
    print("Listening at http://localhost:8000/callback ...")
    httpd.handle_request()  # will exit after first request
    httpd.server_close()

    token_url = "https://secure.soundcloud.com/oauth/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,  # needed in this step
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
        "code": auth_code,
    }
    headers = {
        "accept": "application/json; charset=utf-8",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.post(token_url, data=data, headers=headers)
    tokens = response.json()

    headers = {"Authorization": f"OAuth {tokens['access_token']}"}
    if not os.path.exists(OUTPUT_FILE.split("/")[0]):
        os.makedirs(OUTPUT_FILE.split("/")[0])
    create_new_df(headers)
    print(f"Done! Data saved to {OUTPUT_FILE}")
