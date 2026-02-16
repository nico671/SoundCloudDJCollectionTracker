import base64
import hashlib
import os
import urllib
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import polars as pl
import requests
from dotenv import load_dotenv

OUTPUT_FILE = "data/tracks.parquet"


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


def get_random_state():
    return os.urandom(16).hex()


def has_non_default_purchase_url(purchase_url):
    return purchase_url is not None and str(purchase_url).strip() != ""


def is_track_processed(price, purchase_url):
    return price is not None and has_non_default_purchase_url(purchase_url)


def add_track(track, all_tracks, old_tracks, playlist_name=None):
    if track["kind"] != "track":
        return
    track_id = track["id"]
    track_purchase_url = track.get("purchase_url")
    track_title = track.get("title")
    soundcloud_url = track.get("permalink_url")
    artist_name = track.get("user", {}).get("username")
    track_genre = track.get("genre")
    if track_id in old_tracks:
        track_purchased = old_tracks[track_id]["purchased"]
        track_price = old_tracks[track_id]["price"]
        track_purchase_url = old_tracks[track_id]["purchase_url"]
    else:
        track_purchased = False
        track_price = None
    track_processed = is_track_processed(track_price, track_purchase_url)
    if track_id in all_tracks:
        if playlist_name is None:
            if "liked" not in all_tracks[track_id]["playlists"]:
                all_tracks[track_id]["playlists"] += ", liked"
        elif playlist_name not in all_tracks[track_id]["playlists"]:
            all_tracks[track_id]["playlists"] += f", {playlist_name}"
        return
    all_tracks[track_id] = {
        "title": track_title,
        "id": track_id,
        "purchase_url": track_purchase_url,
        "purchased": track_purchased,
        "price": track_price,
        "processed": track_processed,
        "soundcloud_url": soundcloud_url,
        "playlists": "liked",
        "artist": artist_name,
        "genre": track_genre,
    }


def create_new_df(user_id, headers):
    old_tracks = {}
    # get old dataframe, if exists
    old_len = 0
    if os.path.exists(OUTPUT_FILE):
        old_df = pl.read_parquet(OUTPUT_FILE)
        for row in old_df.iter_rows(named=True):
            old_len += 1
            track_purchase_url = row["purchase_url"]
            track_id = row["id"]
            track_price = row["price"]
            track_downloaded = row["purchased"]
            # if all tracked fields are defaults, we don't need to store them
            if (
                track_price is None
                and track_downloaded is False
                and not has_non_default_purchase_url(track_purchase_url)
            ):
                continue
            old_tracks[track_id] = {
                "price": track_price,
                "purchased": track_downloaded,
                "purchase_url": track_purchase_url,
            }
    print(f"Loaded {old_len} tracks from existing dataframe.")

    all_tracks = {}
    # process liked tracks
    liked_tracks_url = f"https://api.soundcloud.com/users/{user_id}/likes/tracks"
    liked_tracks = requests.get(
        liked_tracks_url,
        headers=headers,
        params={"linked_partitioning": True, "limit": 1000},
    ).json()
    next_href = liked_tracks.get("next_href")
    print("Processing liked tracks...")
    while next_href:
        print("Current href:", next_href)
        for track in liked_tracks["collection"]:
            add_track(track, all_tracks, old_tracks, playlist_name=None)
        liked_tracks = requests.get(
            next_href,
            headers=headers,
            params={"linked_partitioning": True, "limit": 1000},
        ).json()
        next_href = liked_tracks.get("next_href")
    # process playlists
    playlists_url = f"https://api.soundcloud.com/users/{user_id}/playlists"
    playlists = requests.get(
        playlists_url,
        headers=headers,
        params={"linked_partitioning": True, "limit": 1000},
    ).json()
    next_href = playlists.get("next_href")
    print("Processing playlists...")
    while next_href:
        print("Current href:", next_href)
        for playlist in playlists["collection"]:
            playlist_name = playlist["title"]
            for track in playlist["tracks"]:
                add_track(track, all_tracks, old_tracks, playlist_name=playlist_name)
        playlists = requests.get(
            next_href,
            headers=headers,
            params={"linked_partitioning": True, "limit": 1000},
        ).json()
        next_href = playlists.get("next_href")
    new_len = len(all_tracks)
    print(f"Added {new_len - old_len} new tracks, total is now {new_len} tracks.")
    df = pl.DataFrame(list(all_tracks.values()))
    df.write_parquet(OUTPUT_FILE)
    return


if __name__ == "__main__":
    code_verifier, code_challenge = generate_pkce_pair()

    client_id, client_secret = get_client_id_secret()
    random_state = get_random_state()
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
    me = requests.get("https://api.soundcloud.com/me", headers=headers).json()
    user_id = me["id"]
    if not os.path.exists(OUTPUT_FILE.split("/")[0]):
        os.makedirs(OUTPUT_FILE.split("/")[0])
    create_new_df(user_id, headers)
    print(f"Done! Data saved to {OUTPUT_FILE}")
