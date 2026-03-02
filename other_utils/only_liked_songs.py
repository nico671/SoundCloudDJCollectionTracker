import json

import polars as pl

if __name__ == "__main__":
    df = pl.read_parquet("data/tracks.parquet")
    only_liked_songs = []

    for row in df.filter(
        (pl.col("playlists").list.contains("liked"))
        & (pl.col("playlists").list.len() == 1)
    ).iter_rows(named=True):
        only_liked_songs.append(row)
    with open("data/only_liked_songs.json", "w") as f:
        json.dump(only_liked_songs, f, indent=4)
