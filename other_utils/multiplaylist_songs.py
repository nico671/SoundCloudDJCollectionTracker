import json

import polars as pl

if __name__ == "__main__":
    df = pl.read_parquet("data/tracks.parquet")
    multi_playlist_songs = []
    for row in (
        df.with_columns(pl.col("playlists").list.len().alias("playlist_count"))
        .with_columns(
            pl.when(pl.col("playlists").list.contains("liked"))
            .then(pl.col("playlist_count") - 1)
            .otherwise(pl.col("playlist_count"))
            .alias("playlist_count_excluding_liked")
        )
        .with_columns(
            pl.when(pl.col("playlists").list.contains("liverpool"))
            .then(pl.col("playlist_count_excluding_liked") - 1)
            .otherwise(pl.col("playlist_count_excluding_liked"))
            .alias("playlist_count_excluding_liverpool")
        )
        .filter(pl.col("playlist_count_excluding_liverpool") > 1)
        .drop("playlist_count")
        .iter_rows(named=True)
    ):
        multi_playlist_songs.append(row)

    with open("data/multi_playlist_songs.json", "w") as f:
        json.dump(multi_playlist_songs, f, indent=4)
