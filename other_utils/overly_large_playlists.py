import polars as pl

if __name__ == "__main__":
    df = pl.read_parquet("data/tracks.parquet")
    playlist_counts = {}
    for row in df.iter_rows(named=True):
        for playlist in row["playlists"]:
            if playlist == "liked" or playlist == "liverpool":
                continue
            if playlist not in playlist_counts:
                playlist_counts[playlist] = 0
            playlist_counts[playlist] += 1
    for playlist, count in playlist_counts.items():
        if count > 70:
            print(f"{playlist}: {count}")
