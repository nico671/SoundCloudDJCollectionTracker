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
    with open("data/playlist_counts.md", "w") as f:
        f.write("# Playlist Counts\n\n")
    for playlist, count in playlist_counts.items():
        if count > 70:
            with open("data/playlist_counts.md", "a") as f:
                f.write(f"- {playlist}: {count}\n")
