# Example of grouping and aggregating Parquet data with PieceGrouper

from pathlib import Path

from parcake import PieceGrouper


if __name__ == "__main__":
    sources = ["events_0.parquet", "events_1.parquet"]

    with PieceGrouper(
        sources,
        group_by=["country", "device_type"],
        max_chunk_size=250_000,
        keep_sorted=True,
        scratch_directory=Path(".parcake_scratch"),
    ) as grouper:
        # Stream over each group without loading the entire dataset into memory.
        for group_key, chunk_iter in grouper:
            for batch in chunk_iter:
                # batch is a pandas.DataFrame containing the subset for this group.
                # Replace the line below with domain specific processing.
                _ = batch.shape

        # Obtain the unique (country, device_type) combinations quickly.
        unique_groups = grouper.unique()
        print(f"Found {len(unique_groups)} groups")

        # Produce aggregate metrics similar to pandas.GroupBy.agg.
        summary = grouper.aggregate(
            {
                "revenue": ["sum", "max"],
                "event_id": {"event_count": "count"},
                "session_duration": {"avg_session_duration": "avg"},
            }
        )
        print(summary.head())

        # Reuse the pre-sorted Parquet file across runs when keep_sorted=True.
        if grouper.sorted_path is not None:
            print("Sorted output stored at:", grouper.sorted_path)
