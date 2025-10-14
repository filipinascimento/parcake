"""Example script for sorting one or many Parquet files."""

from __future__ import annotations

from pathlib import Path

from parcake import PieceSorter

if __name__ == "__main__":
    sources = [Path("./events_0.parquet"), Path("./events_1.parquet")]
    destination = Path("./sorted_events.parquet")

    sorter = PieceSorter(
        sources,
        columns=["type", ("created", True)],
        ascending=False,
    )

    sorter.sort(
        destination,
        compression="preserve",
        temp_directory=Path("./.duckdb-temp"),
        threads=4,
        memory_limit="4GB",
        progress_bar=True,
    )

    print(f"Sorted file written to {destination}")
