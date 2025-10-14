from __future__ import annotations

from pathlib import Path

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from parcake import PieceSorter

pytest.importorskip("duckdb")


def _write_parquet(path: Path, frame: pd.DataFrame, *, compression: str = "snappy") -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression=compression)


def _column_values(path: Path, column: str) -> list:
    return pq.read_table(path, columns=[column]).column(0).to_pylist()


def test_sorter_orders_rows(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "user": ["carol", "bob", "alice", "bob"],
            "score": [1, 5, 3, 8],
            "event": [3, 2, 1, 2],
        }
    )
    source = tmp_path / "events.parquet"
    _write_parquet(source, frame, compression="snappy")

    sorter = PieceSorter(source, columns=["user", ("score", False)])
    destination = tmp_path / "sorted.parquet"
    sorter.sort(
        destination,
        compression="none",
        progress_bar=True,
        memory_limit=16 * 1024 * 1024,
    )

    expected = frame.sort_values(["user", "score"], ascending=[True, False]).reset_index(drop=True)
    assert _column_values(destination, "user") == expected["user"].tolist()
    assert _column_values(destination, "score") == expected["score"].tolist()


def test_sorter_compression_preserve_with_glob(tmp_path) -> None:
    frame = pd.DataFrame({"value": [4, 1, 2, 3]})
    first = tmp_path / "part-0.parquet"
    second = tmp_path / "part-1.parquet"
    _write_parquet(first, frame.iloc[:2], compression="zstd")
    _write_parquet(second, frame.iloc[2:], compression="zstd")

    sorter = PieceSorter(tmp_path / "*.parquet", columns=["value"], ascending=True)
    destination = tmp_path / "out.parquet"
    sorter.sort(destination, compression="preserve", threads=1)

    metadata = pq.ParquetFile(destination).metadata
    assert metadata is not None
    codec = metadata.row_group(0).column(0).compression
    assert codec is not None
    assert codec.upper() == "ZSTD"

    values = _column_values(destination, "value")
    assert values == sorted(frame["value"].tolist())



def test_sorter_handles_multiple_source_files(tmp_path) -> None:
    frame_one = pd.DataFrame({"value": [5, 1], "bucket": ["a", "a"]})
    frame_two = pd.DataFrame({"value": [3, 2], "bucket": ["b", "b"]})
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_parquet(first, frame_one, compression="snappy")
    _write_parquet(second, frame_two, compression="snappy")

    sorter = PieceSorter([first, second], columns=["value"])
    destination = tmp_path / "merged.parquet"
    sorter.sort(destination, compression="none")

    values = _column_values(destination, "value")
    expected = sorted(frame_one["value"].tolist() + frame_two["value"].tolist())
    assert values == expected
