from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parcake import PieceGrouper

pytest.importorskip("duckdb")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path)


def test_grouper_streams_group_chunks(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "category": ["b", "a", "a", "a", "b"],
            "value": [10, 1, 2, 3, 4],
            "note": ["z", "u", "v", "w", "y"],
        }
    )
    path = tmp_path / "events.parquet"
    _write_parquet(path, frame)

    with PieceGrouper(path, group_by="category", max_chunk_size=2, columns=["value", "note"]) as grouper:
        chunk_lengths: dict[str, list[int]] = {}
        for key, chunk_iter in grouper:
            chunks = list(chunk_iter)
            chunk_lengths[key] = [len(chunk) for chunk in chunks]
            for chunk in chunks:
                assert (chunk["category"] == key).all()

        assert list(chunk_lengths.keys()) == ["a", "b"]
        assert sum(chunk_lengths["a"]) == 3
        assert len(chunk_lengths["a"]) >= 2
        assert sum(chunk_lengths["b"]) == 2

        grouped = dict(grouper.all())
        assert set(grouped) == {"a", "b"}
        assert grouped["a"].shape == (3, 3)
        assert grouped["b"].shape == (2, 3)


def test_grouper_aggregate_mixed_spec(tmp_path) -> None:
    frame_one = pd.DataFrame(
        {
            "category": ["b", "c", "a"],
            "value": [5, 7, 1],
            "duration": [30, 50, 10],
            "weight": [1.4, 2.0, 0.5],
        }
    )
    frame_two = pd.DataFrame(
        {
            "category": ["a", "b", "a"],
            "value": [4, 2, 3],
            "duration": [20, 40, 35],
            "weight": [0.7, 1.1, 0.9],
        }
    )

    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_parquet(first, frame_one)
    _write_parquet(second, frame_two)

    with PieceGrouper([first, second], group_by="category") as grouper:
        result = grouper.aggregate(
            {
                "value": ["sum", "max"],
                "duration": {"avg_duration": "avg"},
                "weight": {"range": lambda s: float(s.max() - s.min())},
            }
        )

    result = result.sort_values("category").reset_index(drop=True)

    expected = (
        pd.concat([frame_one, frame_two], ignore_index=True)
        .groupby("category", as_index=False)
        .agg(
            value_sum=("value", "sum"),
            value_max=("value", "max"),
            avg_duration=("duration", "mean"),
            range=("weight", lambda s: float(s.max() - s.min())),
        )
        .sort_values("category")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        result, expected, check_dtype=False, check_exact=False, rtol=1e-9, atol=1e-9
    )



def test_grouper_keep_sorted_persists_sorted_file(tmp_path) -> None:
    frame_one = pd.DataFrame(
        {
            "category": ["c", "b"],
            "value": [1, 5],
        }
    )
    frame_two = pd.DataFrame(
        {
            "category": ["a", "c"],
            "value": [4, 2],
        }
    )

    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_parquet(first, frame_one)
    _write_parquet(second, frame_two)

    scratch = tmp_path / "scratch"
    with PieceGrouper(
        [first, second],
        group_by="category",
        keep_sorted=True,
        scratch_directory=scratch,
    ) as grouper:
        sorted_path = grouper.sorted_path
        assert sorted_path is not None
        assert sorted_path.exists()
        assert sorted_path.parent == scratch

        keys = grouper.unique()
        assert keys == sorted(keys)

        table = pq.read_table(sorted_path, columns=["category"])
        expected_order = (
            pd.concat([frame_one, frame_two], ignore_index=True)
            .sort_values("category")
            ["category"]
            .tolist()
        )
        assert table.column(0).to_pylist() == expected_order

    assert sorted_path is not None and sorted_path.exists()
