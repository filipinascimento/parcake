from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parcake import PieceGrouper

pytest.importorskip("duckdb")


def _write_parquet(path: Path, frame: pd.DataFrame, *, row_group_size: int | None = None) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, row_group_size=row_group_size)


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

@pytest.mark.parametrize("chunk_size", [2, 3, 4])
def test_grouper_handles_uneven_chunk_boundaries(tmp_path: Path, chunk_size: int) -> None:
    frame = pd.DataFrame(
        {
            "category": (["a"] * 5) + (["b"] * 4) + (["c"] * 3),
            "value": list(range(12)),
        }
    )
    path = tmp_path / "irregular.parquet"
    _write_parquet(path, frame, row_group_size=len(frame))

    expected_counts = frame.groupby("category")["value"].count().to_dict()
    expected_values = frame.groupby("category")["value"].apply(list).to_dict()
    ordered_categories = frame["category"].tolist()

    def expected_length_map(size: int) -> dict[str, list[int]]:
        mapping: dict[str, list[int]] = {key: [] for key in expected_counts}
        for start in range(0, len(ordered_categories), size):
            block = ordered_categories[start : start + size]
            idx = 0
            while idx < len(block):
                current = block[idx]
                j = idx
                while j < len(block) and block[j] == current:
                    j += 1
                mapping[current].append(j - idx)
                idx = j
        return mapping

    with PieceGrouper(
        path,
        group_by="category",
        max_chunk_size=chunk_size,
        columns=["value"],
        sort=False,
    ) as grouper:
        observed_lengths: dict[str, list[int]] = {}
        observed_values: dict[str, list[int]] = {}

        for key, chunk_iter in grouper:
            lengths: list[int] = []
            values: list[int] = []
            for chunk in chunk_iter:
                lengths.append(len(chunk))
                values.extend(chunk["value"].tolist())
                assert len(chunk) <= chunk_size
                assert (chunk["category"] == key).all()
            observed_lengths[key] = lengths
            observed_values[key] = values

    assert observed_values == expected_values
    assert observed_lengths == expected_length_map(chunk_size)


def test_grouper_len_and_chunk_len(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "category": ["a", "a", "a", "b", "b", "c"],
            "value": list(range(6)),
        }
    )
    path = tmp_path / "counts.parquet"
    _write_parquet(path, frame)

    with PieceGrouper(path, group_by="category", max_chunk_size=2) as grouper:
        assert len(grouper) == 3

        chunk_counts: dict[str, int] = {}
        collected: dict[str, list[int]] = {}

        for key, chunk_iter in grouper:
            chunk_counts[str(key)] = len(chunk_iter)
            values: list[int] = []
            for chunk in chunk_iter:
                values.extend(chunk["value"].tolist())
            collected[str(key)] = values

    assert chunk_counts == {"a": 2, "b": 2, "c": 1}
    assert collected == {"a": [0, 1, 2], "b": [3, 4], "c": [5]}


def test_grouper_len_lazy_preload(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "category": ["b", "a", "b", "c"],
            "value": [10, 20, 30, 40],
        }
    )
    path = tmp_path / "lazy.parquet"
    _write_parquet(path, frame)

    grouper = PieceGrouper(path, group_by="category", preload_groups=False, sort=True)
    try:
        assert len(grouper) == 3  # builds the index on demand
        keys: list[str] = []

        for key, chunk_iter in grouper:
            keys.append(str(key))
            assert len(chunk_iter) >= 1
            for _ in chunk_iter:
                pass

        assert keys == ["a", "b", "c"]
    finally:
        grouper.close()




def test_grouper_parallel_index_build(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "category": ["a"] * 3 + ["b"] * 2 + ["c"] * 4,
            "value": list(range(9)),
        }
    )
    path = tmp_path / "parallel.parquet"
    _write_parquet(path, frame)

    with PieceGrouper(path, group_by="category", threads=2) as grouper:
        assert len(grouper) == 3
        keys = [key for key, _ in grouper]

    assert keys == ["a", "b", "c"]


def test_grouper_verbose_reports_steps(tmp_path: Path, monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "category": ["a", "a", "b", "c"],
            "value": [1, 2, 3, 4],
        }
    )
    path = tmp_path / "verbose.parquet"
    _write_parquet(path, frame)

    messages: list[tuple[str, str]] = []
    updates: list[int] = []

    class DummyBar:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def set_postfix_str(self, message: str, refresh: bool = False) -> None:
            messages.append(("postfix", message))

        def update(self, value: int = 1) -> None:
            updates.append(value)

        def write(self, message: str) -> None:
            messages.append(("write", message))

        def close(self) -> None:
            messages.append(("close", ""))

    def dummy_tqdm(*args, **kwargs):
        messages.append(("init", repr(kwargs)))
        return DummyBar()

    monkeypatch.setattr("parcake.grouper.tqdm", dummy_tqdm)

    with PieceGrouper(
        path,
        group_by="category",
        sort=False,
        preload_groups=False,
        verbose=True,
    ) as grouper:
        for _ in grouper.all():
            pass

    assert any(tag == "write" for tag, _ in messages)
    assert sum(updates) >= 1
