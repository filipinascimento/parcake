from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import pytest

from parcake.reader import PieceReader
from parcake.saver import PieceSaver


def _write_parquet_fixture(path: Path, start: int, count: int, *, piece_size: int = 2) -> Path:
    header = {"id": "int64", "value": "float64"}
    saver = PieceSaver(header, path, max_piece_size=piece_size)
    for idx in range(start, start + count):
        saver.add(id=idx, value=float(idx))
    saver.close()
    return path


def _flatten_ids(frames: Iterable[pd.DataFrame]) -> List[int]:
    ids: List[int] = []
    for frame in frames:
        ids.extend(frame["id"].tolist())
    return ids


def identity_lengths(df: pd.DataFrame, rg: int, path: Path) -> Tuple[int, str]:
    return len(df), path.name


def test_piece_reader_iterates_row_groups(tmp_path: Path) -> None:
    path = _write_parquet_fixture(tmp_path / "sample.parquet", start=0, count=5, piece_size=2)
    reader = PieceReader(path)

    frames = list(reader)
    assert len(reader) == 3  # 5 rows at piece_size=2 → 3 row groups
    assert len(frames) == len(reader)
    assert _flatten_ids(frames) == list(range(5))


def test_piece_reader_iter_with_info_multiple_files(tmp_path: Path) -> None:
    path_a = _write_parquet_fixture(tmp_path / "a.parquet", start=0, count=4, piece_size=2)
    path_b = _write_parquet_fixture(tmp_path / "b.parquet", start=100, count=3, piece_size=2)

    reader = PieceReader([path_a, path_b], keep_input_order=True)
    observed = [(p.name, rg, df["id"].tolist()) for df, p, rg in reader.iter_with_info()]

    expected = [
        ("a.parquet", 0, [0, 1]),
        ("a.parquet", 1, [2, 3]),
        ("b.parquet", 0, [100, 101]),
        ("b.parquet", 1, [102]),
    ]
    assert observed == expected
    assert reader.sources == (path_a, path_b)
    assert reader.tasks() == (
        (str(path_a), 0),
        (str(path_a), 1),
        (str(path_b), 0),
        (str(path_b), 1),
    )


def test_piece_reader_process_with_pool_keeps_order(tmp_path: Path) -> None:
    path = _write_parquet_fixture(tmp_path / "pool.parquet", start=0, count=6, piece_size=2)
    reader = PieceReader(path)

    pool = ThreadPool(processes=2)
    try:
        results = list(
            reader.process(
                identity_lengths,
                pool=pool,
                keep_order=True,
                unordered=False,
                chunksize=1,
            )
        )
    finally:
        pool.close()
        pool.join()

    assert results == [(2, "pool.parquet"), (2, "pool.parquet"), (2, "pool.parquet")]


def test_piece_reader_process_with_executor(tmp_path: Path) -> None:
    path = _write_parquet_fixture(tmp_path / "executor.parquet", start=10, count=4, piece_size=2)
    reader = PieceReader(path, columns=("id",))

    def first_id(df: pd.DataFrame) -> int:
        return int(df.iloc[0]["id"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        ids = list(reader.process(first_id, executor=executor, keep_order=True))

    assert ids == [10, 12]
    assert reader.columns == ("id",)
    assert reader.to_pandas_kwargs == {}


def test_piece_reader_missing_source_errors(tmp_path: Path) -> None:
    missing = tmp_path / "missing.parquet"
    with pytest.raises(FileNotFoundError):
        PieceReader(missing)
