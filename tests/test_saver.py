from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parcake.saver import PieceSaver


def test_piece_saver_writes_pieces(tmp_path: Path) -> None:
    output_path = tmp_path / "data.parquet"
    saver = PieceSaver(
        {"id": "int", "name": "str", "created": "datetime64[ns]"},
        output_path,
        max_piece_size=2,
    )

    saver.add(id=1, name="alice", created=pd.Timestamp("2023-01-01"))
    saver.add(id=2, name="bob", created=pd.Timestamp("2023-01-02"))
    assert saver.rows_written == 2
    assert saver.buffer_size == 0
    assert saver.columns == ("id", "name", "created")

    saver.add(id=3, name="carol", created=pd.Timestamp("2023-01-03"))
    saver.close()

    table = pq.read_table(output_path)
    df = table.to_pandas()
    assert list(df["id"]) == [1, 2, 3]
    assert saver.rows_written == 3


def test_piece_saver_add_many_flushes_chunks(tmp_path: Path) -> None:
    rows: List[Dict[str, object]] = [
        {"id": idx, "value": float(idx)} for idx in range(6)
    ]
    output_path = tmp_path / "batch.parquet"
    saver = PieceSaver({"id": "int64", "value": "float64"}, output_path, max_piece_size=3)

    saver.add_many(rows[:3])
    assert saver.rows_written == 3
    assert saver.buffer_size == 0

    saver.add_many(rows[3:])
    saver.close()

    table = pq.read_table(output_path)
    df = table.to_pandas()
    assert df["id"].tolist() == list(range(6))


def test_piece_saver_from_schema(tmp_path: Path) -> None:
    schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
    output_path = tmp_path / "schema.parquet"
    saver = PieceSaver.from_schema(schema, output_path, max_piece_size=2)
    saver.add(id=1, value=1.5)
    saver.add(id=2, value=2.5)
    saver.close()

    table = pq.read_table(output_path)
    assert table.schema == schema


def test_piece_saver_invalid_dtype_alias(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        PieceSaver({"oops": "not-a-type"}, tmp_path / "invalid.parquet")


def test_piece_saver_discard_piece(tmp_path: Path) -> None:
    output_path = tmp_path / "discard.parquet"
    with PieceSaver({"value": "float"}, output_path, max_piece_size=10) as saver:
        saver.add(value=1.0)
        saver.discard_piece()
        assert saver.buffer_size == 0
    assert not output_path.exists()


def test_piece_saver_requires_positive_piece_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        PieceSaver({"id": "int"}, tmp_path / "bad.parquet", max_piece_size=0)
