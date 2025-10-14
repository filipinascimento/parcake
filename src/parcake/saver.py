"""Chunked/Pieced Parquet writing helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PathLike = Union[str, Path]

# Mapping of common dtype aliases to pyarrow and pandas representations.
_PYARROW_TYPE_ALIASES: MutableMapping[str, pa.DataType] = {
    "str": pa.string(),
    "string": pa.string(),
    "datetime64[ns]": pa.timestamp("ns"),
    "int": pa.int64(),
    "int64": pa.int64(),
    "float": pa.float64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
}

_PANDAS_TYPE_ALIASES: MutableMapping[str, str] = {
    "str": "string",
    "string": "string",
    "datetime64[ns]": "datetime64[ns]",
    "int": "Int64",
    "int64": "Int64",
    "float": "float64",
    "float64": "float64",
    "bool": "boolean",
    "boolean": "boolean",
}


def _normalise_type(value: Union[str, pa.DataType]) -> Tuple[pa.DataType, Any]:
    """Translate a declared dtype into Arrow and pandas equivalents.

    :param value: String alias (for example ``"str"``) or explicit
        :class:`pyarrow.DataType` instance describing a column.
    :returns: A tuple containing the Arrow data type and the corresponding pandas
        dtype representation.
    :raises TypeError: If *value* is neither a string nor a
        :class:`pyarrow.DataType`.
    :raises ValueError: If the string alias is not supported.
    """

    if isinstance(value, pa.DataType):
        return value, value.to_pandas_dtype()
    if not isinstance(value, str):  # pragma: no cover - defensive programming
        raise TypeError("Expected str or pyarrow.DataType")

    key = value.strip().lower()
    if key not in _PYARROW_TYPE_ALIASES:
        raise ValueError(f"Unsupported dtype alias: {value}")
    arrow_type = _PYARROW_TYPE_ALIASES[key]
    pandas_dtype = _PANDAS_TYPE_ALIASES[key]
    return arrow_type, pandas_dtype


class PieceSaver:
    """Incrementally persist rows to Parquet files using configurable chunks/pieces.

    :param header_types: Mapping between column names and their declared dtypes.
        Dtypes can be provided as string aliases (``"str"``, ``"int"``,
        ``"datetime64[ns]"`` and so on) or as concrete
        :class:`pyarrow.DataType` instances.
    :param output_path: Destination Parquet file. Missing parent directories are
        created automatically.
    :param max_piece_size: Maximum number of buffered rows before the saver
        flushes to disk.
    :param compression_level: Optional compression level forwarded to
        :class:`pyarrow.parquet.ParquetWriter`. When ``None`` the writer uses its
        default behaviour.
    :raises ValueError: If *header_types* is empty or *max_piece_size* is not a
        positive integer.
    """

    def __init__(
        self,
        header_types: Mapping[str, Union[str, pa.DataType]],
        output_path: PathLike,
        *,
        max_piece_size: int = 1_000_000,
        compression_level: Optional[int] = None,
    ) -> None:
        if not header_types:
            raise ValueError("header_types must define at least one column")
        if max_piece_size <= 0:
            raise ValueError("max_piece_size must be a positive integer")

        self._arrow_types: Dict[str, pa.DataType] = {}
        self._pandas_types: Dict[str, Any] = {}
        for column, dtype in header_types.items():
            arrow_type, pandas_dtype = _normalise_type(dtype)
            self._arrow_types[column] = arrow_type
            self._pandas_types[column] = pandas_dtype

        self._columns = list(self._arrow_types.keys())
        self._schema = pa.schema((name, dtype) for name, dtype in self._arrow_types.items())
        self._output_path = Path(output_path)
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._max_piece_size = max_piece_size
        self._compression_level = compression_level
        self._current_piece: Dict[str, list[Any]] = {col: [] for col in self._columns}
        self._writer: Optional[pq.ParquetWriter] = None
        self._rows_written = 0

    @property
    def columns(self) -> Tuple[str, ...]:
        """Column order stored in the Parquet file.

        :returns: Tuple describing the column order supplied at construction
            time.
        """

        return tuple(self._columns)

    @property
    def buffer_size(self) -> int:
        """Number of rows currently buffered in memory.

        :returns: Count of staged rows awaiting persistence.
        """

        return len(self._current_piece[self._columns[0]]) if self._columns else 0

    @property
    def rows_written(self) -> int:
        """Total number of rows persisted so far.

        :returns: Integer indicating how many rows were written to disk.
        """

        return self._rows_written

    def add(self, **kwargs: Any) -> None:
        """Buffer a row of data.

        :param kwargs: Column values keyed by column name. Missing columns are
            padded with ``None`` to keep column lengths aligned.
        """

        for column in self._columns:
            self._current_piece[column].append(kwargs.get(column))
        if self.buffer_size >= self._max_piece_size:
            self.save_piece()

    def add_many(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Buffer multiple rows sequentially.

        :param rows: Iterable of mapping objects representing column/value pairs
            per row.
        """

        for row in rows:
            self.add(**row)

    def save_piece(self, *, force: bool = False) -> None:
        """Persist the in-memory buffer to disk when it contains rows.

        :param force: When ``True`` the current buffer is written regardless of
            piece-size thresholds. Empty buffers are ignored.
        """

        if self.buffer_size == 0:
            return

        data = {
            column: pd.Series(self._current_piece[column], dtype=self._pandas_types[column])
            for column in self._columns
        }
        df = pd.DataFrame(data, columns=self._columns)
        table = pa.Table.from_pandas(df, schema=self._schema, preserve_index=False)

        if self._writer is None:
            writer_kwargs: Dict[str, Any] = {}
            if self._compression_level is not None:
                writer_kwargs["compression_level"] = self._compression_level
            self._writer = pq.ParquetWriter(self._output_path, self._schema, **writer_kwargs)
        self._writer.write_table(table)
        self._rows_written += len(df)
        self._current_piece = {col: [] for col in self._columns}

    def discard_piece(self) -> None:
        """Reset the in-memory buffer without writing it to disk."""

        self._current_piece = {col: [] for col in self._columns}

    def close(self) -> None:
        """Flush pending rows and close the underlying writer."""

        self.save_piece(force=True)
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> "PieceSaver":
        """Return the saver for use as a context manager."""

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[override]
        """Close the saver when the context exits."""

        self.close()

    @classmethod
    def from_schema(
        cls,
        schema: pa.Schema,
        output_path: PathLike,
        *,
        max_piece_size: int = 1_000_000,
        compression_level: Optional[int] = None,
    ) -> "PieceSaver":
        """Construct a saver directly from a PyArrow schema.

        :param schema: Schema used to derive column names and types.
        :param output_path: Destination Parquet file path.
        :param max_piece_size: Maximum number of buffered rows before writing.
        :param compression_level: Optional compression level forwarded to the
            saver constructor.
        :returns: A new :class:`PieceSaver` configured from *schema*.
        """

        header_types = {field.name: field.type for field in schema}
        return cls(
            header_types,
            output_path,
            max_piece_size=max_piece_size,
            compression_level=compression_level,
        )
