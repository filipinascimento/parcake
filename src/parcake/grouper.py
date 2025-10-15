"""Streaming group-by utilities for large Parquet datasets."""

from __future__ import annotations

import contextlib
import itertools
import os
import tempfile
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow as pa

try:  # pragma: no cover - optional dependency used heavily elsewhere
    import duckdb  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover
    duckdb = None  # type: ignore
    _DUCKDB_IMPORT_ERROR = exc
else:
    _DUCKDB_IMPORT_ERROR = None

from .reader import _normalize_sources
from .sorter import (
    PieceSorter,
    _build_parquet_relation,
    _normalise_memory_limit,
    _prepare_sources,
    _quote_identifier,
)


__all__ = ["PieceGrouper"]

PathLike = Union[str, Path]
GroupKey = Union[Any, Tuple[Any, ...]]


def _value_or_tuple(values: Tuple[Any, ...]) -> GroupKey:
    """Convert a tuple into a scalar when the tuple has a single element."""

    if len(values) == 1:
        return values[0]
    return values


def _normalise_groupby(group_by: Union[str, Sequence[str]]) -> Tuple[str, ...]:
    if isinstance(group_by, str):
        return (group_by,)
    values = tuple(group_by)
    if not values:
        raise ValueError("groupby must reference at least one column.")
    if not all(isinstance(item, str) for item in values):
        raise TypeError("groupby elements must be column names (strings).")
    return values


def _ensure_duckdb() -> None:
    if duckdb is None:  # pragma: no cover - exercised when dependency missing
        raise ModuleNotFoundError(
            "PieceGrouper requires the optional 'duckdb' dependency. "
            "Install it via `pip install duckdb`."
        ) from _DUCKDB_IMPORT_ERROR


def _normalise_columns(
    requested: Optional[Sequence[str]],
    group_cols: Tuple[str, ...],
) -> Tuple[str, ...]:
    if requested is None:
        return tuple()
    ordered: List[str] = []
    seen: set[str] = set()
    for column in itertools.chain(group_cols, requested):
        if column in seen:
            continue
        ordered.append(column)
        seen.add(column)
    return tuple(ordered)


def _normalise_chunk_size(value: Optional[int]) -> int:
    if value is None:
        return 500_000
    if value <= 0:
        raise ValueError("max_chunk_size must be a positive integer.")
    return value


def _quote_columns(columns: Sequence[str]) -> str:
    return ", ".join(_quote_identifier(column) for column in columns)


def _make_temp_path(scratch: Optional[Path]) -> Path:
    if scratch is not None:
        scratch.mkdir(parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix="parcake_sorted_", suffix=".parquet", dir=str(scratch)
        )
        os.close(fd)
        return Path(temp_path)
    fd, temp_path = tempfile.mkstemp(prefix="parcake_sorted_", suffix=".parquet")
    os.close(fd)
    return Path(temp_path)


class _GroupChunkIterator:
    """Internal helper yielding DataFrame chunks for a single group."""

    def __init__(
        self,
        parent: "PieceGrouper",
        key: Tuple[Any, ...],
        initial: pd.DataFrame,
    ) -> None:
        self._parent = parent
        self._key = key
        self._pending = initial

    def __iter__(self) -> Iterator[pd.DataFrame]:
        parent = self._parent
        key = self._key
        df = self._pending
        self._pending = pd.DataFrame()

        while True:
            if df.empty:
                df = parent._fetch_for_group(key)
                if df is None:
                    return

            group_df, remainder = parent._split_by_group(df, key)
            if not group_df.empty:
                yield group_df

            if remainder is not None and not remainder.empty:
                parent._set_next_group_buffer(remainder)
                return

            df = pd.DataFrame()


class PieceGrouper:
    """Iterate over Parquet data grouped by one or more columns."""

    def __init__(
        self,
        source: Union[PathLike, Iterable[PathLike]],
        group_by: Union[str, Sequence[str]],
        *,
        columns: Optional[Sequence[str]] = None,
        sort: bool = True,
        scratch_directory: Optional[PathLike] = None,
        max_memory: Optional[Union[int, str]] = None,
        max_chunk_size: Optional[int] = None,
        keep_sorted: bool = False,
        to_pandas_kwargs: Optional[Mapping[str, Any]] = None,
        threads: Optional[int] = None,
    ) -> None:
        _ensure_duckdb()

        self._sources = _normalize_sources(source)
        self._group_columns = _normalise_groupby(group_by)
        self._project_columns = _normalise_columns(columns, self._group_columns)
        self._select_all = not bool(self._project_columns)
        self._sort_requested = bool(sort)
        self._chunk_size = _normalise_chunk_size(max_chunk_size)
        self._keep_sorted = bool(keep_sorted)
        self._to_pandas_kwargs = dict(to_pandas_kwargs or {})
        self._scratch_dir = Path(scratch_directory).resolve() if scratch_directory else None

        if threads is not None and threads <= 0:
            raise ValueError("threads must be a positive integer when provided.")

        if self._scratch_dir is not None:
            self._scratch_dir.mkdir(parents=True, exist_ok=True)

        self._patterns, _ = _prepare_sources(self._sources)
        self._relation_sql = _build_parquet_relation(self._patterns)

        self._conn = duckdb.connect(database=":memory:", read_only=False)
        if threads is not None:
            self._conn.execute(f"PRAGMA threads={int(threads)}")

        if self._scratch_dir is not None:
            self._conn.execute(f"SET temp_directory='{self._scratch_dir.as_posix()}'")

        if max_memory is not None:
            memory_limit = _normalise_memory_limit(max_memory)
            if memory_limit is not None:
                self._conn.execute(f"PRAGMA memory_limit='{memory_limit}'")

        self._sorted_path: Optional[Path] = None
        self._cleanup_paths: List[Path] = []
        if self._sort_requested and self._keep_sorted:
            temp_path = _make_temp_path(self._scratch_dir)
            sorter = PieceSorter(
                source=self._sources,
                columns=[(column, True) for column in self._group_columns],
            )
            sorter.sort(
                temp_path,
                temp_directory=self._scratch_dir,
                memory_limit=max_memory,
                progress_bar=False,
            )
            self._sorted_path = temp_path
            self._cleanup_paths.append(temp_path)
            self._relation_sql = _build_parquet_relation((str(temp_path),))
            self._sort_requested = False

        self._active_cursor = None
        self._active_reader: Optional[pa.RecordBatchReader] = None
        self._next_group_buffer: Optional[pd.DataFrame] = None

    def _reset_stream(self) -> None:
        if self._active_cursor is not None:
            self._active_cursor = None
        self._active_reader = None
        select_clause = "*"
        if not self._select_all:
            select_clause = _quote_columns(self._project_columns)
        sql = f"SELECT {select_clause} FROM {self._relation_sql}"
        if self._sort_requested:
            order_clause = ", ".join(_quote_identifier(col) for col in self._group_columns)
            sql = f"{sql} ORDER BY {order_clause}"
        self._active_cursor = self._conn.execute(sql)
        self._next_group_buffer = None

    def _fetch_next_chunk(self) -> Optional[pd.DataFrame]:
        assert self._active_cursor is not None
        while True:
            if self._active_reader is not None:
                try:
                    batch = self._active_reader.read_next_batch()
                except StopIteration:
                    self._active_reader = None
                    continue
                if batch is None or batch.num_rows == 0:
                    self._active_reader = None
                    continue
            else:
                try:
                    candidate = self._active_cursor.fetch_record_batch(self._chunk_size)
                except duckdb.InvalidInputException:
                    return None
                if candidate is None:
                    return None
                if isinstance(candidate, pa.RecordBatch):
                    batch = candidate
                else:
                    self._active_reader = candidate
                    try:
                        batch = self._active_reader.read_next_batch()
                    except StopIteration:
                        self._active_reader = None
                        continue
                    if batch is None or batch.num_rows == 0:
                        self._active_reader = None
                        continue

            df = batch.to_pandas(**self._to_pandas_kwargs)
            if not df.empty:
                return df

    def _fetch_for_group(self, key: Tuple[Any, ...]) -> Optional[pd.DataFrame]:
        if self._next_group_buffer is not None:
            df = self._next_group_buffer
            self._next_group_buffer = None
            return df
        return self._fetch_next_chunk()

    def _split_by_group(
        self,
        df: pd.DataFrame,
        key: Tuple[Any, ...],
    ) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        if df.empty:
            return df, None

        mask: Optional[pd.Series] = None
        for column, value in zip(self._group_columns, key):
            current = df[column] == value
            mask = current if mask is None else (mask & current)

        if mask is None:
            return df, None

        values = mask.to_numpy()
        if values.all():
            return df, None

        first_mismatch = int((~values).nonzero()[0][0])
        head = df.iloc[:first_mismatch].reset_index(drop=True)
        tail = df.iloc[first_mismatch:].reset_index(drop=True)
        return head, tail

    def _set_next_group_buffer(self, df: pd.DataFrame) -> None:
        self._next_group_buffer = df.reset_index(drop=True)

    def __iter__(self) -> Iterator[Tuple[GroupKey, Iterator[pd.DataFrame]]]:
        self._reset_stream()
        assert self._active_cursor is not None

        while True:
            if self._next_group_buffer is not None:
                initial = self._next_group_buffer
                self._next_group_buffer = None
            else:
                initial = self._fetch_next_chunk()
                if initial is None:
                    return

            initial = initial.reset_index(drop=True)
            key_row = initial.iloc[0]
            key_tuple = tuple(key_row[column] for column in self._group_columns)
            key = _value_or_tuple(key_tuple)

            iterator = _GroupChunkIterator(self, key_tuple, initial)
            yield key, iter(iterator)

    def all(self) -> Iterator[Tuple[GroupKey, pd.DataFrame]]:
        for key, chunk_iter in self:
            pieces = list(chunk_iter)
            if len(pieces) == 1:
                yield key, pieces[0]
                continue
            df = pd.concat(pieces, ignore_index=True)
            yield key, df

    def unique(self) -> List[GroupKey]:
        select_clause = ", ".join(_quote_identifier(col) for col in self._group_columns)
        sql = f"SELECT DISTINCT {select_clause} FROM {self._relation_sql}"
        if self._sort_requested or self._keep_sorted:
            order_clause = ", ".join(_quote_identifier(col) for col in self._group_columns)
            sql = f"{sql} ORDER BY {order_clause}"
        cursor = self._conn.execute(sql)
        table = cursor.fetch_arrow_table()
        if table is None:
            return []
        results: List[GroupKey] = []
        for batch in table.to_batches():
            df = batch.to_pandas()
            for _, row in df.iterrows():
                values = tuple(row[column] for column in self._group_columns)
                results.append(_value_or_tuple(values))
        return results

    def aggregate(
        self,
        aggregations: Mapping[str, Union[str, Sequence[str], Mapping[str, Union[str, Callable[[pd.Series], Any]]]]],
    ) -> pd.DataFrame:
        duckdb_exprs: List[str] = []
        python_specs: List[Tuple[str, str, Callable[[pd.Series], Any]]] = []

        for column, spec in aggregations.items():
            if callable(spec):
                alias = f"{column}_custom"
                python_specs.append((column, alias, spec))
                continue
            if isinstance(spec, Mapping):
                for alias, value in spec.items():
                    if callable(value):
                        python_specs.append((column, alias, value))
                        continue
                    expr = f"{str(value).upper()}({_quote_identifier(column)}) AS {_quote_identifier(alias)}"
                    duckdb_exprs.append(expr)
                continue
            if isinstance(spec, Sequence) and not isinstance(spec, str):
                for value in spec:
                    expr = f"{str(value).upper()}({_quote_identifier(column)}) AS {_quote_identifier(f'{column}_{value}')}"
                    duckdb_exprs.append(expr)
                continue
            expr = f"{str(spec).upper()}({_quote_identifier(column)}) AS {_quote_identifier(f'{column}_{spec}')}"
            duckdb_exprs.append(expr)

        frames: List[pd.DataFrame] = []
        if duckdb_exprs:
            group_cols = [_quote_identifier(col) for col in self._group_columns]
            select_parts = list(group_cols) + duckdb_exprs
            select_clause = ", ".join(select_parts)
            group_clause = ", ".join(group_cols)
            sql = f"SELECT {select_clause} FROM {self._relation_sql} GROUP BY {group_clause}"
            if self._sort_requested or self._keep_sorted:
                sql = f"{sql} ORDER BY {group_clause}"
            cursor = self._conn.execute(sql)
            table = cursor.fetch_arrow_table()
            if table is not None:
                frames.append(table.to_pandas())

        if python_specs:
            records: List[Dict[str, Any]] = []
            for key, df in self.all():
                record: Dict[str, Any] = {}
                if isinstance(key, tuple):
                    for column, value in zip(self._group_columns, key):
                        record[column] = value
                else:
                    record[self._group_columns[0]] = key
                for column, alias, func in python_specs:
                    record[alias] = func(df[column])
                records.append(record)
            if records:
                frames.append(pd.DataFrame(records))

        if not frames:
            return pd.DataFrame(columns=list(self._group_columns))

        if len(frames) == 1:
            return frames[0]

        merged = frames[0]
        for frame in frames[1:]:
            merged = pd.merge(merged, frame, on=list(self._group_columns), how="outer")
        return merged

    def map(
        self,
        func: Callable[[GroupKey, Iterator[pd.DataFrame]], Any],
    ) -> List[Any]:
        results: List[Any] = []
        for key, chunk_iter in self:
            results.append(func(key, chunk_iter))
        return results

    def apply(
        self,
        func: Callable[[GroupKey, pd.DataFrame], Any],
    ) -> List[Any]:
        results: List[Any] = []
        for key, df in self.all():
            results.append(func(key, df))
        return results

    def filter(
        self,
        predicate: Callable[[GroupKey, pd.DataFrame], bool],
    ) -> List[Tuple[GroupKey, pd.DataFrame]]:
        selected: List[Tuple[GroupKey, pd.DataFrame]] = []
        for key, df in self.all():
            if predicate(key, df):
                selected.append((key, df))
        return selected

    @property
    def sorted_path(self) -> Optional[Path]:
        """Path to the cached sorted dataset when ``keep_sorted`` is true."""

        return self._sorted_path

    def close(self) -> None:
        self._active_cursor = None
        self._active_reader = None
        if hasattr(self, "_conn") and self._conn is not None:
            self._conn.close()
            self._conn = None  # type: ignore
        if not self._keep_sorted:
            for path in self._cleanup_paths:
                with contextlib.suppress(Exception):
                    path.unlink(missing_ok=True)  # type: ignore[arg-type]
        self._cleanup_paths = []

    def __enter__(self) -> "PieceGrouper":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - defensive cleanup
        with contextlib.suppress(Exception):
            self.close()

