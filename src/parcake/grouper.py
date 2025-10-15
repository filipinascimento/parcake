"""Streaming group-by utilities for large Parquet datasets."""

from __future__ import annotations

import contextlib
import itertools
import os
import tempfile
from dataclasses import dataclass
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

from .reader import PieceReader, _normalize_sources
from .sorter import PieceSorter

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


def _split_dataframe(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    if df.empty:
        return []
    if len(df) <= chunk_size:
        return [df.reset_index(drop=True)]
    chunks: List[pd.DataFrame] = []
    for start in range(0, len(df), chunk_size):
        stop = min(start + chunk_size, len(df))
        chunks.append(df.iloc[start:stop].reset_index(drop=True))
    return chunks


_STREAMABLE_AGGREGATIONS = {
    "sum",
    "min",
    "max",
    "count",
    "len",
    "mean",
    "avg",
    "first",
    "last",
    "nunique",
}


def _streamable_name(value: str) -> Optional[str]:
    name = value.strip().lower()
    if name == "avg":
        return "mean"
    if name in _STREAMABLE_AGGREGATIONS:
        return name
    return None


@dataclass(frozen=True)
class _AggregationTemplate:
    column: str
    alias: str
    spec: Union[str, Callable[[pd.Series], Any]]
    stream_name: Optional[str]


@dataclass
class _StreamingAgg:
    name: str
    value: Any = None
    total: float = 0.0
    count: int = 0
    has_value: bool = False
    uniques: Optional[set[Any]] = None

    def update(self, series: pd.Series) -> None:
        if series.empty:
            return
        if self.name == "sum":
            current = series.sum(skipna=True)
            if self.has_value:
                self.value += current
            else:
                self.value = current
                self.has_value = True
            return
        if self.name == "min":
            nonnull = series.dropna()
            if nonnull.empty:
                return
            candidate = nonnull.min()
            if not self.has_value or candidate < self.value:
                self.value = candidate
                self.has_value = True
            return
        if self.name == "max":
            nonnull = series.dropna()
            if nonnull.empty:
                return
            candidate = nonnull.max()
            if not self.has_value or candidate > self.value:
                self.value = candidate
                self.has_value = True
            return
        if self.name == "count":
            self.count += series.count()
            return
        if self.name == "len":
            self.count += len(series)
            return
        if self.name == "mean":
            self.total += series.sum(skipna=True)
            self.count += series.count()
            return
        if self.name == "first":
            if self.has_value:
                return
            nonnull = series.dropna()
            if not nonnull.empty:
                self.value = nonnull.iloc[0]
                self.has_value = True
                return
            self.value = series.iloc[0]
            self.has_value = True
            return
        if self.name == "last":
            nonnull = series.dropna()
            if not nonnull.empty:
                self.value = nonnull.iloc[-1]
            else:
                self.value = series.iloc[-1]
            self.has_value = True
            return
        if self.name == "nunique":
            if self.uniques is None:
                self.uniques = set()
            self.uniques.update(series.dropna().tolist())
            return

    def finalize(self) -> Any:
        if self.name == "sum":
            if not self.has_value:
                return 0
            return self.value
        if self.name in {"min", "max", "first", "last"}:
            if not self.has_value:
                return pd.NA
            return self.value
        if self.name == "count":
            return self.count
        if self.name == "len":
            return self.count
        if self.name == "mean":
            if self.count == 0:
                return float("nan")
            return self.total / self.count
        if self.name == "nunique":
            return len(self.uniques) if self.uniques is not None else 0
        return pd.NA


@dataclass
class _AggregationState:
    template: _AggregationTemplate
    streaming: Optional[_StreamingAgg] = None
    buffers: Optional[List[pd.Series]] = None

    def __post_init__(self) -> None:
        if self.template.stream_name is not None:
            self.streaming = _StreamingAgg(self.template.stream_name)
            self.buffers = None
        else:
            self.buffers = []
            self.streaming = None

    def update(self, series: pd.Series) -> None:
        if self.streaming is not None:
            self.streaming.update(series)
            return
        assert self.buffers is not None
        self.buffers.append(series.copy(deep=False))

    def finalize(self) -> Any:
        if self.streaming is not None:
            return self.streaming.finalize()
        assert self.buffers is not None
        if not self.buffers:
            empty = pd.Series([], dtype="float64")
            if callable(self.template.spec):
                return self.template.spec(empty)
            return empty.agg(self.template.spec)
        combined = pd.concat(self.buffers, ignore_index=True)
        if callable(self.template.spec):
            return self.template.spec(combined)
        return combined.agg(self.template.spec)


def _parse_aggregations(
    aggregations: Mapping[
        str,
        Union[
            str,
            Sequence[Union[str, Callable[[pd.Series], Any]]],
            Mapping[str, Union[str, Callable[[pd.Series], Any]]],
            Callable[[pd.Series], Any],
        ],
    ]
) -> List[_AggregationTemplate]:
    templates: List[_AggregationTemplate] = []
    for column, spec in aggregations.items():
        templates.extend(_expand_aggregation_spec(column, spec))
    return templates


def _expand_aggregation_spec(
    column: str,
    spec: Union[
        str,
        Sequence[Union[str, Callable[[pd.Series], Any]]],
        Mapping[str, Union[str, Callable[[pd.Series], Any]]],
        Callable[[pd.Series], Any],
    ],
) -> List[_AggregationTemplate]:
    if callable(spec):
        return [
            _AggregationTemplate(
                column=column,
                alias=f"{column}_custom",
                spec=spec,
                stream_name=None,
            )
        ]
    if isinstance(spec, Mapping):
        mappings: List[_AggregationTemplate] = []
        for alias, value in spec.items():
            if callable(value):
                mappings.append(
                    _AggregationTemplate(
                        column=column,
                        alias=alias,
                        spec=value,
                        stream_name=None,
                    )
                )
                continue
            value_str = str(value)
            mappings.append(
                _AggregationTemplate(
                    column=column,
                    alias=alias,
                    spec=value_str,
                    stream_name=_streamable_name(value_str),
                )
            )
        return mappings
    if isinstance(spec, Sequence) and not isinstance(spec, (str, bytes)):
        templates: List[_AggregationTemplate] = []
        for index, value in enumerate(spec):
            if callable(value):
                templates.append(
                    _AggregationTemplate(
                        column=column,
                        alias=f"{column}_custom_{index}",
                        spec=value,
                        stream_name=None,
                    )
                )
                continue
            value_str = str(value)
            templates.append(
                _AggregationTemplate(
                    column=column,
                    alias=f"{column}_{value_str}",
                    spec=value_str,
                    stream_name=_streamable_name(value_str),
                )
            )
        return templates
    value_str = str(spec)
    return [
        _AggregationTemplate(
            column=column,
            alias=f"{column}_{value_str}",
            spec=value_str,
            stream_name=_streamable_name(value_str),
        )
    ]


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
        self._sources = tuple(_normalize_sources(source))
        self._group_columns = _normalise_groupby(group_by)
        self._project_columns = _normalise_columns(columns, self._group_columns)
        self._select_all = not bool(self._project_columns)
        self._chunk_size = _normalise_chunk_size(max_chunk_size)
        self._keep_sorted = bool(keep_sorted)
        self._to_pandas_kwargs = dict(to_pandas_kwargs or {})
        self._scratch_dir = Path(scratch_directory).resolve() if scratch_directory else None
        self._max_memory = max_memory

        if threads is not None and threads <= 0:
            raise ValueError("threads must be a positive integer when provided.")
        self._threads = threads

        if self._scratch_dir is not None:
            self._scratch_dir.mkdir(parents=True, exist_ok=True)

        self._sorted_path: Optional[Path] = None
        self._cleanup_paths: List[Path] = []

        if sort:
            temp_path = _make_temp_path(self._scratch_dir)
            sorter = PieceSorter(
                source=self._sources,
                columns=[(column, True) for column in self._group_columns],
            )
            sorter.sort(
                temp_path,
                temp_directory=self._scratch_dir,
                memory_limit=self._max_memory,
                progress_bar=False,
            )
            self._sorted_path = temp_path
            self._active_sources: Tuple[Path, ...] = (temp_path,)
            if not self._keep_sorted:
                self._cleanup_paths.append(temp_path)
        else:
            self._active_sources = self._sources

        self._reader: Optional[PieceReader] = None
        self._reader_iter: Optional[Iterator[pd.DataFrame]] = None
        self._pending_chunks: List[pd.DataFrame] = []
        self._next_group_buffer: Optional[pd.DataFrame] = None

    def _reset_stream(self) -> None:
        columns: Optional[Sequence[str]] = None if self._select_all else self._project_columns
        self._reader = PieceReader(
            self._active_sources,
            columns=columns,
            to_pandas_kwargs=self._to_pandas_kwargs,
        )
        self._reader_iter = iter(self._reader)
        self._pending_chunks = []
        self._next_group_buffer = None

    def _fetch_next_chunk(self) -> Optional[pd.DataFrame]:
        iterator = self._reader_iter
        if iterator is None:
            return None
        while True:
            if self._pending_chunks:
                return self._pending_chunks.pop(0)
            try:
                batch = next(iterator)
            except StopIteration:
                return None
            if batch is None or batch.empty:
                continue
            batch = batch.reset_index(drop=True)
            if len(batch) > self._chunk_size:
                self._pending_chunks.extend(_split_dataframe(batch, self._chunk_size))
                continue
            return batch

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
        assert self._reader_iter is not None

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
        results: List[GroupKey] = []
        for key, chunk_iter in self:
            results.append(key)
            for _ in chunk_iter:
                pass
        return results

    def aggregate(
        self,
        aggregations: Mapping[
            str,
            Union[
                str,
                Sequence[Union[str, Callable[[pd.Series], Any]]],
                Mapping[str, Union[str, Callable[[pd.Series], Any]]],
                Callable[[pd.Series], Any],
            ],
        ],
    ) -> pd.DataFrame:
        templates = _parse_aggregations(aggregations)
        if not templates:
            return pd.DataFrame(columns=list(self._group_columns))

        records: List[Dict[str, Any]] = []
        for key, chunk_iter in self:
            record: Dict[str, Any] = {}
            if isinstance(key, tuple):
                for column, value in zip(self._group_columns, key):
                    record[column] = value
            else:
                record[self._group_columns[0]] = key

            states = [_AggregationState(template) for template in templates]
            for chunk in chunk_iter:
                for state in states:
                    series = chunk[state.template.column]
                    state.update(series)
            for state in states:
                record[state.template.alias] = state.finalize()
            records.append(record)

        if not records:
            return pd.DataFrame(columns=list(self._group_columns))

        return pd.DataFrame.from_records(records)

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
        self._reader_iter = None
        self._reader = None
        self._pending_chunks = []
        self._next_group_buffer = None
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
