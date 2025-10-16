"""Streaming group-by utilities for large Parquet datasets."""

from __future__ import annotations

import contextlib
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

import numpy as np
import pandas as pd

from .reader import PieceReader, _normalize_sources
from .sorter import PieceSorter

try:  # pragma: no cover - optional dependency
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - tolerate missing tqdm
    tqdm = None  # type: ignore[assignment]

__all__ = ["PieceGrouper"]

PathLike = Union[str, Path]
GroupKey = Union[Any, Tuple[Any, ...]]


def _value_or_tuple(values: Tuple[Any, ...]) -> GroupKey:
    """Convert a tuple into a scalar when it has a single element."""

    if len(values) == 1:
        return values[0]
    return values


def _normalize_groupby(group_by: Union[str, Sequence[str]]) -> Tuple[str, ...]:
    if isinstance(group_by, str):
        group_by = (group_by,)
    values = tuple(group_by)
    if not values:
        raise ValueError("group_by must reference at least one column.")
    if not all(isinstance(item, str) for item in values):
        raise TypeError("group_by elements must be column names (strings).")
    return values


def _normalize_columns(
    requested: Optional[Sequence[str]],
    group_cols: Tuple[str, ...],
) -> Tuple[str, ...]:
    if requested is None:
        return tuple()
    ordered: List[str] = []
    seen: set[str] = set()
    for column in (*group_cols, *requested):
        if column in seen:
            continue
        ordered.append(column)
        seen.add(column)
    return tuple(ordered)


def _normalize_chunk_size(value: Optional[int]) -> int:
    if value is None:
        return 500_000
    if value <= 0:
        raise ValueError("max_chunk_size must be a positive integer.")
    return value


def _normalize_workers(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if value == -1:
        return max(1, os.cpu_count() or 1)
    if value <= 0:
        raise ValueError("thread count must be positive or -1 for auto.")
    return max(1, int(value))


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
    if chunk_size <= 0 or len(df) <= chunk_size:
        return [df.reset_index(drop=True)]
    pieces: List[pd.DataFrame] = []
    for start in range(0, len(df), chunk_size):
        stop = min(start + chunk_size, len(df))
        pieces.append(df.iloc[start:stop].reset_index(drop=True))
    return pieces


def _group_change_mask(piece_df: pd.DataFrame, group_columns: Sequence[str]) -> np.ndarray:
    if piece_df.empty:
        return np.zeros(0, dtype=bool)
    grouped = piece_df[list(group_columns)]
    changed_any = grouped.ne(grouped.shift()).any(axis=1)
    mask = changed_any.to_numpy()
    mask[0] = True
    return mask


def _contiguous_run_slices(mask: np.ndarray) -> List[slice]:
    if mask.size == 0:
        return []
    starts = np.flatnonzero(mask)
    if starts.size == 0:
        return []
    ends = np.empty_like(starts)
    ends[:-1] = starts[1:]
    ends[-1] = mask.size
    return [slice(int(s), int(e)) for s, e in zip(starts, ends)]


def _values_equal(left: Any, right: Any) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    try:
        result = left == right
    except Exception:
        return False
    if isinstance(result, bool):
        return result
    if result is pd.NA:
        return False
    if hasattr(result, "item"):
        try:
            return bool(result.item())
        except Exception:
            return False
    try:
        return bool(result)
    except Exception:
        return False


def _keys_match(left: Tuple[Any, ...], right: Tuple[Any, ...]) -> bool:
    if len(left) != len(right):
        return False
    return all(_values_equal(l, r) for l, r in zip(left, right))


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
    """Small wrapper yielding DataFrame chunks for a single group."""

    def __init__(self, frames: Sequence[pd.DataFrame]) -> None:
        self._frames = tuple(frames)
        self._index = 0

    def __iter__(self) -> "_GroupChunkIterator":
        self._index = 0
        return self

    def __next__(self) -> pd.DataFrame:
        if self._index >= len(self._frames):
            raise StopIteration
        frame = self._frames[self._index]
        self._index += 1
        return frame

    def __len__(self) -> int:
        return len(self._frames)


class _VerboseProgress:
    """Simple tqdm-backed reporter for verbose diagnostics."""

    def __init__(self, enabled: bool) -> None:
        self._bar = None
        if not enabled:
            return
        if tqdm is None:
            raise ImportError("PieceGrouper(verbose=True) requires the 'tqdm' package.")
        self._bar = tqdm(
            total=0,
            unit="step",
            leave=False,
            dynamic_ncols=True,
            bar_format="{l_bar}{bar}| {n} steps [{elapsed}] {postfix}",
        )

    def step(self, message: str) -> None:
        if self._bar is None:
            return
        self._bar.set_postfix_str(_truncate(message), refresh=True)
        self._bar.update(1)

    def status(self, message: str) -> None:
        if self._bar is None:
            return
        self._bar.set_postfix_str(_truncate(message), refresh=True)

    def info(self, message: str) -> None:
        if self._bar is None:
            return
        self._bar.write(message)
        self._bar.set_postfix_str(_truncate(message), refresh=True)

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None


def _truncate(message: str, limit: int = 60) -> str:
    if len(message) <= limit:
        return message
    return f"{message[: limit - 3]}..."


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
        preload_groups: bool = True,
        verbose: bool = False,
        to_pandas_kwargs: Optional[Mapping[str, Any]] = None,
        threads: Optional[int] = None,
    ) -> None:
        self._sources = tuple(_normalize_sources(source))
        self._group_columns = _normalize_groupby(group_by)
        self._project_columns = _normalize_columns(columns, self._group_columns)
        self._select_all = not bool(self._project_columns)
        self._chunk_size = _normalize_chunk_size(max_chunk_size)
        self._keep_sorted = bool(keep_sorted)
        self._preload_groups = bool(preload_groups)
        self._verbose = bool(verbose)
        self._progress = _VerboseProgress(self._verbose)
        self._to_pandas_kwargs = dict(to_pandas_kwargs or {})
        self._scratch_dir = Path(scratch_directory).resolve() if scratch_directory else None
        self._max_memory = max_memory
        if threads == 0:
            raise ValueError("threads must be greater than 0 or -1 for auto.")

        default_threads = threads

        self._sorted_path: Optional[Path] = None
        self._cleanup_paths: List[Path] = []
        self._unique_threads_default: Optional[int] = default_threads
        self._unique_keys_cache: Optional[List[GroupKey]] = None

        if self._scratch_dir is not None:
            self._scratch_dir.mkdir(parents=True, exist_ok=True)

        if sort:
            self._progress.info("Sorting input sources by group columns")
            temp_path = _make_temp_path(self._scratch_dir)
            sorter = PieceSorter(
                source=self._sources,
                columns=[(column, True) for column in self._group_columns],
            )
            sorter.sort(
                temp_path,
                temp_directory=self._scratch_dir,
                memory_limit=self._max_memory,
                progress_bar=self._verbose,
            )
            self._sorted_path = temp_path
            self._active_sources: Tuple[Path, ...] = (temp_path,)
            if not self._keep_sorted:
                self._cleanup_paths.append(temp_path)
            self._progress.step("Sorting complete")
        else:
            self._progress.info("Skipping sort (sort=False)")
            self._active_sources = self._sources
            self._progress.step("Using original source order")

        if self._preload_groups:
            self._ensure_unique_cache()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reader_columns(self) -> Optional[Tuple[str, ...]]:
        if self._select_all:
            return None
        return self._project_columns

    def _resolve_unique_threads(self, override: Optional[int]) -> Optional[int]:
        if override is not None:
            return _normalize_workers(override)
        default = self._unique_threads_default
        if default is None:
            return None
        return _normalize_workers(default)

    def _ensure_unique_cache(self, threads: Optional[int] = None) -> None:
        if self._unique_keys_cache is not None:
            return
        workers = self._resolve_unique_threads(threads)
        self._unique_keys_cache = self._collect_unique_keys(workers)

    def _collect_unique_keys(self, workers: Optional[int]) -> List[GroupKey]:
        columns = list(self._group_columns)
        reader = PieceReader(
            self._active_sources,
            columns=columns,
            to_pandas_kwargs=self._to_pandas_kwargs,
        )

        def _summarise(frame: pd.DataFrame) -> List[Tuple[Any, ...]]:
            if frame is None or frame.empty:
                return []
            df = frame.reset_index(drop=True)
            mask = _group_change_mask(df, columns)
            if mask.size == 0:
                return []
            starts = np.flatnonzero(mask)
            if starts.size == 0:
                starts = np.array([0], dtype=int)
            subset = df.iloc[starts][columns]
            return [tuple(row) for row in subset.itertuples(index=False, name=None)]

        if workers is None or workers == 1:
            batches: Iterable[List[Tuple[Any, ...]]] = (_summarise(df) for df in reader)
        else:
            batches = reader.process(
                _summarise,
                ncpu=workers,
                executor_kind="thread",
                keep_order=True,
                unordered=False,
            )

        uniques: List[GroupKey] = []
        previous: Optional[Tuple[Any, ...]] = None
        for batch in batches:
            if not batch:
                continue
            for key_tuple in batch:
                tup = tuple(key_tuple)
                if previous is not None and _keys_match(previous, tup):
                    continue
                previous = tup
                uniques.append(_value_or_tuple(tup))
        return uniques

    def _iter_group_frames(self) -> Iterator[Tuple[Tuple[Any, ...], Tuple[pd.DataFrame, ...]]]:
        reader = PieceReader(
            self._active_sources,
            columns=self._reader_columns(),
            to_pandas_kwargs=self._to_pandas_kwargs,
        )
        pending_key: Optional[Tuple[Any, ...]] = None
        pending_frames: List[pd.DataFrame] = []

        for batch in reader:
            if batch is None or batch.empty:
                continue
            for chunk in _split_dataframe(batch.reset_index(drop=True), self._chunk_size):
                mask = _group_change_mask(chunk, self._group_columns)
                for sl in _contiguous_run_slices(mask):
                    run = chunk.iloc[sl].reset_index(drop=True)
                    key_tuple = tuple(run.iloc[0][column] for column in self._group_columns)
                    if pending_key is None:
                        pending_key = key_tuple
                        pending_frames = [run]
                        continue
                    if _keys_match(pending_key, key_tuple):
                        pending_frames.append(run)
                    else:
                        yield pending_key, tuple(pending_frames)
                        pending_key = key_tuple
                        pending_frames = [run]

        if pending_key is not None and pending_frames:
            yield pending_key, tuple(pending_frames)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[Tuple[GroupKey, Iterator[pd.DataFrame]]]:
        self._progress.info("Starting group iteration")
        yielded = False

        for key_tuple, frames in self._iter_group_frames():
            key = _value_or_tuple(key_tuple)
            iterator = _GroupChunkIterator(frames)
            yielded = True
            yield key, iterator

        if yielded:
            self._progress.step("Iteration complete")

    def all(self) -> Iterator[Tuple[GroupKey, pd.DataFrame]]:
        for key, chunk_iter in self:
            pieces = list(chunk_iter)
            if len(pieces) == 1:
                yield key, pieces[0]
                continue
            yield key, pd.concat(pieces, ignore_index=True)

    def unique(self, threads: Optional[int] = None) -> List[GroupKey]:
        self._ensure_unique_cache(threads)
        if self._unique_keys_cache is None:
            return []
        # Return a shallow copy so callers cannot mutate internal cache.
        return list(self._unique_keys_cache)

    def __len__(self) -> int:
        self._ensure_unique_cache()
        if self._unique_keys_cache is None:
            raise TypeError("Length is unavailable when unique keys cannot be determined.")
        return len(self._unique_keys_cache)

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
        return [func(key, chunk_iter) for key, chunk_iter in self]

    def apply(
        self,
        func: Callable[[GroupKey, pd.DataFrame], Any],
    ) -> List[Any]:
        return [func(key, df) for key, df in self.all()]

    def filter(
        self,
        predicate: Callable[[GroupKey, pd.DataFrame], bool],
    ) -> List[Tuple[GroupKey, pd.DataFrame]]:
        return [(key, df) for key, df in self.all() if predicate(key, df)]

    @property
    def sorted_path(self) -> Optional[Path]:
        return self._sorted_path

    def close(self) -> None:
        self._progress.info("Closing PieceGrouper")
        if not self._keep_sorted:
            for path in self._cleanup_paths:
                with contextlib.suppress(Exception):
                    path.unlink(missing_ok=True)  # type: ignore[arg-type]
        self._cleanup_paths = []
        self._progress.close()

    def __enter__(self) -> "PieceGrouper":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - defensive cleanup
        with contextlib.suppress(Exception):
            self.close()
