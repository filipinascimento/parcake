from __future__ import annotations

import inspect
import os
import random
from pathlib import Path
from typing import (
    Any,
    Callable,
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
import pyarrow.parquet as pq

# Optional: try both backends for parallelism
try:
    from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
except Exception:  # pragma: no cover
    Executor = object  # type: ignore
    ProcessPoolExecutor = object  # type: ignore
    ThreadPoolExecutor = object  # type: ignore
    as_completed = None  # type: ignore

try:
    from multiprocessing import Pool  # type: ignore
except Exception:  # pragma: no cover
    Pool = None  # type: ignore

PathLike = Union[str, Path]


def _normalize_sources(source: Union[PathLike, Iterable[PathLike]]) -> List[Path]:
    if isinstance(source, (str, Path)):
        paths = [Path(source)]
    else:
        paths = [Path(p) for p in source]
    if not paths:
        raise ValueError("No sources provided.")
    missing = [p for p in paths if not p.exists()]
    if missing:
        preview = ", ".join(str(p) for p in missing[:5])
        more = "" if len(missing) <= 5 else f" (and {len(missing)-5} more)"
        raise FileNotFoundError(f"Missing source(s): {preview}{more}")
    return paths


def _call_processor(func: Callable, df: pd.DataFrame, rg: int, path: Path):
    """
    Call func with a flexible signature:
      - (df)
      - (df, path)
      - (df, rg)
      - (df, rg, path) OR (df, path, rg)
    """
    try:
        sig = inspect.signature(func)
        n = len(sig.parameters)
    except (ValueError, TypeError):
        n = 1
    if n <= 1:
        return func(df)
    if n == 2:
        # Heuristic: if first arg name suggests row/group index, pass rg; else path
        names = list(sig.parameters.keys())
        second = names[1].lower()
        if "row" in second or "group" in second or "rg" in second or "index" in second:
            return func(df, rg)
        return func(df, path)
    # 3 or more → pass both (in a stable order: df, rg, path)
    return func(df, rg, path)


class PieceReader:
    """Iterate over Parquet row groups and optionally process them in parallel.

    Example
    -------
    .. code-block:: python

       reader = PieceReader(Path("data").glob("*.parquet"))
       for df in reader:  # sequential
           ...

       def process_fn(df, path: Path, rg: int): ...
       for out in reader.process(process_fn, ncpu=-1, keep_order=True):
           ...
    """

    def __init__(
        self,
        source: Union[PathLike, Iterable[PathLike]],
        *,
        row_groups: Optional[Sequence[int]] = None,
        columns: Optional[Sequence[str]] = None,
        to_pandas_kwargs: Optional[Mapping[str, Any]] = None,
        keep_input_order: bool = True,
        shuffle: bool = False,
    ) -> None:
        """Configure row-group iteration across one or many Parquet files.

        :param source: Path, iterable of paths, or ``Path.glob`` iterator that
            resolves to Parquet files. Missing paths raise
            :class:`FileNotFoundError`.
        :param row_groups: Optional sequence of row-group indices to visit per
            file. When ``None`` every row group is yielded.
        :param columns: Optional subset of column names to project when reading
            Parquet data.
        :param to_pandas_kwargs: Keyword arguments forwarded to
            :meth:`pyarrow.Table.to_pandas`.
        :param keep_input_order: When ``False`` the provided sources are sorted
            lexicographically before building tasks.
        :param shuffle: Shuffle the computed ``(path, row_group)`` tasks. Useful
            when distributing work across unordered pools.
        :raises FileNotFoundError: If any source path does not exist.
        :raises IndexError: When an explicit row-group index falls outside the
            available range for a file.
        """
        self._sources = _normalize_sources(source)
        if not keep_input_order:
            self._sources = sorted(self._sources)

        self._columns = tuple(columns) if columns is not None else None
        self._to_pandas_kwargs = dict(to_pandas_kwargs or {})

        # Build flat task list: (source_path, row_group_index)
        tasks: List[Tuple[Path, int]] = []
        for src in self._sources:
            pf = pq.ParquetFile(src)
            if row_groups is None:
                rgs = range(pf.num_row_groups)
            else:
                rgs = [int(i) for i in row_groups]
                # Validate once (fast) using file's num_row_groups
                for i in rgs:
                    if i < 0 or i >= pf.num_row_groups:
                        raise IndexError(
                            f"Invalid row group {i} for {src} (num_row_groups={pf.num_row_groups})"
                        )
            tasks.extend((src, i) for i in rgs)

        if shuffle:
            random.shuffle(tasks)

        self._tasks: Tuple[Tuple[Path, int], ...] = tuple(tasks)

    # ------------------- Introspection -------------------

    def __len__(self) -> int:
        """Total number of row groups across all sources (after filtering)."""
        return len(self._tasks)

    @property
    def sources(self) -> Tuple[Path, ...]:
        """Tuple of resolved Parquet source paths in task order."""

        return tuple(self._sources)

    @property
    def columns(self) -> Optional[Tuple[str, ...]]:
        """Subset of column names projected when reading row groups."""

        return self._columns if self._columns is not None else None

    @property
    def to_pandas_kwargs(self) -> Mapping[str, Any]:
        """Keyword arguments forwarded to :meth:`pyarrow.Table.to_pandas`."""

        return dict(self._to_pandas_kwargs)

    def tasks(self) -> Tuple[Tuple[str, int], ...]:
        """Cheap (filepath, row_group_index) pairs for external pools."""
        return tuple((str(p), rg) for p, rg in self._tasks)

    def task_count(self) -> int:
        """Total number of (file, row-group) tasks."""
        return len(self._tasks)

    # ------------------- Iteration -------------------

    def __iter__(self) -> Iterator[pd.DataFrame]:
        """Yield DataFrames (sequential, in the prepared task order)."""
        for path, rg in self._tasks:
            yield self._read_rg(path, rg)

    def iter_with_info(self) -> Iterator[Tuple[pd.DataFrame, Path, int]]:
        """Yield (DataFrame, path, row_group_index) sequentially."""
        for path, rg in self._tasks:
            yield self._read_rg(path, rg), path, rg

    # ------------------- Parallel processing -------------------

    def process(
        self,
        func: Callable[..., Any],
        *,
        pool: Optional[Any] = None,
        executor: Optional[Any] = None,
        ncpu: Optional[int] = None,
        cores: Optional[int] = None,
        executor_kind: str = "process",  # "process" | "thread" (only used if we create one)
        keep_order: bool = False,
        unordered: bool = True,
        chunksize: int = 1,
    ) -> Iterable[Any]:
        """Process all (file, row-group) tasks with optional parallelism.

        :param func: Callable executed for each DataFrame. It may accept ``df``
            only, ``(df, path)``, ``(df, rg)`` or ``(df, rg, path)`` in any order.
        :param pool: Existing :class:`multiprocessing.Pool` supporting ``imap``.
            Used directly when supplied.
        :param executor: Existing :class:`concurrent.futures.Executor`. Used as
            the execution backend when provided.
        :param ncpu: Worker count for a temporary executor when neither ``pool``
            nor ``executor`` is supplied. ``-1`` uses all available CPUs.
        :param cores: Alias for ``ncpu`` kept for backwards compatibility.
        :param executor_kind: ``"process"`` (default) creates a
            :class:`~concurrent.futures.ProcessPoolExecutor`; ``"thread"``
            creates a :class:`~concurrent.futures.ThreadPoolExecutor`.
        :param keep_order: When ``True`` results are yielded in deterministic
            task order even when executing in parallel.
        :param unordered: When ``True`` and order is not requested, results are
            streamed as soon as they complete.
        :param chunksize: Forwarded to ``Pool.imap``/``imap_unordered`` when a
            multiprocessing pool is used.
        :returns: Iterable of results produced by *func*.
        """
        # 1) If a pool with imap is given, use it (fast path for multiprocessing.Pool).
        if pool is not None and hasattr(pool, "imap"):
            # NOTE: include `func` so the worker applies it and returns processed results.
            payloads = [
                (str(p), rg, self._columns, self._to_pandas_kwargs, func)
                for p, rg in self._tasks
            ]
            if keep_order:
                for res in pool.imap(_worker_call, payloads, chunksize):
                    yield res
            else:
                imap = getattr(pool, "imap_unordered", None) if unordered else getattr(pool, "imap", None)
                if imap is None:
                    imap = pool.imap
                for res in imap(_worker_call, payloads, chunksize):
                    yield res
            return

        # 2) Else, consider an Executor (provided or temporary).
        exec_obj, is_temp = None, False
        if executor is not None or ncpu is not None or cores is not None:
            exec_obj, is_temp = self._ensure_executor(executor, ncpu, cores, executor_kind)

        try:
            if exec_obj is None:
                # Sequential path
                for df, path, rg in self.iter_with_info():
                    yield _call_processor(func, df, rg, path)
                return

            # Submit all tasks
            futures = []
            for idx, (path, rg) in enumerate(self._tasks):
                futures.append((
                    idx,
                    exec_obj.submit(_worker_call, (str(path), rg, self._columns, self._to_pandas_kwargs, func)),
                ))

            if keep_order:
                # Gather in task order
                futures.sort(key=lambda t: t[0])
                for _, f in futures:
                    yield f.result()
            else:
                # Stream as they complete
                for _, f in futures:
                    pass  # ensure list is built
                for f in as_completed([f for _, f in futures]):  # type: ignore[arg-type]
                    yield f.result()
        finally:
            if exec_obj is not None and is_temp:
                # concurrent.futures API
                exec_obj.shutdown(cancel_futures=True)

    # ------------------- internals -------------------

    def _read_rg(self, path: Path, rg: int) -> pd.DataFrame:
        pf = pq.ParquetFile(path)
        tbl = pf.read_row_group(rg, columns=self._columns)
        return tbl.to_pandas(**self._to_pandas_kwargs)

    @staticmethod
    def _ensure_executor(
        executor: Optional[Any],
        ncpu: Optional[int],
        cores: Optional[int],
        kind: str,
    ):
        if executor is not None:
            return executor, False
        workers = (ncpu if ncpu is not None else cores)
        if workers is None:
            return None, False
        if workers in (-1, 0, None):
            workers = os.cpu_count() or 1
        else:
            workers = max(1, int(workers))
        if kind == "thread":
            return ThreadPoolExecutor(max_workers=workers), True
        return ProcessPoolExecutor(max_workers=workers), True


def _worker_call(args):
    """
    Worker entrypoint for pools/executors.

    Supports two payload shapes:
      (path, rg, columns, pandas_kwargs)
      (path, rg, columns, pandas_kwargs, func)
    If func is not present, just returns the DataFrame.
    """
    path, rg, columns, pandas_kwargs, *rest = args
    pf = pq.ParquetFile(path)
    tbl = pf.read_row_group(rg, columns=columns)
    df = tbl.to_pandas(**pandas_kwargs)
    if not rest:
        return df
    (func,) = rest
    return _call_processor(func, df, rg, Path(path))


# Example usage:
# 
# reader = PieceReader(paths_or_glob, columns=["a","b"], to_pandas_kwargs={"types_mapper": None})
# for df in reader:
#     ...  # process sequentially

# Option 1b
# for df, path, rg in reader.iter_with_info():
#     ...

# Option 2a — Parallel processing with preserved order
# def process_fn(df, path: Path, rg: int):
#     # do work...
#     return (path.name, rg, len(df))

# for res in reader.process(process_fn, ncpu=-1, keep_order=True):
#     ...

# Option 2b — Parallel processing with unordered results
# from multiprocessing import Pool
# with Pool(32) as pool:
#     for res in reader.process(process_fn, pool=pool, keep_order=False, unordered=True, chunksize=4):
#         ...

