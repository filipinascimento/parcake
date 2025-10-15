"""High-level Parquet sorting powered by DuckDB."""

from __future__ import annotations

from glob import glob as _glob_glob, has_magic as _glob_has_magic

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import pyarrow.parquet as pq

try:  # pragma: no cover - exercised indirectly by tests
    import duckdb  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    duckdb = None  # type: ignore

PathLike = Union[str, Path]
ColumnSpec = Union[str, Tuple[str, bool]]


def _quote_identifier(identifier: str) -> str:
    """Return an escaped identifier enclosed in double quotes."""

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _quote_literal(value: str) -> str:
    """Return an escaped SQL string literal."""

    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _prepare_sources(
    source: Union[PathLike, Iterable[PathLike]]
) -> Tuple[Tuple[str, ...], Tuple[Path, ...]]:
    if isinstance(source, (str, Path)):
        items: Tuple[Union[str, Path], ...] = (source,)
    else:
        items = tuple(source)
    if not items:
        raise ValueError("No Parquet sources were provided.")

    patterns: List[str] = []
    candidates: List[Path] = []
    seen_candidates: set[Path] = set()

    for entry in items:
        text = str(entry)
        path_obj = Path(text)

        if _glob_has_magic(text):
            patterns.append(text)
            for match in sorted(Path(p) for p in _glob_glob(text)):
                if match.is_file() and match not in seen_candidates:
                    candidates.append(match)
                    seen_candidates.add(match)
            continue

        if path_obj.exists() and path_obj.is_dir():
            pattern = str(path_obj / "*.parquet")
            patterns.append(pattern)
            for match in sorted(path_obj.glob("*.parquet")):
                if match.is_file() and match not in seen_candidates:
                    candidates.append(match)
                    seen_candidates.add(match)
            continue

        if path_obj.exists() and path_obj.is_file():
            literal = str(path_obj)
            patterns.append(literal)
            if path_obj not in seen_candidates:
                candidates.append(path_obj)
                seen_candidates.add(path_obj)
            continue

        raise FileNotFoundError(f"Missing source: {text}")

    return tuple(patterns), tuple(candidates)


def _build_parquet_relation(patterns: Tuple[str, ...]) -> str:
    if len(patterns) == 1:
        return f"read_parquet({_quote_literal(patterns[0])})"
    literals = ", ".join(_quote_literal(pattern) for pattern in patterns)
    return f"read_parquet([{literals}])"


def _normalise_columns(
    columns: Sequence[ColumnSpec],
    default_ascending: bool,
) -> Tuple[Tuple[str, bool], ...]:
    if not columns:
        raise ValueError("At least one column must be provided for sorting.")
    normalised: List[Tuple[str, bool]] = []
    for column in columns:
        if isinstance(column, str):
            normalised.append((column, default_ascending))
        else:
            name, ascending = column
            normalised.append((name, bool(ascending)))
    return tuple(normalised)


def _detect_source_compression(paths: Tuple[Path, ...]) -> Optional[str]:
    """Inspect available Parquet files and return the first compression codec."""

    for candidate in paths:
        metadata = pq.ParquetFile(candidate).metadata
        if metadata is None or metadata.num_row_groups == 0:
            continue
        row_group = metadata.row_group(0)
        if row_group.num_columns == 0:
            continue
        codec = row_group.column(0).compression
        if codec:
            return codec.upper()
    return None


def _normalise_compression(
    compression: Optional[str],
    candidates: Tuple[Path, ...],
) -> Optional[str]:
    if compression is None:
        return None
    value = compression.strip().lower()
    if value == "preserve":
        detected = _detect_source_compression(candidates)
        return detected
    if value in {"none", "uncompressed"}:
        return "UNCOMPRESSED"
    return compression.upper()


def _normalise_memory_limit(memory_limit: Optional[Union[int, str]]) -> Optional[str]:
    if memory_limit is None:
        return None
    if isinstance(memory_limit, int):
        if memory_limit <= 0:
            raise ValueError("memory_limit must be a positive integer when provided.")
        return f"{memory_limit}B"
    value = memory_limit.strip()
    if not value:
        raise ValueError("memory_limit string must not be empty.")
    return value


@dataclass
class PieceSorter:
    """Sort Parquet datasets using DuckDB and external spill management.

    :param source: Single path, directory, glob pattern, or iterable of paths.
        Directories expand to ``*.parquet`` automatically and glob patterns are
        resolved against the current working directory.
    :param columns: Column names or ``(name, ascending)`` tuples describing the
        sort order. Bare column names share the default direction from the ``ascending`` attribute.
    :param ascending: Default sort direction for column names supplied without
        an explicit ``(name, ascending)`` tuple.
    """

    source: Union[PathLike, Iterable[PathLike]]
    columns: Sequence[ColumnSpec]
    ascending: bool = True

    def sort(
        self,
        destination: PathLike,
        *,
        compression: Optional[str] = "preserve",
        temp_directory: Optional[PathLike] = None,
        threads: Optional[int] = None,
        memory_limit: Optional[Union[int, str]] = None,
        progress_bar: bool = False,
    ) -> Path:
        """Sort input Parquet data and persist the ordered output.

        :param destination: Output file path. Parent directories are created on
            demand.
        :param compression: ``"preserve"`` (default) inherits the codec from the
            first source row group. Pass an explicit DuckDB compression value,
            or ``"none"``/``"uncompressed"`` to emit raw Parquet data.
        :param temp_directory: Optional directory DuckDB can use for on-disk
            spill files. Created automatically when missing.
        :param threads: Override DuckDB's ``PRAGMA threads`` value. Must be a
            positive integer.
        :param memory_limit: DuckDB ``PRAGMA memory_limit`` value. Accepts byte
            counts (``int``) or DuckDB-compatible size strings such as
            ``"8GB"``.
        :param progress_bar: When ``True`` enables DuckDB's COPY progress bar.
        :returns: Path to the sorted destination Parquet file.
        :raises ModuleNotFoundError: If :mod:`duckdb` is not installed.
        :raises ValueError: If no sources are provided, the column list is
            empty, or invalid thread/memory constraints are specified.
        :raises FileNotFoundError: When any source path or glob fails to
            resolve.
        """

        if duckdb is None:  # pragma: no cover - depends on environment
            raise ModuleNotFoundError(
                "duckdb is required to run PieceSorter. Install parcake with the "
                "DuckDB extra, for example `pip install parcake[duckdb]`.",
            )

        patterns, candidates = _prepare_sources(self.source)
        order_columns = _normalise_columns(self.columns, self.ascending)
        relation_sql = _build_parquet_relation(patterns)
        destination_path = Path(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        compression_codec = _normalise_compression(compression, candidates)
        memory_clause = _normalise_memory_limit(memory_limit)

        order_clause = ", ".join(
            f"{_quote_identifier(name)} {'ASC' if asc else 'DESC'}" for name, asc in order_columns
        )

        copy_options: List[str] = ["FORMAT PARQUET"]
        if compression_codec is not None:
            copy_options.append(f"COMPRESSION {_quote_literal(compression_codec)}")
        copy_clause = ", ".join(copy_options)

        sql = (
            "COPY (\n"
            f"  SELECT * FROM {relation_sql}\n"
            f"  ORDER BY {order_clause}\n"
            f") TO {_quote_literal(str(destination_path))} ({copy_clause});"
        )

        with duckdb.connect(database=":memory:") as connection:
            if threads is not None:
                if threads <= 0:
                    raise ValueError("threads must be a positive integer when provided.")
                connection.execute(f"PRAGMA threads={threads}")
            if memory_clause is not None:
                connection.execute(f"PRAGMA memory_limit={_quote_literal(memory_clause)}")
            if temp_directory is not None:
                temp_dir = Path(temp_directory)
                temp_dir.mkdir(parents=True, exist_ok=True)
                connection.execute(f"PRAGMA temp_directory={_quote_literal(str(temp_dir))}")
            pragma_progress = "enable_progress_bar" if progress_bar else "disable_progress_bar"
            connection.execute(f"PRAGMA {pragma_progress};")

            connection.execute(sql)

        return destination_path
