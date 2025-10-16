"""Example script for sorting one or many Parquet files."""

from __future__ import annotations

from pathlib import Path

from parcake import PieceSorter
from parcake import PieceReader
from tqdm import tqdm

import pandas as pd



import numpy as np
import pandas as pd
from typing import Iterable, Iterator, List, Tuple, Optional, Dict, Union, Callable, Any

from collections import Counter
from multiprocessing import Pool


if __name__ == "__main__":
    sources = [Path("./events_0.parquet"), Path("./events_1.parquet")]
    destination = Path("./sorted_events.parquet")

    group_columns = ["user_id", "type"]
    sorter = PieceSorter(
        sources,
        columns=group_columns,
        ascending=False,
    )

    sorter.sort(
        destination,
        compression="preserve",
        temp_directory=Path("./.duckdb-temp"),
        threads=4,
        memory_limit="4GB",
        progress_bar=True,
    )

    print(f"Sorted file written to {destination}")

    reader_unique = PieceReader(destination, columns=group_columns, keep_input_order=False)


    # go over the sorted data and create a Counter of unique group_columns values
    # aggregate the counts across all pieces



    total_counter = Counter()
    def count_groups(df):
        return Counter(tuple(row) for row in df[group_columns].itertuples(index=False, name=None))


    number_processors = 32
    # thread-based could also be used
    counters_list = []
    with Pool(number_processors) as pool:
        for res in tqdm(reader_unique.process(count_groups, pool=pool, keep_order=False, unordered=True, chunksize=4),total=reader_unique.task_count()):
            counters_list.append(res)

    for counter in counters_list:
        total_counter.update(counter)

    # now go over the file in order and group by the group_columns

    # approach 1: yield a collection of sub dataframe for each group
    # approach 2: yield an aggregated dataframe for each group


    reader_grouping = PieceReader(destination, keep_input_order=True)

    def _group_change_mask(piece_df: pd.DataFrame, group_columns: List[str]) -> np.ndarray:
        """
        Return a boolean mask of length len(piece_df) where True marks the start of a new group.
        Works for multiple columns by comparing to a 1-row-shifted copy.
        """
        if len(piece_df) == 0:
            return np.array([], dtype=bool)

        changed_any = piece_df[group_columns].ne(piece_df[group_columns].shift()).any(axis=1)
        mask = changed_any.to_numpy()
        mask[0] = True  # first row of piece always starts a run (relative to this piece)
        return mask

    def _contiguous_run_slices(mask: np.ndarray) -> List[slice]:
        """
        Given a boolean mask marking run starts, return slices [start:end) for each run.
        """
        if mask.size == 0:
            return []
        starts = np.flatnonzero(mask)
        ends = np.r_[starts[1:], mask.size]  # sentinel end
        return [slice(s, e) for s, e in zip(starts, ends)]

    def group_iterator(
        reader_grouping: Iterable[pd.DataFrame],
        group_columns: List[str],
        *,
        aggregate: bool = True,
    ) -> Iterator[Tuple[Tuple, Union[pd.DataFrame, Iterator[pd.DataFrame]]]]:
        """
        Iterate over a file sorted by `group_columns` and yield group-aligned data.

        Parameters
        ----------
        reader_grouping : iterable of pd.DataFrame
            PieceReader (or any iterable yielding piece DataFrames) ordered by `group_columns`.
        group_columns : list[str]
            Columns that define a group key (must match the sort order).
        aggregate : bool, default True
            - True: yield (group_key, single_concat_df) per group.
            - False: yield (group_key, iterator_of_df_pieces), streaming each piece DataFrame
                    that contributes to the group without concatenation.

        Yields
        ------
        (group_key, df_or_iterator)
            group_key is a tuple of the group column values.
            df_or_iterator is either a single concatenated DataFrame (aggregate=True) or
            an iterator over DataFrame slices that compose the group (aggregate=False).
        """
        current_key: Optional[Tuple] = None
        current_slices: List[pd.DataFrame] = []  # store views; concat once when group finishes

        def _emit_aggregate(slices: List[pd.DataFrame]) -> pd.DataFrame:
            # One concat per group; avoid extra copying when possible
            return pd.concat(slices, ignore_index=True, copy=False) if len(slices) > 1 else slices[0].reset_index(drop=True)

        def _emit_stream(slices: List[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Freeze so later mutations don’t affect the yielded iterator
            frozen = tuple(slices)
            for sl in frozen:
                yield sl  # each is a view into its piece; no extra copy

        for piece_df in reader_grouping:
            if piece_df.empty:
                continue

            mask = _group_change_mask(piece_df, group_columns)
            for sl in _contiguous_run_slices(mask):
                run = piece_df.iloc[sl]  # view
                run_key = tuple(run.iloc[0][group_columns])

                if current_key is None:
                    current_key = run_key
                    current_slices = [run]
                    continue

                if run_key == current_key:
                    # continuation of current group (may cross pieces)
                    current_slices.append(run)
                else:
                    # finish previous group
                    if aggregate:
                        yield current_key, _emit_aggregate(current_slices)
                    else:
                        yield current_key, _emit_stream(current_slices)
                    # start new group
                    current_key = run_key
                    current_slices = [run]

        # flush tail
        if current_key is not None and current_slices:
            if aggregate:
                yield current_key, _emit_aggregate(current_slices)
            else:
                yield current_key, _emit_stream(current_slices)


# Aggregated (current behavior)
reader_grouping = PieceReader(destination, keep_input_order=True)
for group_key, group_df in tqdm(group_iterator(reader_grouping, group_columns, aggregate=True), total=len(total_counter)):
    assert len(group_df) == total_counter[group_key]
    # process group_df ...

# Streaming df pieces (no concat), summing sizes to check:
reader_grouping = PieceReader(destination, keep_input_order=True)
for group_key, group_df_iterator in tqdm(group_iterator(reader_grouping, group_columns, aggregate=False),total=len(total_counter)):
    total_size = 0
    for df_piece in group_df_iterator:
        # process each df_piece ...
        total_size += len(df_piece)
    assert total_size == total_counter[group_key]







    ########



    # ---------- helpers you already have ----------
    def _group_change_mask(piece_df: pd.DataFrame, group_columns: List[str]) -> np.ndarray:
        if len(piece_df) == 0:
            return np.array([], dtype=bool)
        changed_any = piece_df[group_columns].ne(piece_df[group_columns].shift()).any(axis=1)
        mask = changed_any.to_numpy()
        mask[0] = True
        return mask

    def _contiguous_run_slices(mask: np.ndarray) -> List[slice]:
        if mask.size == 0:
            return []
        starts = np.flatnonzero(mask)
        ends = np.r_[starts[1:], mask.size]
        return [slice(s, e) for s, e in zip(starts, ends)]

    # ---------- composable reducer machinery ----------
    class _MeanVar:
        """Numerically stable online mean/var (Welford)."""
        __slots__ = ("n", "mean", "M2")
        def __init__(self):
            self.n = 0
            self.mean = 0.0
            self.M2 = 0.0
        def update(self, x: np.ndarray):
            # drop NaNs
            x = np.asarray(x)
            if x.size == 0:
                return
            m = np.isfinite(x)
            x = x[m]
            if x.size == 0:
                return
            for val in x:
                self.n += 1
                delta = val - self.mean
                self.mean += delta / self.n
                delta2 = val - self.mean
                self.M2 += delta * delta2
        def finalize_mean(self):
            return np.nan if self.n == 0 else self.mean
        def finalize_var(self, ddof=1):
            if self.n <= ddof:
                return np.nan
            return self.M2 / (self.n - ddof)
        def finalize_std(self, ddof=1):
            v = self.finalize_var(ddof=ddof)
            return np.sqrt(v) if np.isfinite(v) else np.nan

    ReducerSpec = Union[
        str,  # "sum"|"min"|"max"|"count"|"mean"|"var"|"std"
        Tuple[str, Callable[[str], Any], Callable[[Any, pd.Series], Any], Callable[[Any], Any]]  # ("custom", init, update, finalize)
    ]
    AggMap = Dict[str, Union[ReducerSpec, List[ReducerSpec]]]

    def _init_col_state(col: str, spec: ReducerSpec):
        if isinstance(spec, str):
            if spec == "sum":
                return ("sum", 0.0)
            if spec == "min":
                return ("min", np.inf)
            if spec == "max":
                return ("max", -np.inf)
            if spec == "count":
                return ("count", 0)  # counts rows (incl. NaN)
            if spec == "mean":
                return ("mean", _MeanVar())
            if spec == "var":
                return ("var", _MeanVar())
            if spec == "std":
                return ("std", _MeanVar())
            raise ValueError(f"Unknown reducer '{spec}' for column '{col}'.")
        # custom
        kind, init_fn, _, _ = spec
        if kind != "custom":
            raise ValueError(f"Bad reducer kind '{kind}' for column '{col}'.")
        return ("custom", init_fn(col))

    def _update_col_state(state_entry, series: pd.Series, spec: ReducerSpec):
        kind, state = state_entry
        vals = series.to_numpy(copy=False)
        if isinstance(spec, str):
            if spec == "sum":
                # nan-sum -> ignore NaN
                state += np.nansum(vals)
                return (kind, state)
            if spec == "min":
                mn = np.nanmin(vals) if vals.size else np.inf
                if np.isfinite(mn):
                    state = mn if not np.isfinite(state) else min(state, mn)
                return (kind, state)
            if spec == "max":
                mx = np.nanmax(vals) if vals.size else -np.inf
                if np.isfinite(mx):
                    state = mx if not np.isfinite(state) else max(state, mx)
                return (kind, state)
            if spec == "count":
                state += len(series)  # count rows (keeps NaNs)
                return (kind, state)
            if spec in ("mean", "var", "std"):
                state.update(vals)
                return (kind, state)
            raise ValueError(f"Unknown reducer '{spec}'.")
        # custom
        _, _, update_fn, _ = spec
        state = update_fn(state, series)
        return (kind, state)

    def _finalize_col_state(col: str, state_entry, spec: ReducerSpec) -> Tuple[str, Any]:
        if isinstance(spec, str):
            kind, state = state_entry
            if spec == "sum":
                return f"{col}__sum", state
            if spec == "min":
                return f"{col}__min", (np.nan if not np.isfinite(state) else state)
            if spec == "max":
                return f"{col}__max", (np.nan if not np.isfinite(state) else state)
            if spec == "count":
                return f"{col}__count", state
            if spec == "mean":
                return f"{col}__mean", state.finalize_mean()
            if spec == "var":
                return f"{col}__var", state.finalize_var(ddof=1)
            if spec == "std":
                return f"{col}__std", state.finalize_std(ddof=1)
        # custom
        kind, state = state_entry
        _, _, _, finalize_fn = spec
        return f"{col}__custom", finalize_fn(state)

    # NOTE on median/quantiles:
    # exact median is NOT composable. You either:
    #  - collect values (concat or append to a reservoir) and compute median at the end (high memory), or
    #  - use an approximate algorithm (e.g., TDigest/Greenwald–Khanna).
    # If you need exact median, prefer aggregate=True + concat for that column only.

    def group_reduce_iterator(
        reader_grouping: Iterable[pd.DataFrame],
        group_columns: List[str],
        agg: AggMap,
    ) -> Iterator[Tuple[Tuple, pd.Series]]:
        """
        Streaming group-by reduce over a file sorted by `group_columns`.

        Parameters
        ----------
        reader_grouping : iterable of pd.DataFrame
            PieceReader yielding DataFrames sorted by `group_columns`.
        group_columns : list[str]
            The grouping columns (sort order must match).
        agg : dict[str, ReducerSpec | list[ReducerSpec]]
            Mapping from value-column -> reducer or list of reducers.
            Built-ins: "sum", "min", "max", "count", "mean", "var", "std"
            Custom: ("custom", init_fn, update_fn, finalize_fn)

        Yields
        ------
        (group_key, result_row: pd.Series)
            result_row has flat columns like "{col}__sum", "{col}__mean", etc.
        """
        # normalize agg to dict[str, list[ReducerSpec]]
        norm_agg: Dict[str, List[ReducerSpec]] = {}
        for col, spec in agg.items():
            if isinstance(spec, list):
                norm_agg[col] = spec
            else:
                norm_agg[col] = [spec]

        current_key: Optional[Tuple] = None
        # states: dict[col] -> list[state_entry] aligned with norm_agg[col]
        states: Dict[str, List[Tuple[str, Any]]] = {}

        def _reset_states_for_key():
            nonlocal states
            states = {col: [_init_col_state(col, spec) for spec in specs]
                    for col, specs in norm_agg.items()}

        def _update_states(run: pd.DataFrame):
            for col, specs in norm_agg.items():
                s = run[col]
                col_states = states[col]
                for i, spec in enumerate(specs):
                    col_states[i] = _update_col_state(col_states[i], s, spec)

        def _finalize_series_for_key(key: Tuple) -> pd.Series:
            out_names = []
            out_vals = []
            for col, specs in norm_agg.items():
                for i, spec in enumerate(specs):
                    name, val = _finalize_col_state(col, states[col][i], spec)
                    out_names.append(name)
                    out_vals.append(val)
            return pd.Series(out_vals, index=out_names)

        for piece_df in reader_grouping:
            if piece_df.empty:
                continue
            mask = _group_change_mask(piece_df, group_columns)
            for sl in _contiguous_run_slices(mask):
                run = piece_df.iloc[sl]
                run_key = tuple(run.iloc[0][group_columns])

                if current_key is None:
                    current_key = run_key
                    _reset_states_for_key()
                    _update_states(run)
                    continue

                if run_key == current_key:
                    _update_states(run)
                else:
                    # emit previous
                    yield current_key, _finalize_series_for_key(current_key)
                    # start new
                    current_key = run_key
                    _reset_states_for_key()
                    _update_states(run)

        if current_key is not None:
            yield current_key, _finalize_series_for_key(current_key)



    # Suppose your value columns are: ["value_a", "value_b"]
    agg = {
        "duration": ["sum", "mean", "std"],
        # "created": ["min", "max", "count"],
    }

    reader_grouping = PieceReader(destination, keep_input_order=True)
    for group_key, row in tqdm(group_reduce_iterator(reader_grouping, group_columns, agg),total=len(total_counter)):
        # row is a Series with names: "duration__sum", "duration__mean", "duration__std", "created__min", ..
        pass

    # If you need a DataFrame at the end:
    results = []
    reader_grouping = PieceReader(destination, keep_input_order=True)
    for key, row in group_reduce_iterator(reader_grouping, group_columns, agg):
        results.append((key, row))
    out = pd.DataFrame(
        [(*k, *r.values) for k, r in results],
        columns=[*group_columns, *results[0][1].index]
    )
