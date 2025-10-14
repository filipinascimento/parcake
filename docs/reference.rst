API Reference
=============

.. module:: parcake

PieceSaver
----------

.. autoclass:: parcake.PieceSaver
   :members:
   :special-members: __init__
   :show-inheritance:

The :class:`PieceSaver` class buffers incoming rows and writes them to Parquet when
``max_piece_size`` is reached. Key parameters:

``header_types``
   Mapping of column names to pandas or Arrow type declarations.
``output_path``
   Destination Parquet file or folder path to create if it does not exist.
``max_piece_size``
   Number of rows to buffer before flushing a new row group.
``compression_level``
   Optional compression level forwarded to :mod:`pyarrow` writers.

Common helpers:

``add``
   Buffer a single row of data using keyword arguments.
``add_many``
   Stream an iterable of mapping objects directly to the saver.
``save_piece``
   Force the current buffer to flush to disk regardless of size.
``close``
   Write pending data and close the Parquet writer (also triggered by ``__exit__``).

PieceReader
-----------

.. autoclass:: parcake.PieceReader
   :members:
   :special-members: __iter__
   :show-inheritance:

The :class:`PieceReader` exposes efficient row-group iteration across one or many
Parquet files. Key construction arguments:

``source``
   Path, glob, or iterable of Parquet paths processed in order by default.
``row_groups``
   Optional list of row-group indices to limit the iteration domain.
``columns``
   Subset of columns to materialise per row group.
``to_pandas_kwargs``
   Additional options forwarded to :func:`pyarrow.Table.to_pandas`.
``keep_input_order``
   Whether to respect the order the files are provided.
``shuffle``
   Randomise the task order—useful for unordered parallel processing.

Important methods:

``iter_with_info``
   Yield ``(DataFrame, path, row_group_id)`` tuples for richer context.
``process``
   Map a callable across row groups with optional executors or multiprocessing pools.
``tasks``
   Access the internal list of ``(path, row_group_id)`` tasks to inspect workloads.

PieceSorter
-------------

.. autoclass:: parcake.PieceSorter
   :members:
   :special-members: __init__
   :show-inheritance:

The :class:`PieceSorter` uses DuckDB to perform external sorts over Parquet
sources, supporting glob patterns, per-column sort directions, and on-disk spill
control. Primary configuration points:

``source``
   Single Parquet path, directory, glob pattern, or iterable combining several inputs.
   Directories expand to ``*.parquet`` automatically so you can sort entire folders.
``columns``
   Column list or ``(name, ascending)`` tuples describing ORDER BY semantics.
``ascending``
   Default ordering for column names supplied without tuple form.

Runtime keyword arguments for :meth:`PieceSorter.sort`:

``compression``
   ``"preserve"`` (default) keeps the input codec, pass explicit values like
   ``"ZSTD"``, or use ``"none"`` to emit uncompressed output.
``temp_directory``
   Filesystem location for DuckDB spills (created automatically).
``threads``
   Override DuckDB's thread pool for multi-core sorting.
``memory_limit``
   Cap DuckDB's working memory (supports integers or DuckDB size strings).
``progress_bar``
   Enable DuckDB's COPY progress bar for long-running sorts.

Example:

.. code-block:: python

   from pathlib import Path
   from parcake import PieceSorter

   sorter = PieceSorter(Path("./data"), columns=["timestamp", ("user", False)])
   sorter.sort(
       Path("./data_sorted.parquet"),
       compression="preserve",
       temp_directory=Path("./duckdb-temp"),
       threads=8,
       memory_limit="8GB",
       progress_bar=True,
   )
