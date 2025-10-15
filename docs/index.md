<p align="center"><img src="assets/parcake.png" alt="Parcake logo" width="220"></p>

<p align="center"><a class="md-button md-button--primary" href="https://github.com/filipinascimento/parcake">View on GitHub</a></p>

# Parcake Documentation

Parcake makes Parquet workflows a piece of cake to slice, save, and serve. Use the
library to stream data to and from Parquet files without juggling low-level
pyarrow plumbing.

## Key Capabilities

- Chunked writing with schema enforcement via `PieceSaver`
- Efficient row-group iteration that yields pandas DataFrames with `PieceReader`
- Parallel row-group processing using high level helpers
- DuckDB-powered sorting for large datasets with `PieceSorter`
- Streaming group-by iteration and aggregations via `PieceGrouper`

## Installation

```bash
pip install parcake
```

## Getting Started

- Explore the [examples](examples.md) to see chunked writes, row-group reads, and sorting in action.
- Review the [API reference](reference.md) for full documentation of the public classes.
- Check out the [project repository](https://github.com/filipinascimento/parcake) for release notes and issue tracking.

If you run into anything unexpected, please open an issue on GitHub—feedback helps make
Parcake even better.
