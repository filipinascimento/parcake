# 🧁 Parcake

<p align="center">
  <img src="media/parcake.png" alt="Parcake logo" width="250"/>
</p>

**Parcake** makes Parquet workflows a *piece of cake* to slice, save, and serve.
It focuses on ergonomic chunked writing and row-group iteration so you can
stream data to and from Parquet without juggling low-level details.

---

## ✨ Features

Parcake provides high-level helpers to simplify chunked/pieced reading and
writing of Parquet files with a focus on **efficiency** and **ease of use**:

- 🍰 **Chunked writing** with schema enforcement via `PieceSaver`
- 🔁 **Row-group iteration** that yields pandas DataFrames with `PieceReader`
- ⚙️ **Parallel row-group processing** with `PieceReader.process`
- 💾 **Memory-aware operations** that avoid loading entire datasets at once

These utilities make it easier to work with large Parquet datasets—ideal for
data pipelines, preprocessing, or scalable ETL jobs.

---

## 🛠️ Installation

Install from PyPI:

```bash
pip install parcake
```

Or install from source in editable mode:

```bash
git clone https://github.com/your-org/parcake.git
cd parcake
pip install meson-python meson ninja
pip install -e . --no-build-isolation
```

The option `--no-build-isolation` is required to avoid dependency conflicts when
installing from source.

---

## 🚀 Quick Start

### PieceSaver — write Parquet pieces safely

```python
from pathlib import Path
import pandas as pd
from parcake import PieceSaver

output = Path("./events.parquet")
header = {"user": "str", "timestamp": "datetime64[ns]", "value": "float"}

with PieceSaver(header, output, max_piece_size=10_000) as saver:
    saver.add(user="alice", timestamp=pd.Timestamp.utcnow(), value=1.0)
    saver.add(user="bob", timestamp=pd.Timestamp.utcnow(), value=2.0)
```

`PieceSaver` buffers rows until `max_piece_size` is reached, then flushes a new
row group to disk automatically. You can call `add_many()` with an iterable of
rows, construct an instance `from_schema()`, or inspect `rows_written` and the
current `buffer_size` for monitoring.

---

### PieceReader — iterate row groups efficiently

```python
from parcake import PieceReader

reader = PieceReader("./events.parquet")
for frame in reader:  # sequential iteration
    print(len(frame))
```

Need additional context? Use `iter_with_info()` to obtain the path and row-group
index alongside each DataFrame:

```python
for frame, path, rg in PieceReader(["events_a.parquet", "events_b.parquet"]).iter_with_info():
    print(path.name, rg, len(frame))
```

---

### Parallel processing with PieceReader.process

```python
from concurrent.futures import ThreadPoolExecutor
from parcake import PieceReader

reader = PieceReader("./events.parquet")

def summarise(df, rg, path):
    return path.name, rg, df["value"].sum()

with ThreadPoolExecutor(max_workers=4) as executor:
    for result in reader.process(summarise, executor=executor, keep_order=True):
        print(result)
```

`PieceReader.process` can work with multiprocessing pools, executors, or
sequentially when no pool is provided. Set `ncpu=-1` to use all available cores,
`keep_order=True` to emit results in file × row-group order, or `unordered=True`
to stream results as soon as they are ready.

---

## 🧪 Examples

See `scripts/save_example.py` and `scripts/load_example.py` for end-to-end
examples that create Parquet files with `PieceSaver` and iterate through them
with `PieceReader`, including progress reporting and parallel execution recipes.

---

## 📦 API Overview

| Class/Method | Description |
|--------------|-------------|
| `PieceSaver` | Buffered writer that saves rows in fixed-size pieces with schema validation |
| `PieceReader` | Iterator that yields Parquet row groups as pandas DataFrames |
| `PieceReader.process` | Apply a callable to each row group sequentially or in parallel |

---

## 🧠 Why Parcake?

- **Memory Efficient**: Designed for datasets larger than available RAM
- **Simple and Composable**: Fits naturally into Pythonic data workflows
- **Scalable**: Parallel row-group processing with flexible execution backends
- **Lightweight**: Built only on essential dependencies (pandas, pyarrow)

---

## 📜 License

This project is licensed under the **MIT License** — see the [`LICENSE`](LICENSE)
file for details.

---

## 🧁 About the Name

Parcake = *Parquet* + *Piece of Cake* 🍰
It’s a small, sweet library to make your Parquet workflows simpler, chunk by
chunk.
