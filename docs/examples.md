# Examples

Real-world snippets that demonstrate Parcake's helpers.

## Chunked Writing

```python
--8<-- "examples/save_example.py"
```

## Row-Group Reading

```python
--8<-- "examples/load_example.py"
```

## Parquet Sorting

```python
--8<-- "examples/sort_example.py"
```

## Grouped Processing

Use `PieceGrouper` to stream contiguous groups, compute aggregations, or persist
sorted scratch files without loading the entire dataset at once.

```python
--8<-- "examples/group_example.py"
```
