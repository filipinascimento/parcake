
# Example of reading and processing Parquet files in pieces using PieceReader

from multiprocessing.pool import ThreadPool

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from parcake.reader import PieceReader
from tqdm.auto import tqdm


if __name__ == "__main__":
    paths = ["events_0.parquet", "events_1.parquet"]

    # Example usage:

    # Option 1a - Simple iteration with one file
    reader = PieceReader(paths[0])
    for df in tqdm(reader):
        pass

    # Option 1b - Simple iteration with multiple files
    reader = PieceReader(paths)
    for df in tqdm(reader):
        pass


    # Option 1c - Iteration with additional info
    reader = PieceReader(paths)
    with tqdm(reader.iter_with_info(),total=len(reader.tasks())) as pbar:
        for df, path, rg in pbar:
            pbar.set_description(f"Processing {path.name} rg={rg}")
            # do work...
            pass

    # Option 2a — Parallel processing with preserved order
    def get_lengths(df):
        # Compute the length of the DataFrame
        # do work...
        return len(df)

    all_lengths = []
    for res in tqdm(reader.process(get_lengths, ncpu=-1, keep_order=True),total=reader.task_count()):
        all_lengths.append(res)

    print("Total entries: ", sum(all_lengths))

    # Option 2b — Parallel processing with unordered results
    from multiprocessing import Pool 
    def get_first_id(df):
        # do work...
        return df['event_id'].iloc[0]

    all_first_ids = []
    with Pool(32) as pool:
        for res in tqdm(reader.process(get_first_id, pool=pool, keep_order=False, unordered=True, chunksize=4),total=reader.task_count()):
            all_first_ids.append(res)

