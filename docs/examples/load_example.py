
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
    def get_lengths_and_duration_sum(df):
        # Compute the length of the DataFrame and sum of the 'duration' column
        # do work...
        return len(df), df['duration'].sum()

    all_lengths = []
    all_duration_sum = 0.0
    for length, duration_sum in tqdm(reader.process(get_lengths_and_duration_sum, ncpu=-1, keep_order=True),total=reader.task_count()):
        all_lengths.append(length)
        all_duration_sum += duration_sum

    total_entries = sum(all_lengths)
    avg_duration = all_duration_sum / total_entries if total_entries > 0 else 0
    print("Total event entries: ", total_entries)
    print("Avg event duration: ", avg_duration)


    # Option 2b — Parallel processing with unordered results
    from multiprocessing import Pool 
    def get_first_id(df):
        # do work...
        return df['event_id'].iloc[0]

    all_first_ids = []
    with Pool(32) as pool:
        for res in tqdm(reader.process(get_first_id, pool=pool, keep_order=False, unordered=True, chunksize=4),total=reader.task_count()):
            all_first_ids.append(res)

    # Print first 10 event IDs
    # Note: these may be in any order due to unordered processing
    print("First 10 event IDs: ", all_first_ids[:10]) 

