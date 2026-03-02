
# Example of saving data in pieces using PieceSaver

import pandas as pd
import pyarrow.parquet as pq

# That is how you import PieceSaver
from parcake.saver import PieceSaver

from tqdm.auto import tqdm

import random

N = 10_000_000

# Define the schema of the data to be saved
schema = {
    "event_id": "int",
    "user_id": "str",
    "type": "str",
    "name": "str",
    "created": "datetime64[ns]",
    "duration": "float",
}

output_path0 =  "events_0.parquet"
saver = PieceSaver(
    schema,
    output_path0,
    max_piece_size=10000,
)

# simulate some event data
# event types have different average durations
# this is just to make the data a bit more interesting
eventType_to_avg_durations = {
    "click": 5.0,
    "view": 10.0,
    "purchase": 20.0,
    "like": 3.0,
}

for index in tqdm(range(N)):
    user_id = f"user_{random.randint(0, N//20)}"
    event_type = random.choice(list(eventType_to_avg_durations.keys()))
    event_name = f"event_{random.randint(0, 100)}"
    event_duration = random.uniform(0, 2*eventType_to_avg_durations[event_type])
    saver.add(
        event_id=str(index),
        user_id=user_id,
        type=event_type,
        name=event_name,
        created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index),
        duration=event_duration,
    )
saver.close()

# save another file but now with smaller pieces
# (may be slower due to more overhead)

output_path1 =  "events_1.parquet"
# you can use with statement
with PieceSaver(schema, output_path1, max_piece_size=10000) as saver:
    for index in tqdm(range(N, N*2)):
        user_id = f"user_{random.randint(0, N//20)}"
        event_type = random.choice(list(eventType_to_avg_durations.keys()))
        event_name = f"event_{random.randint(0, 100)}"
        event_duration = random.uniform(0, 2*eventType_to_avg_durations[event_type])
        saver.add(
            event_id=str(index),
            user_id=user_id,
            type=event_type,
            name=event_name,
            created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index),
            duration=event_duration,
        )

table = pq.read_table(output_path0)
df = table.to_pandas()
print(df)

table = pq.read_table(output_path1)
df = table.to_pandas()
print(df)


# save from chunked DataFrames directly
# useful when your upstream pipeline already yields DataFrame chunks
output_path2 = "events_2.parquet"
chunk_size = 25_000

with PieceSaver(schema, output_path2, max_piece_size=10000) as saver:
    for start in tqdm(range(0, N, chunk_size)):
        stop = min(start + chunk_size, N)
        size = stop - start
        chunk_types = [random.choice(list(eventType_to_avg_durations.keys())) for _ in range(size)]
        chunk = pd.DataFrame(
            {
                "event_id": range(start, stop),
                "user_id": [f"user_{random.randint(0, N//20)}" for _ in range(size)],
                "type": chunk_types,
                "name": [f"event_{random.randint(0, 100)}" for _ in range(size)],
                "created": [pd.Timestamp("2023-01-01") + pd.Timedelta(seconds=index) for index in range(start, stop)],
                "duration": [
                    random.uniform(0, 2 * eventType_to_avg_durations[event_type]) for event_type in chunk_types
                ],
            }
        )
        saver.write_dataframe(chunk)

table = pq.read_table(output_path2)
df = table.to_pandas()
print(df)
