
# Example of saving data in pieces using PieceSaver

import pandas as pd
import pyarrow.parquet as pq

from parcake.saver import PieceSaver

from tqdm.auto import tqdm

import random

N = 10_000_000

schema = {
    "event_id": "int",
    "user_id": "str",
    "type": "str",
    "name": "str",
    "created": "datetime64[ns]"
}

output_path0 =  "events_0.parquet"
saver = PieceSaver(
    schema,
    output_path0,
    max_piece_size=10000,
)

for index in tqdm(range(N)):
    user_id = f"user_{random.randint(0, N//20)}"
    event_type = random.choice(["click", "view", "purchase", "like"])
    event_name = f"event_{random.randint(0, 100)}"
    saver.add(
        event_id=str(index),
        user_id=user_id,
        type=event_type,
        name=event_name,
        created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index)
    )
saver.close()

# save another file but now with smaller pieces
# (may be slower due to more overhead)

output_path1 =  "events_1.parquet"
# you can use with statement
with PieceSaver(schema, output_path1, max_piece_size=1000) as saver:
    for index in tqdm(range(N, N*2)):
        user_id = f"user_{random.randint(0, N//20)}"
        event_type = random.choice(["click", "view", "purchase", "like"])
        event_name = f"event_{random.randint(0, 100)}"
        saver.add(
            event_id=str(index),
            user_id=user_id,
            type=event_type,
            name=event_name,
            created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index)
        )

table = pq.read_table(output_path0)
df = table.to_pandas()
print(df)

table = pq.read_table(output_path1)
df = table.to_pandas()
print(df)
