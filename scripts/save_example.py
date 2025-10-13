


# /kellogg/proj/dashun/Sciscinet-v2/
import pandas as pd
import pyarrow.parquet as pq

from parcake.saver import PieceSaver

from tqdm.auto import tqdm

N = 10_000_000

output_path0 =  "test_0.parquet"
saver = PieceSaver(
    {"id": "int", "name": "str", "created": "datetime64[ns]"},
    output_path0,
    max_piece_size=10000,
)

for index in tqdm(range(N)):
    saver.add(id=index, name=f"user_{index}", created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index))
saver.close()

# save another file
output_path1 =  "test_1.parquet"
saver = PieceSaver(
    {"id": "int", "name": "str", "created": "datetime64[ns]"},
    output_path1,
    max_piece_size=10000,
)

for index in tqdm(range(N, N*2)):
    saver.add(id=index, name=f"user_{index}", created=pd.Timestamp("2023-01-01")+pd.Timedelta(seconds=index))
saver.close()

table = pq.read_table(output_path0)
df = table.to_pandas()
print(df)

table = pq.read_table(output_path1)
df = table.to_pandas()
print(df)
