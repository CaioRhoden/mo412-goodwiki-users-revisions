import os
import polars as pl
PATH = "../data/"

data: list[pl.DataFrame] = []
for file in os.listdir(PATH):
    if file.endswith(".parquet") and file.startswith("revisions_"):
        _data = pl.read_parquet(os.path.join(PATH, file))
        try:
            _data = _data.drop((["anon"]))
        except Exception as e:
            pass
        data.append(_data)

df = pl.concat(data)

# Filtering
df = df.filter(pl.col("userid") != 0)

df.write_parquet(f"{PATH}/revisions_dataset.parquet")
