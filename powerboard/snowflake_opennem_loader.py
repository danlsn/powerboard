import os
from pathlib import Path

import yaml
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv(".env")


ctx = snowflake.connector.connect(
    host=os.getenv("SF_HOST"),
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA"),
)

cur = ctx.cursor()

sql = """
        select ID, TIMESTAMP
        from LOAD_OPENNEM__TEMPERATURE
    """

cur.execute("alter user set timezone = 'Australia/Melbourne'")
cur.execute(sql)

df = cur.fetch_pandas_all()

existing_idx = df.set_index(["ID", "TIMESTAMP"]).index

temperature_files = [
    *Path("../data/opennem-extract/7d").glob("au.nem.*.temperature_*.csv")
]

li_df = []
for file in temperature_files:
    df = pd.read_csv(file)
    li_df.append(df)

new_df = pd.concat(li_df, axis=0, ignore_index=True).drop_duplicates()
new_index = new_df.set_index(["id", "timestamp"]).index

mask = ~new_index.isin(existing_idx)

upload_set = new_df.loc[mask]
upload_set = upload_set.rename(str.upper, axis="columns")
success, nchunks, nrows, _ = write_pandas(ctx, upload_set, "LOAD_OPENNEM__TEMPERATURE")
...
