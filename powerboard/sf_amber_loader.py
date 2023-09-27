import os
from pathlib import Path

import yaml
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

load_dotenv(".env")

ctx = connect(
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
        select NEM_TIME, SITE_ID
        from LOAD_AMBER__USAGE
    """

cur.execute("alter user set timezone = 'Australia/Melbourne'")
cur.execute(sql)

df_existing = cur.fetch_pandas_all()

usage_csvs = Path("../data/amber-extract/daily").glob("*.csv")

li_df = []
for file in usage_csvs:
    df = pd.read_csv(file)
    li_df.append(df)

new_df = (
    pd.concat(li_df, axis=0, ignore_index=True)
    .drop_duplicates()
    .rename(str.upper, axis="columns")
)

idx_existing_records = df_existing.set_index(["NEM_TIME", "SITE_ID"]).index
idx_new_records = new_df.set_index(["NEM_TIME", "SITE_ID"]).index

mask = ~idx_new_records.isin(idx_existing_records)

upload_set = new_df.loc[mask]
upload_set = upload_set.rename(columns={"DATE": "date"})
success, nchunks, nrows, _ = write_pandas(ctx, upload_set, "LOAD_AMBER__USAGE")

print(f"Uploaded {nrows} rows to LOAD_AMBER__USAGE")
...
