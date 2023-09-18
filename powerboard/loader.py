import os
from pathlib import Path

import yaml
import pandas as pd
import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(".env")

CONFIG = "./config.yml"
amber_sites = yaml.load(open(CONFIG, "r"), Loader=yaml.FullLoader)["sites"]

df_sites = pd.DataFrame(amber_sites).convert_dtypes(dtype_backend="pyarrow")
df_sites.to_csv("../data/amber_sites.csv", index=False)

Path("powerboard.duckdb").unlink(missing_ok=True)
con = duckdb.connect("powerboard.duckdb")

sql = """
        CREATE TABLE IF NOT EXISTS usage AS
            SELECT *
            FROM read_csv_auto('../data/amber-extract/daily/*.csv');
        DROP TABLE IF EXISTS subscription_fees;
        CREATE TABLE IF NOT EXISTS subscription_fees AS
            SELECT *
            FROM read_csv_auto('../data/amber_subscription_fees.csv');
        DROP TABLE IF EXISTS credits;
        CREATE TABLE IF NOT EXISTS credits AS
            SELECT *
            FROM read_csv_auto('../data/amber_credits.csv');
        DROP TABLE IF EXISTS sites;
        CREATE TABLE IF NOT EXISTS sites AS
            SELECT *
            FROM read_csv_auto('../data/amber_sites.csv');
        DROP TABLE IF EXISTS vdo;
        CREATE TABLE IF NOT EXISTS vdo AS
            SELECT *
            FROM read_csv_auto('../data/vdo.csv');
        """

con.sql(sql)


tables = ["usage", "subscription_fees", "credits", "sites", "vdo"]
for table in tables:
    con.table(table).show()
...


# Get list of tables in duckdb
tables = con.sql("""SHOW ALL TABLES;""").fetchall()

my_conn = create_engine(
    f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}"
)

my_conn.begin()

for table in tables:
    table_name = table[2]
    df = con.table(table_name).to_df()
    df.to_sql(
        name=table_name,
        con=my_conn,
        schema="powerboard",
        if_exists="replace",
        index=False,
    )

my_conn.()

...
