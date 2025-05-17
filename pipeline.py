
import json
import re
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
from prefect import flow, task

SRC_FILE = Path("scraping_data.jsonl")
DB_FILE = Path("pricehubble.db")
TABLE_NAME = "property_offers"

@task
def extract(src: Path) -> pd.DataFrame:
    records = []
    with src.open("r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return pd.DataFrame.from_records(records)

def _parse_price(raw_price: str) -> float:
    # keep digits and dots/comma
    digits = re.sub(r"[^\d.]", "", raw_price.replace(" ", ""))
    if digits == "":
        return None
    return float(digits)
@task
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["price"] = df["raw_price"].apply(_parse_price)
    df["price_per_square_meter"] = df["price"] / df["living_area"]
    df["price_per_square_meter"] = df["price_per_square_meter"].round(2)
    df["scraping_date"] = pd.to_datetime(df["scraping_date"])
    # filter
    df = df[
        (df["price_per_square_meter"].between(500, 15000)) &
        (df["property_type"].isin(["apartment", "house"])) &
        (df["scraping_date"] > pd.Timestamp("2020-03-05"))
    ]
    # format date back to string
    df["scraping_date"] = df["scraping_date"].dt.strftime("%Y-%m-%d")
    # select output columns
    out_cols = ["id", "scraping_date", "property_type", "municipality",
                "price", "living_area", "price_per_square_meter"]
    df = df[out_cols].drop_duplicates()  # DROP DUPLICATES HERE
    print(f"Records processed: {len(df)}")
    return df

@task
def load(df: pd.DataFrame, db_file: Path, table: str) -> None:
    import os

    con = duckdb.connect(str(db_file))
    con.execute(f"DROP TABLE IF EXISTS {table};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id VARCHAR,
            scraping_date DATE,
            property_type VARCHAR,
            municipality VARCHAR,
            price DOUBLE,
            living_area DOUBLE,
            price_per_square_meter DOUBLE
        );
    """)
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM tmp_df;")
    con.close()

    # Logging after closing the DB
    print("DuckDB database saved as", db_file)
    print(f"File exists: {os.path.exists(db_file)}")
    print(f"File size: {os.path.getsize(db_file)} bytes")
    print(f"Records processed: {len(df)}")

@flow(name="pricehubble_etl")
def etl_flow():
    df_raw = extract(SRC_FILE)
    df_clean = transform(df_raw)
    load(df_clean, DB_FILE, TABLE_NAME)

if __name__ == "__main__":
    etl_flow()
