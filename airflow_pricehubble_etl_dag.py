"""
💡 Airflow Version (Reference Only)

This is an untested Airflow implementation of the ETL pipeline
originally written in Prefect. It is provided solely to demonstrate
familiarity with Airflow and DAG structuring for batch ETL workloads.
"""

from datetime import datetime
import json
import re
import pandas as pd
import duckdb
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

SRC_FILE = Path("scraping_data.jsonl")
DB_FILE = Path("pricehubble.db")
TABLE_NAME = "property_offers"

def extract(**kwargs):
    records = []
    with SRC_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    df = pd.DataFrame.from_records(records)
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json(orient='records'))

def _parse_price(raw_price: str) -> float:
    digits = re.sub(r"[^\d.]", "", raw_price.replace(" ", ""))
    if digits == "":
        return None
    return float(digits)

def transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.read_json(raw_data, orient='records')

    df["price"] = df["raw_price"].apply(_parse_price)
    df["price_per_square_meter"] = df["price"] / df["living_area"]
    df["price_per_square_meter"] = df["price_per_square_meter"].round(2)
    df["scraping_date"] = pd.to_datetime(df["scraping_date"])

    df = df[
        (df["price_per_square_meter"].between(500, 15000)) &
        (df["property_type"].isin(["apartment", "house"])) &
        (df["scraping_date"] > pd.Timestamp("2020-03-05"))
    ]

    df["scraping_date"] = df["scraping_date"].dt.strftime("%Y-%m-%d")

    out_cols = ["id", "scraping_date", "property_type", "municipality",
                "price", "living_area", "price_per_square_meter"]
    df = df[out_cols].drop_duplicates()

    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json(orient='records'))

def load(**kwargs):
    clean_data = kwargs['ti'].xcom_pull(key='clean_data', task_ids='transform')
    df = pd.read_json(clean_data, orient='records')

    con = duckdb.connect(str(DB_FILE))
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
    con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM tmp_df;")
    con.close()

    print("✅ DuckDB DB saved as", DB_FILE)

with DAG(
    dag_id="pricehubble_etl_airflow",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "pricehubble"],
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t1 >> t2 >> t3
