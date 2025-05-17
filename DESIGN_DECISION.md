# Design Decisions – PriceHubble Take-Home

## 1. Tech Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Orchestration | **Prefect 2** | Minimal boilerplate, easy local dev; can migrate to Airflow/Dagster if company standard. |
| Storage | **DuckDB** (file-based `pricehubble.db`) | Zero-config, analytics-grade SQL, embeddable in Docker; swap-able for Snowflake/Postgres. |
| Packaging | **Docker** | Reproducible, CI-friendly; keeps host clean. |
| Tests | **pytest** | Lightweight and ubiquitous in Python data teams. |

## 2. Pipeline Structure

* Mirrors the classic _ELT_ model and DBT’s `stg → int → mart` philosophy.
* Business rules (`price_per_sqm`, property-type whitelist, date threshold) live in `config.yaml` so changes don’t require code edits.

## 3. Data Quality & Transparency

* **Parsing helper** isolates messy price strings (`raw_price`) → numeric float.
* Row counts logged before & after filters:
* Automated tests assert key helpers (`_parse_price`) behave across edge-cases.

## 4. Idempotency & Incremental Loads

* DuckDB table schema created with `CREATE TABLE IF NOT EXISTS`.
* Future incremental pattern: store watermark (`MAX(scraping_date)`) and ingest only newer rows; merge on `id`.

## 5. Scale-Out Path

1. Swap DuckDB for Snowflake by changing connection string and `CREATE TABLE` DDL.  
2. Add cloud object storage (S3/GCS) and read JSONL in streaming chunks.  
3. Register flow in Prefect Cloud / Airflow for SLA-backed scheduling & alerting.

## 6. Why Not … ?

* **Pyspark** – overkill for current dataset; introduce only when data size warrants.
* **Heavy Dagster repo** – Prefect keeps repo footprint ~3 files; easier for reviewers.
