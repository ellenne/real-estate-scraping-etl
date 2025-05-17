
# PriceHubble Take‑Home – Senior Data Engineer

## Overview
This repo contains a simple, reproducible ETL pipeline that:

1. Reads `scraping_data.jsonl`
2. Cleans & filters the data
3. Loads it into a local DuckDB database

The pipeline is orchestrated with **Prefect 2** and packaged in Docker for easy review.

## Quick Start

```bash
# Clone repo and place scraping_data.jsonl in project root
docker build -t pricehubble-etl .
docker run -v $PWD:/app pricehubble-etl
```

After the container runs you will find:

* `pricehubble.db` – a DuckDB file containing the `property_offers` table.

## Project Structure

| File | Purpose |
|------|---------|
| `pipeline.py` | Prefect flow with extract, transform, load tasks |
| `requirements.txt` | Python dependencies |
| `Dockerfile` | Reproducible build |
| `scraping_data.jsonl` | _Place provided data file here_ |

## Transformation Logic

* **Price parsing** – strip non‑numeric characters from `raw_price`.
* **Derived column** – `price_per_square_meter = price / living_area`.
* **Filters** – keep only:<br>
  – `price_per_square_meter` between **500** and **15 000**.<br>
  – `property_type` in {`apartment`,`house`}<br>
  – `scraping_date` \> **2020‑03‑05**

## Extending / Scaling

* **Replace Prefect** with Airflow or Dagster by swapping the orchestration layer.  
* **Incremental loads** – store latest `scraping_date` in DuckDB and skip older rows.  
* **CI** – integrate `docker build && pytest` in GitLab/GitHub actions.
