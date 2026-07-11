# CDC NNDSS Disease Surveillance Pipeline

End-to-end data pipeline that ingests weekly CDC National Notifiable Diseases Surveillance System (NNDSS) data, transforms it into an analytics-ready star schema, and surfaces trends in Power BI. Built to mirror a production-style ELT setup: Dockerized orchestration, incremental ingestion, automated data quality tests, and DAG-based dependency management.

**Stack:** Airflow · Docker · Google Cloud Storage · BigQuery · dbt · Power BI

---

## Overview

The CDC publishes weekly, cumulative disease case counts across the U.S. through the Socrata Open Data (SODA) API. This pipeline automates the full lifecycle of that data — from raw API ingestion through to a queryable, tested, and visualized dataset — without requiring manual re-downloads or full-table reloads on every run.

Rather than pulling the entire dataset on every run, the pipeline tracks its own progress and asks the API only for what's new since the last successful load.


## Dashboard
 
**Overview page** — high-level KPIs (total cases, states reporting, STI burden rate, unique diseases tracked, outbreak alert rate), disease category breakdown, regional disease burden, and year-over-year trend.
 
![Dashboard overview](assets/BI%20PAGE%201.jpg)
 
**Detail page** — disease volatility across categories, year-over-year comparison, a sortable per-100K case table by disease, and regional breakdown.
 
![Dashboard detail](assets/BI%20PAGE%202.jpg)



## Architecture

```
CDC SODA API
     │
     ▼
Airflow (Dockerized)  ──▶  Google Cloud Storage (raw landing zone)
     │                              │
     │                              ▼
     │                        BigQuery (raw)
     │                              │
     └──────────────────────────────┘
                    │
                    ▼
                  dbt
        staging → dims/facts → marts
                    │
                    ▼
                Power BI
```

**Orchestration:** Apache Airflow, fully Dockerized, coordinates the pipeline end to end — extraction, load, dedup/quarantine handling, and dbt runs — as a single DAG with explicit task dependencies.

**Incremental ingestion:** A `pipeline_watermark` table tracks the last successfully ingested year/week. Each run queries the CDC API only for records past that watermark, rather than reloading historical data every time. The watermark itself is a composite value (`year * 100 + week`) rather than tracking year and week as separate fields — this avoids a subtle bug where taking `MAX(year)` and `MAX(week)` independently can produce a watermark that doesn't correspond to any real week (e.g. combining the max year from one row with the max week from another).

**Transformation:** dbt models the raw data into a star schema:
- **Staging** — light cleaning and type casting on raw source data
- **Dimensions / Facts** — conformed dimensions and a fact table at the disease/week/jurisdiction grain
- **Marts** — analytics-ready models with window functions (`LAG`) for week-over-week comparisons and running cumulative totals

**Visualization:** Power BI dashboards with custom DAX measures for year-over-year change and period-over-period change, built on top of the marts layer.

## Idempotent Loading

Re-running the pipeline (whether due to a retry, backfill, or manual re-trigger) doesn't produce duplicate rows in BigQuery. This is handled through a **left join deduplication pattern** at load time:

- The **newly pulled API data** (latest extraction) is used as the **left table**.
- The **existing BigQuery table** is used as the **right table**.
- A `LEFT JOIN` is performed between them on the natural key (e.g. disease, jurisdiction, year, week).
- Any row from the left table that finds a match in the right table — meaning it already exists in BigQuery — is excluded from the insert.
- Only rows with **no match** (genuinely new records) get loaded.

This means the same extraction can safely be run multiple times without needing a separate "check if exists" step or relying on `INSERT` failing on duplicates — the join itself does the filtering. It's a cheap, set-based way to guarantee idempotency without a full `MERGE` statement, since the pipeline's use case only needs new-row insertion rather than updating existing rows.

## Data Quality

- **Schema tests** via `schema.yml` — not-null, uniqueness, and relationship checks across staging, dims, and facts, with `severity='warn'` on fields sourced directly from the external API (rather than `error`) since upstream data quality issues shouldn't necessarily block the whole run.
- **Singular tests:**
  - `test_case_count_spike` — flags anomalous week-over-week jumps in case counts
  - `test_null_cases_no_flag` — catches null case counts that aren't explicitly flagged as such by the source
  - `test_duplicate_staging_rows` — guards against duplicate records slipping through staging
  - `test_future_week_rows` — catches records with a reporting week that hasn't happened yet (a signal of a data quality issue upstream)
- **Unmatched foreign key investigation** — BigQuery stored procedures to identify and surface fact rows that don't resolve to a valid dimension record, rather than silently dropping them.


## Project Structure

```
.
├── airflow/
│   ├── dags/            # DAG definitions
│   ├── extraction/       # API extraction scripts
│   ├── plugins/
│   └── config/
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── dims/
│   │   ├── facts/
│   │   └── marts/
│   ├── tests/            # Singular tests
│   └── macros/
├── docker-compose.yml
└── README.md
```

## Running Locally

1. Clone the repo and set up your `env.example` file with the required variables (GCP project ID, GCS bucket, BigQuery dataset/table names, and the local path to your GCP service account key). Don't forget to rename it to `.env` after.
2. Place your GCP service account JSON key locally and point `GOOGLE_APPLICATION_CREDENTIALS_HOST` to it in `.env`.
3. Build and start the stack:
   ```bash
   docker-compose up --build
   ```
4. Access the Airflow UI at `localhost:8080`.
5. The `cdc_health_pipeline` DAG ships **paused by default** — unpause it manually to trigger a run, or trigger it directly from the UI.

The DAG is scheduled `@weekly` to match the CDC's actual NNDSS publish cadence, but nothing runs automatically until it's unpaused.

## Key Design Decisions

- **Watermark-based incremental loading** over full-table refresh, to avoid redundant API calls and keep the pipeline scalable as historical data grows.
- **Idempotent loading, this pipeline has been designed to handle unexpected pipeline runs, retries, and errors that can lead to multiple duplication of data.
- **Composite watermark** (`year * 100 + week`) instead of separate max(year)/max(week) tracking, to avoid producing an invalid watermark from independently-maxed fields.
- **`severity='warn'` on external-source columns** rather than hard failures, since the pipeline shouldn't halt entirely over data quality issues in an upstream source it doesn't control.
- **DAG ships paused**, with an explicit weekly schedule defined — demonstrates realistic production scheduling without the DAG firing unexpectedly on clone.

---
