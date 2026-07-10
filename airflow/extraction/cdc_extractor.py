import os
import json
import requests
import pandas as pd
from pandas import json_normalize
from google.cloud import bigquery, storage
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
DATASET_RAW = os.getenv("BQ_DATASET_RAW")
TABLE_NNDSS = os.getenv("BQ_TABLE_NNDSS")
FULL_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{TABLE_NNDSS}"
CDC_URL = "https://data.cdc.gov/resource/x9gk-5huc.json"


def fetch_cdc_data(last_year, last_week):
    all_data = []
    offset = 0
    limit = 50000

    while True:
        params = {
            "$where": f"(year > '{last_year}') OR (year = '{last_year}' AND week > '{last_week}')",
            "$limit": limit,
            "$offset": offset,
            "$order": "year ASC, week ASC"
        }

        response = requests.get(CDC_URL, params=params)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_data.extend(batch)
        print(f"Fetched {len(all_data)} rows so far...")

        if len(batch) < limit:
            break

        offset += limit

    print(f"Total rows fetched from CDC API: {len(all_data)}")
    return all_data


def backfill_by_year(start_year=2022, end_year=2026):
    client = bigquery.Client(project=PROJECT_ID)

    try:
        existing_query = f"""
            SELECT DISTINCT states, year, week, label
            FROM `{FULL_TABLE}`
        """
        existing_df = client.query(existing_query).to_dataframe()
        print(f"Existing rows in BigQuery: {len(existing_df)}")
    except Exception as e:
        print(f"Error fetching existing keys: {e}")  # ← add this
        existing_df = pd.DataFrame(columns=["states", "year", "week", "label"])

    for year in range(start_year, end_year + 1):
        print(f"\n--- Loading year {year} ---")

        for week in range(1, 54):
            all_data = []
            offset = 0
            limit = 50000

            while True:
                params = {
                    "$where": f"year = '{year}' AND week = '{week}'",
                    "$limit": limit,
                    "$offset": offset,
                    "$order": "week ASC"
                }

                response = requests.get(CDC_URL, params=params)
                response.raise_for_status()
                batch = response.json()

                if not batch:
                    break

                all_data.extend(batch)

                if len(batch) < limit:
                    break

                offset += limit

            if not all_data:
                continue

            df = json_normalize(all_data)
            df.columns = df.columns.str.replace(".", "_", regex=False)
            df = df.drop_duplicates(subset=["states", "year", "week", "label"])

            try:
                existing_query = f"""
                    SELECT DISTINCT states, year, week, label
                    FROM `{FULL_TABLE}`
                """
                existing_df = client.query(existing_query).to_dataframe()
                df = df.merge(existing_df, on=["states", "year", "week", "label"], how="left", indicator=True)
                df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
            except Exception:
                print("  Raw table does not exist yet, skipping dedup check.")

            if df.empty:
                continue

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )

            job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
            job.result()

            print(f"  Loaded year={year} week={week} — {len(df)} rows")


def upload_to_gcs(all_data):
    # Date only — same day reruns overwrite the same file
    date_stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    blob_name = f"nndss_weekly/raw_{date_stamp}.json"

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        json.dumps(all_data),
        content_type="application/json"
    )

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    print(f"Uploaded raw data to {gcs_uri}")
    return gcs_uri


def load_to_bigquery(all_data):
    if not all_data:
        print("No new data to load.")
        return

    df = json_normalize(all_data)
    df.columns = df.columns.str.replace(".", "_", regex=False)
    df = df.drop_duplicates(subset=["states", "year", "week", "label"])

    client = bigquery.Client(project=PROJECT_ID)

    # Check what's already in BigQuery
    try:
        existing_query = f"""
            SELECT DISTINCT states, year, week, label
            FROM `{FULL_TABLE}`
        """
        existing_df = client.query(existing_query).to_dataframe()

        df = df.merge(
            existing_df,
            on=["states", "year", "week", "label"],
            how="left",
            indicator=True
        )
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

        print(f"Rows after dedup against BigQuery: {len(df)}")

    except Exception:
        print("Raw table does not exist yet, skipping dedup check.")

    if df.empty:
        print("No new rows to load after dedup check.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    job = client.load_table_from_dataframe(
        df,
        FULL_TABLE,
        job_config=job_config
    )
    job.result()

    print(f"Successfully loaded {len(df)} rows to {FULL_TABLE}")
    

def create_watermark_table_if_not_exists():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_RAW}.pipeline_watermark`
        (
            year    INT64,
            week    INT64,
            loaded_at TIMESTAMP
        )
    """
    client.query(query).result()
    print("Watermark table ready.")


def get_watermark():
    client = bigquery.Client(project=PROJECT_ID)

    # First try reading from watermark table
    try:
        query = f"""
            SELECT year, week
            FROM `{PROJECT_ID}.{DATASET_RAW}.pipeline_watermark`
            ORDER BY loaded_at DESC
            LIMIT 1
        """
        result = client.query(query).result()
        for row in result:
            print(f"Watermark found: year={row.year}, week={row.week}")
            return str(row.year), str(row.week)

    except Exception as e:
        print(f"Watermark table error: {e}")

    # Single fallback — reads from raw table once
    print("Falling back to raw table...")
    try:
        fallback_query = f"""
                    SELECT year AS last_year, week AS last_week
                    FROM `{FULL_TABLE}`
                    ORDER BY CAST(year AS INT64) * 100 + CAST(week AS INT64) DESC
                    LIMIT 1
                  """
        result = client.query(fallback_query).result()
        for row in result:
            if row.last_year and row.last_week:
                print(f"Fallback watermark: year={row.last_year}, week={row.last_week}")
                return str(row.last_year), str(row.last_week)

    except Exception as e:
        print(f"Raw table error: {e}")

    print("No existing data found. Starting from 2022 week 0.")
    return "2022", "0"


def update_watermark(last_year, last_week):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.pipeline_watermark`
        (year, week, loaded_at)
        VALUES ({last_year}, {last_week}, CURRENT_TIMESTAMP())
    """
    client.query(query).result()
    print(f"Watermark updated: year={last_year}, week={last_week}")


def run():
    create_watermark_table_if_not_exists()
    last_year, last_week = get_watermark()

    # Peek at latest CDC data
    params = {
        "$limit": 1,
        "$order": "year DESC, week DESC"
    }
    response = requests.get(CDC_URL, params=params)
    response.raise_for_status()
    latest = response.json()

    if latest:
        api_year = latest[0]["year"]
        api_week = latest[0]["week"]

        api_stamp = int(api_year) * 100 + int(api_week)
        last_stamp = int(last_year) * 100 + int(last_week)

        if api_stamp <= last_stamp:
            print(f"No new data. Skipping run.")
            return

    data = fetch_cdc_data(last_year, last_week)

    if not data:
        print("No new data fetched. Skipping.")
        return

    upload_to_gcs(data)
    load_to_bigquery(data)

    # Only runs if everything above succeeded
    latest_loaded_year = max(int(r["year"]) for r in data)
    latest_loaded_week = max(int(r["week"]) for r in data)
    update_watermark(latest_loaded_year, latest_loaded_week)

if __name__ == "__main__":
    run()
