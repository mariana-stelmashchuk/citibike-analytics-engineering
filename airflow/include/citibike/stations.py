"""Load a daily snapshot of Citi Bike station information (GBFS) into BigQuery."""

from datetime import datetime, timezone

import duckdb
from google.cloud import bigquery

PROJECT_ID = "citibike-analytics-510413"
TABLE_ID = f"{PROJECT_ID}.raw_citibike.station_information"
FEED_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"


def load_station_snapshot() -> int:
    """
    Download the current GBFS station list and replace today's partition in BigQuery.

    The snapshot date is the date the feed was fetched (UTC), not the Airflow logical date:
    GBFS only exposes the current state, so a snapshot cannot be taken for a past date.
    Rerunning on the same day replaces the same partition, so the load is idempotent.

    Returns the number of rows loaded.
    """
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    partition = snapshot_date.replace("-", "")
    parquet_path = f"/tmp/station_information_{partition}.parquet"

    # 1. Download the feed and flatten it to one row per station
    duckdb.sql(f"""
        COPY (
            WITH feed AS (
                SELECT *
                FROM read_json_auto('{FEED_URL}')
            ),

            stations AS (
                SELECT
                    unnest(data.stations) AS s,
                    last_updated
                FROM feed
            )

            SELECT
                CAST(s.station_id AS VARCHAR)   AS station_id,
                CAST(s.short_name AS VARCHAR)   AS short_name,
                CAST(s.name AS VARCHAR)         AS name,
                CAST(s.lat AS DOUBLE)           AS lat,
                CAST(s.lon AS DOUBLE)           AS lon,
                CAST(s.region_id AS VARCHAR)    AS region_id,
                CAST(s.capacity AS BIGINT)      AS capacity,
                CAST(s.is_charging AS BOOLEAN)  AS is_charging,
                to_timestamp(last_updated)      AS _feed_last_updated,
                DATE '{snapshot_date}'          AS _snapshot_date,
                current_timestamp               AS _loaded_at
            FROM stations
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)

    # 2. Replace today's partition in BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(parquet_path, "rb") as parquet_file:
        load_job = client.load_table_from_file(
            parquet_file,
            f"{TABLE_ID}${partition}",
            job_config=job_config,
        )

    load_job.result()  # wait for the load to finish; raises an error if it failed

    return load_job.output_rows