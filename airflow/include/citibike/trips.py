"""Load one month of Citi Bike trips from s3.amazonaws.com/tripdata into BigQuery."""

import shutil
import zipfile
from pathlib import Path

import duckdb
import requests
from google.cloud import bigquery

PROJECT_ID = "citibike-analytics-510413"
TABLE_ID = f"{PROJECT_ID}.raw_citibike.trips"
BASE_URL = "https://s3.amazonaws.com/tripdata"
WORK_ROOT = Path("/tmp/citibike")

# DuckDB shares the container with Airflow; keep its memory use bounded and let it
# spill to disk instead of being killed by the out-of-memory killer.
DUCKDB_MEMORY_LIMIT = "2GB"
DUCKDB_THREADS = 2


def find_month_file_url(month: str) -> str | None:
    """
    Return the download URL of the monthly file, or None if it is not published yet.
    The file extension differs between years, so both names are tried.
    """
    for file_name in (f"{month}-citibike-tripdata.zip", f"{month}-citibike-tripdata.csv.zip"):
        url = f"{BASE_URL}/{file_name}"
        response = requests.head(url, timeout=30)
        if response.status_code == 200:
            return url
    return None


def _download(url: str, target_path: Path) -> None:
    """Download a file in chunks, so a large archive never has to fit in memory."""
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with open(target_path, "wb") as target_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                target_file.write(chunk)


def _extract_all(zip_path: Path, target_dir: Path) -> None:
    """Unzip the archive, including archives nested inside it, and drop macOS metadata."""
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(target_dir)

    for nested_zip in list(target_dir.rglob("*.zip")):
        with zipfile.ZipFile(nested_zip) as archive:
            archive.extractall(nested_zip.parent)
        nested_zip.unlink()

    for macos_dir in list(target_dir.rglob("__MACOSX")):
        shutil.rmtree(macos_dir, ignore_errors=True)


def _convert_to_parquet(month: str, csv_dir: Path, parquet_path: Path, spill_dir: Path) -> None:
    """Combine all CSVs of the month into one Parquet file with an explicit schema."""
    connection = duckdb.connect()
    try:
        connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        connection.execute(f"SET threads = {DUCKDB_THREADS}")
        connection.execute(f"SET temp_directory = '{spill_dir}'")
        connection.execute("SET preserve_insertion_order = false")

        connection.execute(f"""
            COPY (
                SELECT
                    CAST(ride_id AS VARCHAR)            AS ride_id,
                    CAST(rideable_type AS VARCHAR)      AS rideable_type,
                    CAST(started_at AS TIMESTAMP)       AS started_at,
                    CAST(ended_at AS TIMESTAMP)         AS ended_at,
                    CAST(start_station_name AS VARCHAR) AS start_station_name,
                    CAST(start_station_id AS VARCHAR)   AS start_station_id,
                    CAST(end_station_name AS VARCHAR)   AS end_station_name,
                    CAST(end_station_id AS VARCHAR)     AS end_station_id,
                    CAST(start_lat AS DOUBLE)           AS start_lat,
                    CAST(start_lng AS DOUBLE)           AS start_lng,
                    CAST(end_lat AS DOUBLE)             AS end_lat,
                    CAST(end_lng AS DOUBLE)             AS end_lng,
                    CAST(member_casual AS VARCHAR)      AS member_casual,
                    regexp_extract(filename, '[^/]+$')  AS _source_file,
                    strptime('{month}', '%Y%m')::DATE   AS _batch_month,
                    current_timestamp                   AS _loaded_at
                FROM read_csv(
                    '{csv_dir}/**/*.csv',
                    header = true,
                    filename = true,
                    all_varchar = true
                )
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)
    finally:
        connection.close()


def load_trip_month(month: str) -> int:
    """
    Load one month (format YYYYMM, e.g. 202609) into its _batch_month partition.

    The partition is replaced as a whole, so rerunning the same month is safe (idempotent).
    Returns the number of rows loaded.
    """
    url = find_month_file_url(month)
    if url is None:
        raise ValueError(f"No Citi Bike trip file is published for {month} yet")

    work_dir = WORK_ROOT / month
    zip_path = work_dir / "tripdata.zip"
    csv_dir = work_dir / "csv"
    spill_dir = work_dir / "duckdb_spill"
    parquet_path = work_dir / f"{month}.parquet"

    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True)

    try:
        # 1. Download and unzip
        _download(url, zip_path)
        _extract_all(zip_path, csv_dir)

        # 2. Convert all CSVs of the month to one Parquet file
        _convert_to_parquet(month, csv_dir, parquet_path, spill_dir)

        # 3. Replace the month's partition in BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(parquet_path, "rb") as parquet_file:
            load_job = client.load_table_from_file(
                parquet_file,
                f"{TABLE_ID}${month}",
                job_config=job_config,
            )
        load_job.result()

        return load_job.output_rows

    finally:
        # 4. Always free disk space, even if a step above failed
        shutil.rmtree(work_dir, ignore_errors=True)

def get_next_month_to_load() -> str:
    """Return the month after the latest _batch_month already in the raw table (YYYYMM)."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT FORMAT_DATE('%Y%m', DATE_ADD(MAX(_batch_month), INTERVAL 1 MONTH)) AS next_month
        FROM `{TABLE_ID}`
    """
    rows = client.query(query).result()
    return next(iter(rows)).next_month