#!/usr/bin/env bash
# Loads one month of Citi Bike trips into BigQuery.
# Usage: ./scripts/load_month.sh 202509
set -euo pipefail

MONTH="$1"
PROJECT="citibike-analytics-510413"
TABLE="raw_citibike.trips"
BASE_URL="https://s3.amazonaws.com/tripdata"

DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
WORK_DIR="$DATA_DIR/$MONTH"
ZIP="$DATA_DIR/$MONTH-citibike-tripdata.zip"
PARQUET="$DATA_DIR/parquet/$MONTH.parquet"

mkdir -p "$WORK_DIR" "$DATA_DIR/parquet"

# 1. Download (file extension differs between years)
if [ ! -f "$ZIP" ]; then
  curl -fsSL -o "$ZIP" "$BASE_URL/$MONTH-citibike-tripdata.zip" \
    || curl -fsSL -o "$ZIP" "$BASE_URL/$MONTH-citibike-tripdata.csv.zip"
fi

# 2. Unzip, including nested archives; drop macOS metadata folders
unzip -oq "$ZIP" -d "$WORK_DIR"
find "$WORK_DIR" -name "*.zip" -exec unzip -oq {} -d "$WORK_DIR" \;
rm -rf "$WORK_DIR/__MACOSX"

# 3. Convert all CSVs of the month to one Parquet file with an explicit schema
duckdb -c "
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
        regexp_extract(filename, '[^/]+\$') AS _source_file,
        strptime('$MONTH', '%Y%m')::DATE   AS _batch_month,
        current_timestamp                   AS _loaded_at
    FROM read_csv(
        '$WORK_DIR/**/*.csv',
        header = true,
        filename = true,
        all_varchar = true
    )
) TO '$PARQUET' (FORMAT PARQUET);
"

# 4. Replace the month's partition (idempotent)
bq load --replace --source_format=PARQUET \
  "$PROJECT:$TABLE\$$MONTH" \
  "$PARQUET"

# 5. Free disk space
rm -rf "$WORK_DIR" "$ZIP"

echo "Loaded $MONTH"