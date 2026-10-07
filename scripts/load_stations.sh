#!/usr/bin/env bash
# Saves today's snapshot of Citi Bike station information (GBFS) into BigQuery.
# Usage: ./scripts/load_stations.sh
set -euo pipefail

PROJECT="citibike-analytics-510413"
TABLE="raw_citibike.station_information"
URL="https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

SNAPSHOT_DATE="$(date -u +%Y-%m-%d)"
PARTITION="$(date -u +%Y%m%d)"

DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/stations"
PARQUET="$DATA_DIR/station_information_$PARTITION.parquet"

mkdir -p "$DATA_DIR"

# 1. Download the feed and flatten it to one row per station
duckdb -c "
COPY (
    WITH feed AS (
        SELECT *
        FROM read_json_auto('$URL')
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
        DATE '$SNAPSHOT_DATE'           AS _snapshot_date,
        current_timestamp               AS _loaded_at
    FROM stations
) TO '$PARQUET' (FORMAT PARQUET);
"

# 2. Replace today's partition (idempotent); create the table on the first run
if bq show --quiet "$PROJECT:$TABLE" > /dev/null 2>&1; then
  bq load --replace --source_format=PARQUET \
    "$PROJECT:$TABLE\$$PARTITION" \
    "$PARQUET"
else
  bq load --source_format=PARQUET \
    --time_partitioning_field=_snapshot_date \
    --time_partitioning_type=DAY \
    "$PROJECT:$TABLE" \
    "$PARQUET"
fi

echo "Loaded station snapshot for $SNAPSHOT_DATE"