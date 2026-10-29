{% docs __overview__ %}

# Citi Bike Analytics Engineering

dbt project on public NYC Citi Bike data in BigQuery: trips from September 2025 onwards,
loaded monthly as Citi Bike publishes them, and daily snapshots of the official station list.

## Where to start

- **`fct_trips`**: one row per trip with duration, distance and trip flags.
- **`fct_station_daily`**: departures, arrivals and net flow per station per day,
  for rebalancing analysis.
- **`dim_stations`**: current state of every station; **`dim_stations_history`**:
  SCD Type 2 history for point-in-time joins.
- **`dim_date`**: calendar in New York local time.

Open the lineage graph (button in the bottom right corner) to see how data flows
from the raw sources through staging and intermediate models to these marts.

## Layers

| Layer | Purpose |
|---|---|
| staging | One model per source table: renaming, type fixes, local time to UTC |
| intermediate | Deduplication, station id repair, derived fields, station sources |
| snapshots | SCD Type 2 history of GBFS station attributes |
| marts | Public tables with enforced contracts |

Source code, design decisions and known source data issues are described in the
[repository README](https://github.com/mariana-stelmashchuk/citibike-analytics-engineering).

{% enddocs %}