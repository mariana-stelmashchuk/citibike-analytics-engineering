{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': '_batch_month',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = ['started_date_local', 'start_station_id'],
        on_schema_change = 'fail'
    )
}}

{#- Partitions follow source files, not trip dates: a trip date can span two files,
    and insert_overwrite must always rewrite whole partitions. -#}
{% if is_incremental() and execute %}
    {% set max_batch_month = run_query('select max(_batch_month) from ' ~ this).columns[0][0] %}
{% endif %}

select
    -- ids
    ride_id,
    start_station_id,
    end_station_id,

    -- attributes
    rideable_type,
    rider_type,

    -- measures
    duration_minutes,
    distance_km,

    -- flags
    has_end_station,
    is_round_trip,
    is_over_24h,
    is_dst_adjusted,

    -- local time parts
    started_date_local,
    started_hour_local,

    -- timestamps
    started_at_local,
    started_at,
    ended_at_local,
    ended_at,

    -- metadata
    _batch_month

from {{ ref('int_trips__enriched') }}
{% if is_incremental() %}
where _batch_month >= date '{{ max_batch_month }}'
{% endif %}