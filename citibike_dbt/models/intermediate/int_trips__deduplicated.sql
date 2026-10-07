{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': '_batch_month',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = ['start_station_id'],
        on_schema_change = 'fail'
    )
}}

{#- On incremental runs, find the latest batch already processed.
    It is fetched as a constant so BigQuery can prune partitions. -#}
{% if is_incremental() and execute %}
    {% set max_batch_month = run_query('select max(_batch_month) from ' ~ this).columns[0][0] %}
{% endif %}

with trips as (

    select *
    from {{ ref('stg_citibike__trips') }}
    {% if is_incremental() %}
    -- The latest stored batch is reprocessed together with any newer ones, so duplicates
    -- between it and the next file are resolved and both partitions are rewritten.
    where _batch_month >= date '{{ max_batch_month }}'
    {% endif %}

),

-- Station ids in the canonical two-decimal format (e.g. 5785.10).
-- Used to repair ids that lost a trailing zero in some source files (e.g. 5785.1).
-- On incremental runs, ids already stored in this table are included too.
known_station_ids as (

    select start_station_id as station_id
    from trips
    where regexp_contains(start_station_id, r'^\d+\.\d{2}$')

    union distinct

    select end_station_id as station_id
    from trips
    where regexp_contains(end_station_id, r'^\d+\.\d{2}$')

    {% if is_incremental() %}
    union distinct

    select start_station_id as station_id
    from {{ this }}
    where regexp_contains(start_station_id, r'^\d+\.\d{2}$')

    union distinct

    select end_station_id as station_id
    from {{ this }}
    where regexp_contains(end_station_id, r'^\d+\.\d{2}$')
    {% endif %}

),

-- Some trips appear in two adjacent monthly files (April 2026 file was cut by start date).
-- Keep the copy from the most recent file.
deduplicated as (

    select *
    from trips
    qualify row_number() over (
        partition by ride_id
        order by _batch_month desc, _source_file desc
    ) = 1

),

final as (

    select
        -- ids
        ride_id,
        case
            when regexp_contains(start_station_id, r'^\d+\.\d$')
                and concat(start_station_id, '0') in (select station_id from known_station_ids)
            then concat(start_station_id, '0')
            else start_station_id
        end as start_station_id,
        case
            when regexp_contains(end_station_id, r'^\d+\.\d$')
                and concat(end_station_id, '0') in (select station_id from known_station_ids)
            then concat(end_station_id, '0')
            else end_station_id
        end as end_station_id,

        -- attributes
        rideable_type,
        rider_type,
        start_station_name,
        end_station_name,

        -- coordinates
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        -- timestamps
        started_at_local,
        started_at,
        ended_at_local,
        ended_at,

        -- flags
        is_dst_adjusted,

        -- metadata
        _source_file,
        _batch_month,
        _loaded_at

    from deduplicated

)

select *
from final