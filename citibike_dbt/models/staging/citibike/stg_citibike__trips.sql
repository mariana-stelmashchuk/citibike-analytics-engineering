with source as (

    select *
    from {{ source('citibike', 'trips') }}

),

-- Station ids in the canonical two-decimal format (e.g. 5785.10).
-- Used to repair ids that lost a trailing zero in some source files (e.g. 5785.1).
known_station_ids as (

    select start_station_id as station_id
    from source
    where regexp_contains(start_station_id, r'^\d+\.\d{2}$')

    union distinct

    select end_station_id as station_id
    from source
    where regexp_contains(end_station_id, r'^\d+\.\d{2}$')

),

-- Some trips appear in two monthly files (April 2026 file was cut by start date).
-- Keep the copy from the most recent file.
deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by ride_id
        order by _batch_month desc, _source_file desc
    ) = 1

),

converted as (

    select
        *,
        timestamp(datetime(started_at), 'America/New_York') as started_at_utc,
        timestamp(datetime(ended_at), 'America/New_York') as ended_at_utc
    from deduplicated

),

-- On the day DST ends, 1:00-2:00 local time happens twice and the source has no offset.
-- A trip that starts in the first 1:xx and ends in the second one gets a negative duration
-- after conversion; shift its end by one hour. Only gaps under one hour are treated this way.
dst_adjusted as (

    select
        *,
        ended_at_utc < started_at_utc
            and timestamp_diff(started_at_utc, ended_at_utc, second) < 3600 as is_dst_adjusted
    from converted

),

renamed as (

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
        member_casual as rider_type,
        start_station_name,
        end_station_name,

        -- coordinates
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        -- timestamps: source holds New York wall-clock time labeled as UTC
        datetime(started_at) as started_at_local,
        started_at_utc as started_at,
        datetime(ended_at) as ended_at_local,
        case
            when is_dst_adjusted then timestamp_add(ended_at_utc, interval 1 hour)
            else ended_at_utc
        end as ended_at,

        -- flags
        is_dst_adjusted,

        -- metadata
        _source_file,
        _batch_month,
        _loaded_at

    from dst_adjusted

)

select *
from renamed