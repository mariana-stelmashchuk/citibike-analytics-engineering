with source as (

    select *
    from {{ source('citibike', 'trips') }}

),

converted as (

    select
        *,
        timestamp(datetime(started_at), 'America/New_York') as started_at_utc,
        timestamp(datetime(ended_at), 'America/New_York') as ended_at_utc
    from source

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
        start_station_id,
        end_station_id,

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