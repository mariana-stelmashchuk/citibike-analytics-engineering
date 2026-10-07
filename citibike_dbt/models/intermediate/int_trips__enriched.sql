with trips as (

    select
        ride_id,
        start_station_id,
        end_station_id,
        rideable_type,
        rider_type,
        start_station_name,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        started_at_local,
        started_at,
        ended_at_local,
        ended_at,
        is_dst_adjusted
    from {{ ref('int_trips__deduplicated') }}

),

calculated as (

    select
        *,
        timestamp_diff(ended_at, started_at, second) / 60 as duration_minutes,
        st_distance(
            st_geogpoint(start_lng, start_lat),
            st_geogpoint(end_lng, end_lat)
        ) / 1000 as distance_km
    from trips

),

final as (

    select
        -- ids
        ride_id,
        start_station_id,
        end_station_id,

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

        -- measures
        duration_minutes,
        distance_km,

        -- flags
        end_station_id is not null as has_end_station,
        coalesce(start_station_id = end_station_id, false) as is_round_trip,
        duration_minutes > 1440 as is_over_24h,
        is_dst_adjusted,

        -- local time parts
        date(started_at_local) as started_date_local,
        date(ended_at_local) as ended_date_local,
        extract(hour from started_at_local) as started_hour_local,

        -- timestamps
        started_at_local,
        started_at,
        ended_at_local,
        ended_at

    from calculated

)

select *
from final