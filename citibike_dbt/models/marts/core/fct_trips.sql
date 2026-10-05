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

    -- local time parts
    started_date_local,
    started_hour_local,

    -- timestamps
    started_at_local,
    started_at,
    ended_at_local,
    ended_at

from {{ ref('int_trips__enriched') }}