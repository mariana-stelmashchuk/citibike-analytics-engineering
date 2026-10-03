with trips as (

    select
        start_station_id,
        end_station_id,
        start_station_name,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        started_at,
        ended_at
    from {{ ref('stg_citibike__trips') }}

),

station_events as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        start_lat as station_lat,
        start_lng as station_lng,
        started_at as event_at
    from trips
    where start_station_id is not null

    union all

    select
        end_station_id as station_id,
        end_station_name as station_name,
        end_lat as station_lat,
        end_lng as station_lng,
        ended_at as event_at
    from trips
    where end_station_id is not null

),

final as (

    select
        station_id,
        station_name,
        station_lat,
        station_lng,
        min(event_at) over (partition by station_id) as first_trip_at,
        max(event_at) over (partition by station_id) as last_trip_at
    from station_events
    qualify row_number() over (
        partition by station_id
        order by event_at desc, station_name
    ) = 1

)

select *
from final