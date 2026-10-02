with source as (

    select *
    from {{ source('citibike', 'trips') }}

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

        -- timestamps
        datetime(started_at) as started_at_local,
        timestamp(datetime(started_at), 'America/New_York') as started_at,
        datetime(ended_at) as ended_at_local,
        timestamp(datetime(ended_at), 'America/New_York') as ended_at,

        -- metadata
        _source_file,
        _loaded_at

    from source

)

select *
from renamed