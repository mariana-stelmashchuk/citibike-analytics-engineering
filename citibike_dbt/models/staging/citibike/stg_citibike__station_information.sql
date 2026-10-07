with source as (

    select *
    from {{ source('citibike', 'station_information') }}

),

renamed as (

    select
        -- ids
        short_name as station_id,
        station_id as gbfs_station_id,

        -- attributes
        name as station_name,
        region_id,
        capacity,
        is_charging,

        -- coordinates
        lat as station_lat,
        lon as station_lng,

        -- metadata
        _feed_last_updated,
        _snapshot_date,
        _loaded_at

    from source

)

select *
from renamed