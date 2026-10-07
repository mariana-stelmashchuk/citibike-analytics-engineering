with gbfs_stations as (

    select *
    from {{ ref('int_stations__current') }}

),

trip_stations as (

    select *
    from {{ ref('int_stations__from_trips') }}

),

final as (

    select
        -- ids
        coalesce(gbfs_stations.station_id, trip_stations.station_id) as station_id,
        gbfs_stations.gbfs_station_id,

        -- attributes
        coalesce(gbfs_stations.station_name, trip_stations.station_name) as station_name,
        gbfs_stations.region_id,
        gbfs_stations.capacity,
        gbfs_stations.is_charging,

        -- coordinates
        coalesce(gbfs_stations.station_lat, trip_stations.station_lat) as station_lat,
        coalesce(gbfs_stations.station_lng, trip_stations.station_lng) as station_lng,

        -- flags
        gbfs_stations.station_id is not null as is_in_gbfs,

        -- timestamps
        trip_stations.first_trip_at,
        trip_stations.last_trip_at

    from gbfs_stations
    full outer join trip_stations
        on gbfs_stations.station_id = trip_stations.station_id

)

select *
from final