with stations as (

    select *
    from {{ ref('stg_citibike__station_information') }}

),

latest_snapshot as (

    select max(_snapshot_date) as snapshot_date
    from stations

)

select
    stations.station_id,
    stations.gbfs_station_id,
    stations.station_name,
    stations.region_id,
    stations.capacity,
    stations.is_charging,
    stations.station_lat,
    stations.station_lng,
    stations._snapshot_date
from stations
inner join latest_snapshot
    on stations._snapshot_date = latest_snapshot.snapshot_date