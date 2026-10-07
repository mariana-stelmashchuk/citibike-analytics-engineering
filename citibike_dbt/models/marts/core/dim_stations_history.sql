with snapshot_versions as (

    select *
    from {{ ref('snp_stations') }}

),

trip_stations as (

    select *
    from {{ ref('int_stations__from_trips') }}

),

-- The snapshot starts on the first GBFS load; the first known version of each
-- station is treated as valid since the beginning of time, so historical trips
-- still find a version.
gbfs_versions as (

    select
        station_id,
        gbfs_station_id,
        station_name,
        region_id,
        capacity,
        is_charging,
        station_lat,
        station_lng,
        case
            when row_number() over (partition by station_id order by dbt_valid_from) = 1
            then timestamp('1900-01-01')
            else dbt_valid_from
        end as valid_from,
        dbt_valid_to as valid_to,
        true as is_in_gbfs
    from snapshot_versions

),

-- Stations seen in trips but never in GBFS (closed before the first snapshot):
-- one version with attributes from trips, valid for all time.
trip_only_versions as (

    select
        trip_stations.station_id,
        cast(null as string) as gbfs_station_id,
        trip_stations.station_name,
        cast(null as string) as region_id,
        cast(null as int64) as capacity,
        cast(null as boolean) as is_charging,
        trip_stations.station_lat,
        trip_stations.station_lng,
        timestamp('1900-01-01') as valid_from,
        timestamp('9999-12-31') as valid_to,
        false as is_in_gbfs
    from trip_stations
    where trip_stations.station_id not in (
        select station_id
        from snapshot_versions
    )

),

unioned as (

    select * from gbfs_versions
    union all
    select * from trip_only_versions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['station_id', 'valid_from']) }} as station_version_key,
        *,
        valid_to = timestamp('9999-12-31') as is_current
    from unioned

)

select *
from final