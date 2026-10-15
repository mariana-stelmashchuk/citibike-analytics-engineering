with station_daily as (

    select *
    from {{ ref('fct_station_daily') }}

),

stations as (

    select *
    from {{ ref('dim_stations') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

final as (

    select
        -- grain
        station_daily.station_id,
        station_daily.date_day,

        -- station attributes (current state)
        stations.station_name,
        stations.region_id,
        stations.capacity,
        concat(
            cast(stations.station_lat as string), ',', cast(stations.station_lng as string)
        ) as station_location,

        -- calendar attributes for filtering
        dates.month_start_date,
        dates.is_weekend,

        -- additive measures
        station_daily.departures_cnt,
        station_daily.arrivals_cnt,
        station_daily.net_flow,
        station_daily.electric_departures_cnt,
        station_daily.electric_arrivals_cnt

    from station_daily
    inner join stations
        on station_daily.station_id = stations.station_id
    inner join dates
        on station_daily.date_day = dates.date_day

)

select *
from final
