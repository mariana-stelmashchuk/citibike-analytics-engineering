with trips as (

    select
        start_station_id,
        end_station_id,
        started_date_local,
        ended_date_local,
        rider_type,
        rideable_type
    from {{ ref('int_trips__enriched') }}

),

departures as (

    select
        start_station_id as station_id,
        started_date_local as date_day,
        count(*) as departures_cnt,
        countif(rider_type = 'member') as member_departures_cnt,
        countif(rider_type = 'casual') as casual_departures_cnt,
        countif(rideable_type = 'electric_bike') as electric_departures_cnt
    from trips
    where start_station_id is not null
    group by station_id, date_day

),

arrivals as (

    select
        end_station_id as station_id,
        ended_date_local as date_day,
        count(*) as arrivals_cnt,
        countif(rider_type = 'member') as member_arrivals_cnt,
        countif(rider_type = 'casual') as casual_arrivals_cnt,
        countif(rideable_type = 'electric_bike') as electric_arrivals_cnt
    from trips
    where end_station_id is not null
    group by station_id, date_day

),

joined as (

    select
        coalesce(departures.station_id, arrivals.station_id) as station_id,
        coalesce(departures.date_day, arrivals.date_day) as date_day,

        coalesce(departures.departures_cnt, 0) as departures_cnt,
        coalesce(departures.member_departures_cnt, 0) as member_departures_cnt,
        coalesce(departures.casual_departures_cnt, 0) as casual_departures_cnt,
        coalesce(departures.electric_departures_cnt, 0) as electric_departures_cnt,

        coalesce(arrivals.arrivals_cnt, 0) as arrivals_cnt,
        coalesce(arrivals.member_arrivals_cnt, 0) as member_arrivals_cnt,
        coalesce(arrivals.casual_arrivals_cnt, 0) as casual_arrivals_cnt,
        coalesce(arrivals.electric_arrivals_cnt, 0) as electric_arrivals_cnt

    from departures
    full outer join arrivals
        on departures.station_id = arrivals.station_id
        and departures.date_day = arrivals.date_day

),

final as (

    select
        *,
        arrivals_cnt - departures_cnt as net_flow
    from joined

)

select *
from final