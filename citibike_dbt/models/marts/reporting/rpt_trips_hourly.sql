with trips as (

    select *
    from {{ ref('fct_trips') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

final as (

    select
        -- grain
        trips.started_date_local as date_day,
        trips.started_hour_local as hour_of_day,
        trips.rider_type,
        trips.rideable_type,

        -- calendar attributes for filtering
        dates.month_start_date,
        dates.day_name,
        dates.day_of_week,
        dates.is_weekend,

        -- additive measures
        count(*) as trips_cnt,
        countif(not trips.is_over_24h) as trips_under_24h_cnt,
        sum(if(not trips.is_over_24h, trips.duration_minutes, 0)) as duration_minutes_under_24h_sum,
        countif(trips.is_round_trip) as round_trips_cnt

    from trips
    inner join dates
        on trips.started_date_local = dates.date_day
    group by
        trips.started_date_local,
        trips.started_hour_local,
        trips.rider_type,
        trips.rideable_type,
        dates.month_start_date,
        dates.day_name,
        dates.day_of_week,
        dates.is_weekend

)

select *
from final
