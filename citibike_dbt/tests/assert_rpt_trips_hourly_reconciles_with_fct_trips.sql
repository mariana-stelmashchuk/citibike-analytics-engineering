-- The reporting aggregate must contain exactly the same number of trips as the fact table:
-- no trips lost or duplicated by the aggregation or the join to dim_date.

with reporting as (

    select sum(trips_cnt) as trips_cnt
    from {{ ref('rpt_trips_hourly') }}

),

fact as (

    select count(*) as trips_cnt
    from {{ ref('fct_trips') }}

)

select
    reporting.trips_cnt as reporting_trips_cnt,
    fact.trips_cnt as fact_trips_cnt
from reporting
cross join fact
where reporting.trips_cnt != fact.trips_cnt
