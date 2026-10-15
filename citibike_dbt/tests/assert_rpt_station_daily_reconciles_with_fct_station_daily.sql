-- The reporting table must keep every station-day of the fact table: an inner join to
-- dim_stations or dim_date must not drop rows.

with reporting as (

    select count(*) as rows_cnt
    from {{ ref('rpt_station_daily') }}

),

fact as (

    select count(*) as rows_cnt
    from {{ ref('fct_station_daily') }}

)

select
    reporting.rows_cnt as reporting_rows_cnt,
    fact.rows_cnt as fact_rows_cnt
from reporting
cross join fact
where reporting.rows_cnt != fact.rows_cnt
