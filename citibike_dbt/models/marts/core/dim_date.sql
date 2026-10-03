with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2025-01-01' as date)",
            end_date="date_add(current_date('America/New_York'), interval 1 year)"
        )
    }}

),

final as (

    select
        cast(date_day as date) as date_day,

        -- calendar parts
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        extract(isoweek from date_day) as iso_week,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,

        -- flags
        extract(dayofweek from date_day) in (1, 7) as is_weekend,

        -- period starts
        date_trunc(cast(date_day as date), month) as month_start_date,
        date_trunc(cast(date_day as date), isoweek) as week_start_date

    from date_spine

)

select *
from final