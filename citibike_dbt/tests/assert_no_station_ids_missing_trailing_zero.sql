-- Fails if any station id with one decimal still has a two-decimal twin,
-- i.e. the trailing-zero repair in stg_citibike__trips missed something.
with station_ids as (

    select start_station_id as station_id
    from {{ ref('stg_citibike__trips') }}
    where start_station_id is not null

    union distinct

    select end_station_id as station_id
    from {{ ref('stg_citibike__trips') }}
    where end_station_id is not null

)

select short_ids.station_id
from station_ids as short_ids
inner join station_ids as full_ids
    on concat(short_ids.station_id, '0') = full_ids.station_id
where regexp_contains(short_ids.station_id, r'^\d+\.\d$')д