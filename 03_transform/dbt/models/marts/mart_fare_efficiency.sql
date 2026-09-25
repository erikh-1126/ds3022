-- Mart: fare per mile and per minute by trip-distance band — answers
-- QUESTIONS.md #7 (are short trips proportionally pricier per mile,
-- reflecting the flag-drop / minimum-fare structure?).
-- One row per distance band.
--
-- fleet_* columns are total fare / total miles (or minutes): robust to
-- tiny-distance trips, whose per-trip $/mile can be in the hundreds.
-- median_fare_per_mile is the typical single trip, also outlier-robust.

with banded as (
    select
        *,
        case
            when trip_distance_miles < 1  then 1
            when trip_distance_miles < 2  then 2
            when trip_distance_miles < 3  then 3
            when trip_distance_miles < 5  then 4
            when trip_distance_miles < 10 then 5
            when trip_distance_miles < 20 then 6
            else 7
        end as band_order
    from {{ ref('fct_trips') }}
)

select
    band_order,
    case band_order
        when 1 then '0-1 mi'
        when 2 then '1-2 mi'
        when 3 then '2-3 mi'
        when 4 then '3-5 mi'
        when 5 then '5-10 mi'
        when 6 then '10-20 mi'
        else '20+ mi'
    end                                   as distance_band,
    count(*)                              as trip_count,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(fare_amount), 2)            as avg_fare_amount,
    round(median(fare_amount / trip_distance_miles), 2)
                                          as median_fare_per_mile,
    round(sum(fare_amount) / sum(trip_distance_miles), 2)
                                          as fleet_fare_per_mile,
    round(sum(fare_amount) / sum(trip_duration_minutes), 2)
                                          as fleet_fare_per_minute
from banded
group by all
order by band_order
