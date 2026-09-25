-- Mart: histogram of trip_distance_miles in 1-mile bins — answers
-- QUESTIONS.md #4 (what does the distribution look like, and where is a
-- sensible cutoff before a trip stops looking like an in-city ride?).
-- cumulative_pct lets you read off cutoffs directly, e.g. the first bin
-- where cumulative_pct >= 99. Replaces the manual trip_distance_cutoffs.png.

with binned as (
    select
        floor(trip_distance_miles)::integer   as bin_start_miles,
        count(*)                              as trip_count,
        round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
        round(avg(avg_speed_mph), 2)          as avg_speed_mph,
        round(avg(fare_amount), 2)            as avg_fare_amount
    from {{ ref('fct_trips') }}
    group by all
)

select
    bin_start_miles,
    bin_start_miles + 1                       as bin_end_miles,
    trip_count,
    round(trip_count * 100.0 / sum(trip_count) over (), 4)
                                              as pct_of_trips,
    -- running share of trips at or below this bin's upper edge
    round(sum(trip_count) over (order by bin_start_miles) * 100.0
          / sum(trip_count) over (), 4)       as cumulative_pct,
    avg_duration_minutes,
    avg_speed_mph,
    avg_fare_amount
from binned
order by bin_start_miles
