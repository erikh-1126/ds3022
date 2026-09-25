-- Mart: trip characteristics by passenger_count — answers QUESTIONS.md #9
-- (are most trips solo, and does group size track distance, time of day,
-- or tipping?). One row per passenger_count. Note: staging drops rows
-- with NULL or 0 passengers, so this covers only trips that report 1+.

select
    passenger_count,
    count(*)                              as trip_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)
                                          as pct_of_trips,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(fare_amount), 2)            as avg_fare_amount,
    round(avg(tip_pct) * 100, 2)          as avg_tip_pct,
    -- total tips / total fares (dollar-weighted), vs. the mean of per-trip tip %
    round(sum(tip_amount) / sum(fare_amount) * 100, 2)
                                          as fleet_tip_pct,
    -- time-of-day / week mix: do groups skew to nights and weekends?
    round(avg(case when hour(pickup_at) >= 22 or hour(pickup_at) < 5
                   then 1 else 0 end) * 100, 2)
                                          as pct_late_night_trips,
    round(avg(case when dayofweek(pickup_at) in (0, 6)
                   then 1 else 0 end) * 100, 2)
                                          as pct_weekend_trips
from {{ ref('fct_trips') }}
group by all
order by passenger_count
