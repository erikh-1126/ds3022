-- Mart: trip volume and averages by day of week — answers QUESTIONS.md #2
-- (do counts, distance, and fare shift between weekdays and weekends?).
-- One row per day of week. avg_trips_per_day normalizes for the fact
-- that some weekdays occur 53 times in a year and others 52.

-- note: DuckDB's dayofweek() is 0 = Sunday ... 6 = Saturday
select
    dayofweek(pickup_at)                  as pickup_dow,
    dayname(pickup_at)                    as pickup_day_name,
    dayofweek(pickup_at) in (0, 6)        as is_weekend,
    count(*)                              as trip_count,
    count(distinct pickup_date)           as day_count,
    round(count(*) / count(distinct pickup_date), 0)
                                          as avg_trips_per_day,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(fare_amount), 2)            as avg_fare_amount,
    round(avg(total_amount), 2)           as avg_total_amount,
    round(avg(tip_pct) * 100, 2)          as avg_tip_pct,
    -- total tips / total fares (dollar-weighted), vs. the mean of per-trip tip %
    round(sum(tip_amount) / sum(fare_amount) * 100, 2)
                                          as fleet_tip_pct,
    -- share of the day's trips starting 22:00-04:59 (late-night skew)
    round(avg(case when hour(pickup_at) >= 22 or hour(pickup_at) < 5
                   then 1 else 0 end) * 100, 2)
                                          as pct_late_night_trips
from {{ ref('fct_trips') }}
group by all
order by pickup_dow
