-- Mart: the 25 busiest pickup -> dropoff zone pairs — answers
-- QUESTIONS.md #8 (do a handful of routes dominate, or is it a long tail?).
-- pct_of_trips / cumulative_pct are shares of ALL trips, so the last
-- row's cumulative_pct shows how much of total volume the top 25 carry.
-- Excludes zones 264/265 (Unknown / Outside of NYC), matching
-- mart_location_rankings. Ties broken by location ids for a stable rank.

with pairs as (
    select
        pickup_location_id,
        dropoff_location_id,
        count(*)                              as trip_count,
        round(avg(trip_distance_miles), 2)    as avg_distance_miles,
        round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
        round(avg(fare_amount), 2)            as avg_fare_amount
    from {{ ref('fct_trips') }}
    where pickup_location_id not in (264, 265)
      and dropoff_location_id not in (264, 265)
    group by all
),

ranked as (
    select
        *,
        row_number() over (
            order by trip_count desc, pickup_location_id, dropoff_location_id
        )                                     as corridor_rank,
        sum(trip_count) over ()               as total_trips
    from pairs
)

select
    r.corridor_rank,
    r.pickup_location_id,
    pz.borough                                as pickup_borough,
    pz.zone                                   as pickup_zone,
    r.dropoff_location_id,
    dz.borough                                as dropoff_borough,
    dz.zone                                   as dropoff_zone,
    r.pickup_location_id = r.dropoff_location_id
                                              as is_same_zone,
    r.trip_count,
    round(r.trip_count * 100.0 / r.total_trips, 3)
                                              as pct_of_trips,
    round(sum(r.trip_count) over (order by r.corridor_rank) * 100.0
          / r.total_trips, 3)                 as cumulative_pct,
    r.avg_distance_miles,
    r.avg_duration_minutes,
    r.avg_fare_amount
from ranked r
left join {{ ref('stg_taxi_zones') }} pz on pz.location_id = r.pickup_location_id
left join {{ ref('stg_taxi_zones') }} dz on dz.location_id = r.dropoff_location_id
where r.corridor_rank <= 25
order by r.corridor_rank
