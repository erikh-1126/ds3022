-- Singular test (QUESTIONS.md #9): solo riders should dominate, and the
-- mart should account for every trip.

with m as (select * from {{ ref('mart_passenger_count') }})

select 'pct_of_trips does not sum to ~100' as failed_check
where abs((select sum(pct_of_trips) from m) - 100) > 0.1

union all
select 'trip_count total does not match fct_trips'
where (select sum(trip_count) from m)
   <> (select count(*) from {{ ref('fct_trips') }})

union all
select 'solo riders are not a majority of trips'
where coalesce((select pct_of_trips from m where passenger_count = 1), 0) <= 50

union all
-- trips with 1-4 passengers (a normal sedan) should be nearly all trips
select '1-4 passengers are under 95% of trips'
where (select sum(pct_of_trips) from m where passenger_count <= 4) < 95
