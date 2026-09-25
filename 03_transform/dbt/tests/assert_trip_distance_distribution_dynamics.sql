-- Singular test (QUESTIONS.md #4): the distance histogram is complete,
-- its cumulative share is well-formed, and it has the expected shape
-- (dominated by short in-city trips, with a thin long tail).

with m as (select * from {{ ref('mart_trip_distance_distribution') }})

select 'trip_count total does not match fct_trips' as failed_check
where (select sum(trip_count) from m)
   <> (select count(*) from {{ ref('fct_trips') }})

union all
select 'cumulative_pct decreases between bins'
where exists (
    select 1 from (
        select cumulative_pct,
               lag(cumulative_pct) over (order by bin_start_miles) as prev_pct
        from m
    ) where cumulative_pct < prev_pct
)

union all
select 'cumulative_pct does not end at 100'
where abs((select max(cumulative_pct) from m) - 100) > 0.001

union all
-- the most common trip should be a short one (under 3 miles)
select 'most common distance bin is not under 3 miles'
where (select arg_max(bin_start_miles, trip_count) from m) >= 3

union all
-- at least 99% of trips should be under 30 miles
select 'fewer than 99% of trips are under 30 miles'
where (select max(cumulative_pct) from m where bin_start_miles < 30) < 99
