-- Singular test (QUESTIONS.md #8): the corridor ranking is complete and
-- ordered, and the top 25 are a small slice of all trips (long tail).

with m as (select * from {{ ref('mart_top_corridors') }})

select 'expected exactly 25 corridors' as failed_check
where (select count(*) from m) <> 25

union all
select 'trip_count increases as rank gets worse'
where exists (
    select 1 from (
        select trip_count,
               lag(trip_count) over (order by corridor_rank) as prev
        from m
    ) where trip_count > prev
)

union all
select 'cumulative_pct is not strictly increasing'
where exists (
    select 1 from (
        select cumulative_pct,
               lag(cumulative_pct) over (order by corridor_rank) as prev
        from m
    ) where cumulative_pct <= prev
)

union all
-- if 25 zone pairs out of ~69,000 carried half the traffic, the
-- aggregation (or join) would be broken
select 'top 25 corridors carry 50% or more of all trips'
where (select max(cumulative_pct) from m) >= 50
