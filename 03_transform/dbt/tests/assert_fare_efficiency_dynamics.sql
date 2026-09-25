-- Singular test (QUESTIONS.md #7): the flag-drop / minimum fare means
-- $/mile should fall steadily as trips get longer.

with m as (select * from {{ ref('mart_fare_efficiency') }})

select 'expected exactly 7 distance bands' as failed_check
where (select count(*) from m) <> 7

union all
select 'trip_count total does not match fct_trips'
where (select sum(trip_count) from m)
   <> (select count(*) from {{ ref('fct_trips') }})

union all
select 'fleet_fare_per_mile does not fall with every longer band'
where exists (
    select 1 from (
        select fleet_fare_per_mile,
               lag(fleet_fare_per_mile) over (order by band_order) as prev
        from m
    ) where fleet_fare_per_mile >= prev
)

union all
select 'median_fare_per_mile does not fall with every longer band'
where exists (
    select 1 from (
        select median_fare_per_mile,
               lag(median_fare_per_mile) over (order by band_order) as prev
        from m
    ) where median_fare_per_mile >= prev
)
