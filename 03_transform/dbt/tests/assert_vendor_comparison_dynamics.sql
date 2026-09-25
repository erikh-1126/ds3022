-- Singular test (QUESTIONS.md #10): vendor shares add up, and no single
-- vendor's metrics are wildly out of line with the fleet.

with m as (select * from {{ ref('mart_vendor_comparison') }})

select 'pct_of_trips does not sum to ~100' as failed_check
where abs((select sum(pct_of_trips) from m) - 100) > 0.1

union all
select 'trip_count total does not match fct_trips'
where (select sum(trip_count) from m)
   <> (select count(*) from {{ ref('fct_trips') }})

union all
select 'expected at least 2 vendors'
where (select count(*) from m) < 2

union all
-- vendors serve the same city under the same fare rules, so their
-- average fares should agree within 25%
select 'vendor avg fares differ by more than 25%'
where (select max(avg_fare_amount) / min(avg_fare_amount) from m) > 1.25
