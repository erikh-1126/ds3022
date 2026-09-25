-- Singular test (QUESTIONS.md #2): basic weekday/weekend dynamics of
-- mart_dow_summary. Each check returns a row only when it fails, so the
-- test passes when the whole query returns nothing.

with m as (select * from {{ ref('mart_dow_summary') }})

select 'expected exactly 7 days of week' as failed_check
where (select count(*) from m) <> 7

union all
select 'expected exactly 2 weekend days'
where (select count(*) from m where is_weekend) <> 2

union all
-- mart must account for every trip in fct_trips, no more, no less
select 'trip_count total does not match fct_trips'
where (select sum(trip_count) from m)
   <> (select count(*) from {{ ref('fct_trips') }})

union all
-- nightlife: every weekend day should have a larger late-night share
-- than any weekday
select 'weekend late-night share not above every weekday'
where (select min(pct_late_night_trips) from m where is_weekend)
   <= (select max(pct_late_night_trips) from m where not is_weekend)
