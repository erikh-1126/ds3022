-- Singular test (QUESTIONS.md #6): cash tips are paid off-meter, so the
-- data should show near-zero cash tipping and substantial card tipping.

with m as (select * from {{ ref('mart_tips_by_payment_type') }})

select 'pct_of_trips does not sum to ~100' as failed_check
where abs((select sum(pct_of_trips) from m) - 100) > 0.1

union all
select 'credit card trips missing, or card tip rate below 10%'
where coalesce((select fleet_tip_pct from m where payment_type_code = 1), 0) < 10

union all
select 'cash trips record more than 1% tips'
where (select fleet_tip_pct from m where payment_type_code = 2) >= 1

union all
select 'credit card is not the most common payment type'
where (select arg_max(payment_type_code, trip_count) from m) <> 1
