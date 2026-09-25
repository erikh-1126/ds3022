-- Mart: tipping behavior by payment type — answers QUESTIONS.md #6
-- (do cash trips show far lower recorded tips than card trips?). Cash
-- tips are paid off-meter, so TLC data records them as ~$0.
-- One row per payment_type_code; names per the TLC 2025 data dictionary.

select
    payment_type_code,
    case payment_type_code
        when 0 then 'Flex Fare'
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end                                   as payment_type_name,
    count(*)                              as trip_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)
                                          as pct_of_trips,
    round(avg(tip_amount), 2)             as avg_tip_amount,
    round(avg(tip_pct) * 100, 2)          as avg_tip_pct,
    -- total tips / total fares (dollar-weighted), vs. the mean of per-trip tip %
    round(sum(tip_amount) / sum(fare_amount) * 100, 2)
                                          as fleet_tip_pct,
    round(avg(case when tip_amount > 0 then 1 else 0 end) * 100, 2)
                                          as pct_trips_with_tip,
    round(avg(fare_amount), 2)            as avg_fare_amount
from {{ ref('fct_trips') }}
group by all
order by payment_type_code
