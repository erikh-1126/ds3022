-- Mart: side-by-side trip characteristics per vendor — answers
-- QUESTIONS.md #10 (do vendors differ in fare, distance, tipping, or
-- reporting patterns?). One row per vendor_id; names per the TLC 2025
-- data dictionary. The pct_* columns flag reporting differences (zero
-- tips, cash share, unmapped zones). Note: staging already drops NULL/0
-- passenger counts, so null-reporting differences upstream aren't visible.

select
    vendor_id,
    case vendor_id
        when 1 then 'Creative Mobile Technologies'
        when 2 then 'Curb Mobility'
        when 6 then 'Myle Technologies'
        when 7 then 'Helix'
    end                                   as vendor_name,
    count(*)                              as trip_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)
                                          as pct_of_trips,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(avg_speed_mph), 2)          as avg_speed_mph,
    round(avg(fare_amount), 2)            as avg_fare_amount,
    round(avg(total_amount), 2)           as avg_total_amount,
    round(avg(tip_pct) * 100, 2)          as avg_tip_pct,
    -- total tips / total fares (dollar-weighted), vs. the mean of per-trip tip %
    round(sum(tip_amount) / sum(fare_amount) * 100, 2)
                                          as fleet_tip_pct,
    round(avg(passenger_count), 2)        as avg_passenger_count,
    round(avg(case when tip_amount = 0 then 1 else 0 end) * 100, 2)
                                          as pct_zero_tip,
    round(avg(case when payment_type_code = 2 then 1 else 0 end) * 100, 2)
                                          as pct_cash,
    round(avg(case when pickup_location_id in (264, 265)
                     or dropoff_location_id in (264, 265)
                   then 1 else 0 end) * 100, 2)
                                          as pct_unmapped_zone
from {{ ref('fct_trips') }}
group by all
order by vendor_id
