-- Mart: trip departures (pickups) per taxi zone — one row per zone, all 265.
--
-- Starts from the zone lookup so zones with zero pickups still appear with
-- trip_count = 0. Non-borough IDs (1 = EWR, 264 = Unknown, 265 = Outside of
-- NYC) are kept here; filter them downstream if you only want the 5 boroughs.

{{ config(materialized='view') }}

with pickups as (
    select pickup_location_id as location_id, count(*) as trip_count
    from {{ ref('fct_trips') }}
    group by all
)

select
    zones.location_id,
    zones.borough                                                as borough,
    zones.zone                                                   as zone,
    zones.service_zone                                           as service_zone,
    coalesce(pickups.trip_count, 0)                              as trip_count,
    round(100.0 * coalesce(pickups.trip_count, 0)
          / sum(coalesce(pickups.trip_count, 0)) over (), 4)     as pct_of_trips
from {{ ref('stg_taxi_zones') }} as zones
left join pickups using (location_id)
order by zones.location_id
