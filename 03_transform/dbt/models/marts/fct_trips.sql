-- Fact table: one row per trip, with derived metrics computed once
-- here so every downstream model/query reuses the same definitions.
--
-- Incremental: the first build (or `dbt build --full-refresh`) loads every
-- parquet file; later builds append only rows from files not already in
-- this table, so adding a new month doesn't reprocess the whole year.
-- Rebuild with --full-refresh after changing staging/derivation logic or
-- replacing an already-loaded file — incremental runs won't pick those up.

{{ config(materialized='incremental') }}

with trips as (
    select * from {{ ref('stg_yellow_tripdata') }}
    {% if is_incremental() %}
    -- {{ this }} = the existing fct_trips table
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

derived as (
    select
        *,
        date_diff('second', pickup_at, dropoff_at) / 60.0   as trip_duration_minutes,
        trip_distance_miles
            / nullif(date_diff('second', pickup_at, dropoff_at) / 3600.0, 0)
                                                             as avg_speed_mph,
        case
            when fare_amount > 0 then tip_amount / fare_amount
            else null
        end                                                  as tip_pct,
        date_trunc('day', pickup_at)                        as pickup_date
    from trips
)

select *
from derived
-- guard against timestamp glitches producing absurd durations
where trip_duration_minutes between 1 and 180
