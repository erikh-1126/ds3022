-- Staging: TLC taxi zone lookup (LocationID -> Borough, Zone).
-- Materialized as a table so the CSV is fetched over HTTPS once per
-- build, not on every downstream query. Keeps all 265 IDs (including
-- 264 = Unknown, 265 = Outside of NYC) so trip location IDs always join.
-- (Overrides the staging folder's default of view in dbt_project.yml.)

{{ config(materialized='table') }}

select
    cast(locationid as integer) as location_id,
    borough,
    zone,
    service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
