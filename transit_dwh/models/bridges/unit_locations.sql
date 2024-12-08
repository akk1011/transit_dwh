-- models/bridges/unit_locations.sql
WITH unit_locations AS (
    SELECT DISTINCT
        stop_id,
        remote_unit_id
    FROM {{ ref('stg_raw_transit') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stop_id', 'remote_unit_id']) }} as location_id,
    stop_id,
    remote_unit_id
FROM unit_locations
WHERE stop_id IS NOT NULL 
    AND remote_unit_id IS NOT NULL