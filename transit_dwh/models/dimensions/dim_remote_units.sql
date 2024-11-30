WITH remote_unit_dimension AS (
    SELECT DISTINCT
        remote_unit_id,
        remote_unit
    FROM {{ ref('stg_raw_transit') }}
)

SELECT *
FROM remote_unit_dimension