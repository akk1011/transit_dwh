WITH stop_dimension AS (
    SELECT DISTINCT -- ensure unique combinations
        stop_id,
        FIRST_VALUE(stop_name) OVER (PARTITION BY stop_id ORDER BY datetime) as stop_name,
        FIRST_VALUE(division) OVER (PARTITION BY stop_id ORDER BY datetime) as division,
        FIRST_VALUE(north_direction_label) OVER (PARTITION BY stop_id ORDER BY datetime) as north_direction_label,
        FIRST_VALUE(south_direction_label) OVER (PARTITION BY stop_id ORDER BY datetime) as south_direction_label,
        FIRST_VALUE(near_commercial_area) OVER (PARTITION BY stop_id ORDER BY datetime) as near_commercial_area,
        FIRST_VALUE(near_university) OVER (PARTITION BY stop_id ORDER BY datetime) as near_university
    FROM {{ ref('stg_raw_transit') }}
)

SELECT * FROM stop_dimension
WHERE stop_id IS NOT NULL -- ensure no nulls