
WITH split_routes AS (
    SELECT DISTINCT
        stop_id,
        TRIM(value) as daytime_route
    FROM {{ ref('stg_raw_transit') }},
    TABLE(SPLIT_TO_TABLE(daytime_routes, ','))
),
joined_routes AS (
    SELECT DISTINCT
        s.stop_id,
        l.line_id
    FROM split_routes s
    JOIN {{ ref('dim_lines') }} l 
        ON TRIM(l.line) = TRIM(s.daytime_route)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stop_id', 'line_id']) }} as route_id,
    stop_id,
    line_id
FROM joined_routes
WHERE stop_id IS NOT NULL 
    AND line_id IS NOT NULL