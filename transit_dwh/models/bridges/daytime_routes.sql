-- SKIPPING DAYTIME ROUTES
/*
WITH normalized_routes AS (
    SELECT DISTINCT
        stop_id,
        TRIM(REGEXP_REPLACE(daytime_routes, '\\s+', '')) AS clean_daytime_routes
    FROM {{ ref('stg_raw_transit') }}
    WHERE daytime_routes IS NOT NULL
),
split_routes AS (
    SELECT 
        stop_id,
        TRIM(value) AS route_code
    FROM normalized_routes,
         LATERAL FLATTEN(INPUT => SPLIT(clean_daytime_routes, ','))
),
joined_routes AS (
    SELECT DISTINCT
        s.stop_id,
        l.LINE_ID AS line_id
    FROM split_routes s
    JOIN {{ ref('dim_lines') }} l ON UPPER(s.route_code) = UPPER(l.line)
    WHERE s.stop_id IS NOT NULL
      AND l.LINE_ID IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY stop_id, line_id) AS route_id,
    stop_id,
    line_id
FROM joined_routes
