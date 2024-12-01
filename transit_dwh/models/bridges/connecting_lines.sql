WITH normalized_lines AS (
    SELECT DISTINCT
        stop_id,
        TRIM(REGEXP_REPLACE(connecting_lines, '\\s+', '')) AS clean_connecting_lines
    FROM {{ ref('stg_raw_transit') }}
    WHERE connecting_lines IS NOT NULL
),
split_lines AS (
    SELECT 
        stop_id,
        TRIM(value) AS line_code
    FROM normalized_lines,
         LATERAL FLATTEN(INPUT => SPLIT(clean_connecting_lines, ','))
),
joined_lines AS (
    SELECT DISTINCT
        s.stop_id,
        l.LINE_ID AS line_id
    FROM split_lines s
    JOIN {{ ref('dim_lines') }} l ON UPPER(s.line_code) = UPPER(l.line_ID)
    WHERE s.stop_id IS NOT NULL
      AND l.LINE_ID IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY stop_id, line_id) AS connection_id,
    stop_id,
    line_id
FROM joined_lines
