-- models/bridges/connecting_lines.sql
WITH split_lines AS (
    SELECT DISTINCT
        stop_id,
        TRIM(value) as connected_line
    FROM {{ ref('stg_raw_transit') }},
    TABLE(SPLIT_TO_TABLE(connecting_lines, ','))
),
joined_lines AS (
    SELECT DISTINCT
        s.stop_id,
        l.line_id
    FROM split_lines s
    JOIN {{ ref('dim_lines') }} l 
        ON TRIM(l.line) = TRIM(s.connected_line)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stop_id', 'line_id']) }} as connection_id,
    stop_id,
    line_id
FROM joined_lines
WHERE stop_id IS NOT NULL 
    AND line_id IS NOT NULL