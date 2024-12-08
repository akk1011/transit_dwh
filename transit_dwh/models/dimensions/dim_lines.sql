WITH line_dimension AS (
    SELECT DISTINCT -- ensure unique combinations
        line_id,
        FIRST_VALUE(line) OVER (PARTITION BY line_id ORDER BY datetime) as line,
        FIRST_VALUE(division) OVER (PARTITION BY line_id ORDER BY datetime) as division
    FROM {{ ref('stg_raw_transit') }}
)

SELECT * FROM line_dimension
WHERE line_id IS NOT NULL -- ensure no nulls