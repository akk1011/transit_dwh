WITH time_dimension AS (
    SELECT DISTINCT -- Get unique time records
        datetime,
        hour,
        day,
        month,
        year,
        CASE 
            WHEN day IN ('Saturday', 'Sunday') THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        CASE 
            WHEN Peak_Off_Peak = 'Peak' THEN TRUE 
            ELSE FALSE 
        END as is_peak_hour,
        CASE 
            WHEN day IN ('Saturday', 'Sunday') THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type
    FROM {{ ref('stg_raw_transit') }}
),
numbered AS (
    -- Add a row number to create unique keys
    SELECT 
        ROW_NUMBER() OVER (ORDER BY datetime) as time_key,
        *
    FROM time_dimension
)

SELECT * FROM numbered