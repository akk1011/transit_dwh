WITH time_dimension AS (
    SELECT DISTINCT
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
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['datetime']) }} as time_key,
    *
FROM time_dimension