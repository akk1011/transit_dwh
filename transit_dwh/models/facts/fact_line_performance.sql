-- models/facts/fact_line_performance.sql
WITH fact_line_performance AS (
    SELECT 
        t.time_key,
        l.line_id,
        r.schedule_on_time as schedule_status,
        r.trip_timing,
        CASE 
            WHEN r.schedule_on_time = 'Delayed' 
            THEN FLOOR(r.trip_timing)
            ELSE 0 
        END as delay_minutes,
        CASE 
            WHEN r.schedule_on_time = 'On-Time' THEN 1.0 
            ELSE 0.0 
        END as on_time_rate,
        r.capacity as estimated_capacity
    FROM {{ ref('stg_raw_transit') }} r
    JOIN {{ ref('dim_time') }} t 
        ON r.datetime = t.datetime
    JOIN {{ ref('dim_lines') }} l 
        ON r.line_id = l.line_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY time_key, line_id) as fact_id,
    *
FROM fact_line_performance