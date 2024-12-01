-- models/facts/fact_stop_utilization.sql
WITH fact_stop_utilization AS (
    SELECT 
        t.time_key,
        s.stop_id,
        CASE 
            WHEN r.capacity > 0 THEN (r.rider_count::FLOAT / r.capacity) 
            ELSE NULL 
        END as utilization_rate,
        r.capacity as scheduled_capacity,
        r.rider_count as actual_usage,
        CASE 
            WHEN r.peak_off_peak = 'Peak' THEN TRUE 
            ELSE FALSE 
        END as is_peak
    FROM {{ ref('stg_raw_transit') }} r
    JOIN {{ ref('dim_time') }} t 
        ON r.datetime = t.datetime
    JOIN {{ ref('dim_stops') }} s 
        ON r.stop_id = s.stop_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY time_key, stop_id) as fact_id,
    *
FROM fact_stop_utilization