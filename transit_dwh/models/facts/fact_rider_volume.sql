-- models/facts/fact_rider_volume.sql
WITH fact_rider_volume AS (
    SELECT 
        t.time_key,
        s.stop_id,
        l.line_id,
        r.remote_unit_id,
        r.rider_count,
        r.entries,
        r.exits,
        r.capacity,
        CASE 
            WHEN r.capacity > 0 THEN (r.rider_count::FLOAT / r.capacity) 
            ELSE NULL 
        END as utilization_rate
    FROM {{ ref('stg_raw_transit') }} r
    JOIN {{ ref('dim_time') }} t 
        ON r.datetime = t.datetime
    JOIN {{ ref('dim_stops') }} s 
        ON r.stop_id = s.stop_id
    JOIN {{ ref('dim_lines') }} l 
        ON r.line_id = l.line_id
    JOIN {{ ref('dim_remote_units') }} ru 
        ON r.remote_unit_id = ru.remote_unit_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY time_key, stop_id, line_id) as fact_id,
    *
FROM fact_rider_volume