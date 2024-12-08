-- models/facts/fact_rider_segments.sql
WITH fact_rider_segments AS (
    SELECT 
        t.time_key,
        s.stop_id,
        r.demographic,
        r.travel_pattern,
        r.rider_count as segment_count,
        CASE 
            WHEN r.near_commercial_area THEN 'Commercial'
            WHEN r.near_university THEN 'University'
            ELSE 'Regular'
        END as location_type
    FROM {{ ref('stg_raw_transit') }} r
    JOIN {{ ref('dim_time') }} t 
        ON r.datetime = t.datetime
    JOIN {{ ref('dim_stops') }} s 
        ON r.stop_id = s.stop_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY time_key, stop_id, demographic, travel_pattern) as fact_id,
    *
FROM fact_rider_segments