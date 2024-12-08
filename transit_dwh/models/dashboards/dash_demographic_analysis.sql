WITH demographic_metrics AS (
    SELECT 
        t.datetime::DATE as report_date,
        s.stop_name,
        rs.demographic,
        rs.travel_pattern,
        rs.location_type,
        SUM(rs.segment_count) as total_riders,
        COUNT(DISTINCT t.hour) as active_hours
    FROM {{ ref('fact_rider_segments') }} rs
    JOIN {{ ref('dim_time') }} t ON rs.time_key = t.time_key
    JOIN {{ ref('dim_stops') }} s ON rs.stop_id = s.stop_id
    GROUP BY 
        t.datetime::DATE,
        s.stop_name,
        rs.demographic,
        rs.travel_pattern,
        rs.location_type
)
SELECT * FROM demographic_metrics