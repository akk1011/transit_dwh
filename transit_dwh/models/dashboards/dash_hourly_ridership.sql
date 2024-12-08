WITH hourly_ridership AS (
    SELECT 
        t.datetime::DATE as report_date,
        t.hour,
        s.stop_name,
        l.line,
        SUM(rv.rider_count) as total_riders,
        AVG(rv.rider_count) as avg_riders,
        AVG(rv.entries) as avg_entries,
        AVG(rv.exits) as avg_exits,
        SUM(rv.entries) as total_entries,
        SUM(rv.exits) as total_exits,
        AVG(rv.utilization_rate) as avg_utilization
    FROM {{ ref('fact_rider_volume') }} rv
    JOIN {{ ref('dim_time') }} t ON rv.time_key = t.time_key
    JOIN {{ ref('dim_stops') }} s ON rv.stop_id = s.stop_id
    JOIN {{ ref('dim_lines') }} l ON rv.line_id = l.line_id
    GROUP BY 
        t.datetime::DATE,
        t.hour,
        s.stop_name,
        l.line
)
SELECT * FROM hourly_ridership

