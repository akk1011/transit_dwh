WITH performance_metrics AS (
    SELECT 
        t.datetime::DATE as report_date,
        t.hour,
        rm.status,
        rm.event_type,
        rm.battery_health,
        lp.schedule_status,
        lp.delay_minutes,
        s.stop_name,
        COUNT(*) as event_count
    FROM {{ ref('fact_remote_monitor') }} rm
    JOIN {{ ref('dim_time') }} t ON rm.time_key = t.time_key
    JOIN {{ ref('fact_line_performance') }} lp ON t.time_key = lp.time_key
    JOIN {{ ref('dim_stops') }} s ON s.stop_id = s.stop_id
    GROUP BY 
        t.datetime::DATE,
        t.hour,
        rm.status,
        rm.event_type,
        rm.battery_health,
        lp.schedule_status,
        lp.delay_minutes,
        s.stop_name
)
SELECT * FROM performance_metrics