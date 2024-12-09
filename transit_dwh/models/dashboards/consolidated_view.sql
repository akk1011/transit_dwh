-- models/dashboard/dash_consolidated_metrics.sql
WITH base_metrics AS (
    SELECT 
        t.datetime::DATE as report_date,
        t.hour,
        s.stop_name,
        l.line,
        rs.demographic,
        rs.travel_pattern,
        rs.location_type,
        -- Raw metrics (no aggregation)
        rv.rider_count,
        rv.entries,
        rv.exits,
        rv.utilization_rate,
        rm.battery_health,
        rm.status as equipment_status,
        rm.event_type,
        lp.schedule_status,
        lp.delay_minutes,
        -- Pre-calculated flags
        CASE WHEN lp.schedule_status = 'On-Time' THEN 1 ELSE 0 END as is_on_time,
        CASE WHEN rm.status IN ('Degraded', 'Offline') THEN 1 ELSE 0 END as is_alert
    FROM {{ ref('fact_rider_volume') }} rv
    JOIN {{ ref('dim_time') }} t 
        ON rv.time_key = t.time_key
    JOIN {{ ref('dim_stops') }} s 
        ON rv.stop_id = s.stop_id
    JOIN {{ ref('dim_lines') }} l 
        ON rv.line_id = l.line_id
    LEFT JOIN {{ ref('fact_remote_monitor') }} rm 
        ON t.time_key = rm.time_key
    LEFT JOIN {{ ref('fact_line_performance') }} lp 
        ON t.time_key = lp.time_key
    LEFT JOIN {{ ref('fact_rider_segments') }} rs 
        ON t.time_key = rs.time_key 
        AND s.stop_id = rs.stop_id
)
SELECT * FROM base_metrics