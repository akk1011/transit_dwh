WITH kpi_metrics AS (
    SELECT 
        t.datetime::DATE as report_date,
        SUM(rv.rider_count) as total_ridership,
        AVG(rv.rider_count) as avg_ridership,
        AVG(rv.utilization_rate) as avg_utilization,
        AVG(CASE WHEN lp.schedule_status = 'On-Time' THEN 1 ELSE 0 END) as on_time_rate,
        AVG(rm.battery_health) as avg_battery_health,
        COUNT(DISTINCT CASE WHEN rm.status = 'Degraded' OR rm.status = 'Offline' THEN rm.remote_unit_id END) as alert_count
    FROM {{ ref('fact_rider_volume') }} rv
    JOIN {{ ref('dim_time') }} t ON rv.time_key = t.time_key
    LEFT JOIN {{ ref('fact_line_performance') }} lp ON t.time_key = lp.time_key
    LEFT JOIN {{ ref('fact_remote_monitor') }} rm ON t.time_key = rm.time_key
    GROUP BY t.datetime::DATE
)
SELECT * FROM kpi_metrics