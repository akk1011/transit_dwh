-- models/facts/fact_remote_monitor.sql
WITH fact_remote_monitor AS (
    SELECT 
        t.time_key,
        ru.remote_unit_id,
        r.event_type,
        r.status,
        r.battery_health,
        s.stop_name as location
    FROM {{ ref('stg_raw_transit') }} r
    JOIN {{ ref('dim_time') }} t 
        ON r.datetime = t.datetime
    JOIN {{ ref('dim_remote_units') }} ru 
        ON r.remote_unit_id = ru.remote_unit_id
    JOIN {{ ref('dim_stops') }} s 
        ON r.stop_id = s.stop_id
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY time_key, remote_unit_id) as fact_id,
    *
FROM fact_remote_monitor