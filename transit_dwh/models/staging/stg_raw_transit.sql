WITH source AS (
    SELECT *
    FROM {{ source('staging', 'raw_transit_data') }}
)

SELECT 
    FACT_ID as fact_id,
    DATETIME as datetime,
    STOP_ID as stop_id,
    REMOTE_UNIT_ID as remote_unit_id,
    LINE_ID as line_id,
    RIDER_COUNT as rider_count,
    STOP_NAME as stop_name,
    NORTH_DIRECTION_LABEL as north_direction_label,
    SOUTH_DIRECTION_LABEL as south_direction_label,
    DIVISION as division,
    LINE as line,
    CONNECTING_LINES as connecting_lines,
    DAYTIME_ROUTES as daytime_routes,
    REMOTE_UNIT as remote_unit,
    HOUR as hour,
    DAY as day,
    MONTH as month,
    YEAR as year,
    CAPACITY as capacity,
    PEAK_OFF_PEAK as peak_off_peak,
    SCHEDULE_ON_TIME as schedule_on_time,
    TRIP_TIMING as trip_timing,
    EVENT_TYPE as event_type,
    STATUS as status,
    BATTERY_HEALTH as battery_health,
    DEMOGRAPHIC as demographic,
    TRAVEL_PATTERN as travel_pattern,
    ENTRIES as entries,
    EXITS as exits,
    NEAR_COMMERCIAL_AREA as near_commercial_area,
    NEAR_UNIVERSITY as near_university
FROM source