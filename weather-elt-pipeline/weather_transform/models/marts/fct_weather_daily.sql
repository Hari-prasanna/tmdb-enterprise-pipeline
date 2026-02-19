{{ config(materialized='table') }}

WITH hourly_weather AS (
    -- We pull from staging, NOT raw!
    SELECT * FROM {{ ref('stg_weather') }}
),

daily_summary AS (
    SELECT
        -- 1. Group by day
        station_id,
        DATE(weather_timestamp) AS report_date,

        -- 2. Calculate statistics
        ROUND(MIN(temp_celsius)) AS min_temp,
        ROUND(MAX(temp_celsius)) AS max_temp,
        ROUND(AVG(temp_celsius)) AS avg_temp,
        ROUND(MAX(wind_speed_kmh)) AS max_wind_speed,
        ROUND(AVG(humidity_pct::int), 0) AS avg_humidity
        
    FROM hourly_weather
    GROUP BY 1,2
    ORDER BY 2
)

SELECT * FROM daily_summary