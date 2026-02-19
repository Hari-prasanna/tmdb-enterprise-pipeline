{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'weather_data') }}
    -- 🚨 THE FIX: Throw away the empty boxes!
    WHERE temperature_2m IS NOT NULL 
),

renamed_and_casted AS (
    SELECT
        md5(station_id || CAST(time AS VARCHAR)) AS weather_id,
        station_id,
        CAST(time AS TIMESTAMP) AS weather_timestamp,
        
        ROUND(temperature_2m::NUMERIC, 1) AS temp_celsius,
        relative_humidity_2m AS humidity_pct,
        wind_speed_10m AS wind_speed_kmh
    FROM raw_data
)

SELECT * FROM renamed_and_casted