{{ config(materialized='table') }}

WITH staging_weather AS (
    -- 🚨 THE MAGIC: We use ref() to build on top of our staging model!
    SELECT * FROM {{ ref('stg_weather') }}
),

business_logic AS (
    SELECT
        station_id,
        weather_timestamp,
        ROUND(temp_celsius),
        humidity_pct,
        wind_speed_kmh,

        -- Add Logic: Define "Comfort" using our clean column names
        CASE 
            WHEN temp_celsius BETWEEN 20 AND 25 AND humidity_pct < 50 THEN 'Perfect'
            WHEN temp_celsius > 30 OR humidity_pct > 80 THEN 'Sticky'
            WHEN temp_celsius < 5 THEN 'Freezing'
            ELSE 'Normal'
        END AS comfort_status

    FROM staging_weather
)

SELECT * FROM business_logic