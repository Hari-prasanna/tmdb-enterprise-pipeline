{{ config(
    materialized='incremental',
    unique_key=['movie_id', 'actor_id'] 
) }}

WITH raw_credits AS (
    SELECT * FROM {{ source('raw_data', 'raw_movie_credits') }}
),

deduplicated_movies AS (
    SELECT 
        id::INT AS movie_id,
        "cast",
        (loaded_at)::TIMESTAMP AS loaded_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) as row_num
    FROM raw_credits
    
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

unpacked_cast AS (
    SELECT 
        movie_id,
        loaded_at,
        jsonb_array_elements("cast"::JSONB) AS actor_json
    FROM deduplicated_movies
    WHERE row_num = 1
),

cleaned_cast AS (
    SELECT
        movie_id,
        (actor_json ->> 'id')::INT AS actor_id,
        actor_json ->> 'name' AS actor_name,
        loaded_at
    FROM unpacked_cast
)

SELECT * FROM cleaned_cast