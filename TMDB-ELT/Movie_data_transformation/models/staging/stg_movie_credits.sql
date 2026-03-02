WITH raw_credits AS (
    SELECT * FROM {{ source('raw_data', 'raw_movie_credits') }}
),

unpacked_cast AS (
    SELECT 
        id::INT AS movie_id,
        jsonb_array_elements("cast"::JSONB) AS actor_json
    FROM raw_credits
),

cleaned_cast AS (
    SELECT
        movie_id,
        (actor_json ->> 'id')::INT AS actor_id,
        actor_json ->> 'name' AS actor_name
        
        -- We are ignoring the 'crew' and 'character' names for now to keep our model fast and focused!
    FROM unpacked_cast
)

SELECT * FROM cleaned_cast