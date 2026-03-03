WITH raw_details AS (
    SELECT * FROM {{ source('raw_data', 'raw_movie_details') }}
),
unpacked_genres AS (
    SELECT 
        id::INT AS movie_id,
        jsonb_array_elements(genres::JSONB) AS genre_json
    FROM raw_details
)

SELECT 
    (genre_json ->> 'id')::INT AS genre_id,
    genre_json ->> 'name' AS genre_name
FROM unpacked_genres