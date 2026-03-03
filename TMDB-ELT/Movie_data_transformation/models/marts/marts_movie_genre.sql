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
    movie_id,
    (genre_json ->> 'id')::INT AS genre_id
FROM unpacked_genres