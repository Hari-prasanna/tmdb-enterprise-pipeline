{{config(materialized = 'incremental', unique_key = 'movie_id')}}

WITH raw_movies_data AS (
    SELECT * FROM {{source('raw_data','raw_movies')}}
),

cleaned AS (
    SELECT 
        id::INT AS movie_id,
        title::VARCHAR,
        original_language :: VARCHAR AS language, 
        release_date::DATE,
        popularity :: FLOAT,
        vote_count :: FLOAT,
        ROUND(vote_average::NUMERIC,1) AS vote_avg,
        (loaded_at)::TIMESTAMP AS loaded_at
    FROM raw_movies_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY loaded_at DESC) as row_num
    FROM cleaned
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    movie_id,
    title,
    language,
    release_date,
    popularity,
    vote_count,
    vote_avg,
    loaded_at
FROM deduplicated
WHERE row_num = 1