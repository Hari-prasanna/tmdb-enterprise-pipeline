{{ config(
    materialized='incremental',
    unique_key='movie_id'
) }}

WITH movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
),

details AS (
    SELECT * FROM {{ ref('stg_raw_movie_details') }}
),

final_dimension AS (
    SELECT
        m.movie_id,
        m.title,
        m.language,
        m.release_date,
        d.runtime_minutes,
        d.release_status,
        m.popularity,
        m.vote_avg,
        GREATEST(m.loaded_at, d.loaded_at) AS updated_at
    FROM movies m
    LEFT JOIN details d ON m.movie_id = d.movie_id
)

SELECT * FROM final_dimension
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}