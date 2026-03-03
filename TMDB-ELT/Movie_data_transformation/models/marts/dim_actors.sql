WITH stg_movie_credits AS (SELECT *
FROM {{ ref('stg_movie_credits') }})


SELECT
    DISTINCT actor_id, 
    actor_name
FROM stg_movie_credits