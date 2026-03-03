with stg_movies AS (SELECT *
FROM {{ ref('stg_movies') }}),

stg_movie_details AS (SELECT *
FROM {{ ref('stg_raw_movie_details') }})

select 
	m.movie_id,
	m.title,
	m."language",
	m.release_date,
	d.runtime_minutes AS run_time,
	d.status,
	m.popularity,
	m.vote_count,
	m.vote_avg
from stg_movies AS m
LEFT JOIN stg_movie_details AS d
ON m.movie_id = d.movie_id 

