WITH raw_movies_data AS (SELECT * 
FROM {{source('raw_data','raw_movies')}}
)


SELECT 
	id::INT AS movie_id,
	title::VARCHAR,
	original_language :: VARCHAR AS language, 
	release_date::DATE,
	popularity :: FLOAT,
	vote_count :: FLOAT,
	ROUND(vote_average::NUMERIC,1) AS vote_avg
FROM raw_movies_data