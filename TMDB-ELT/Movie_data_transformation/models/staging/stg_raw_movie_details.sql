WITH raw_data AS (SELECT *
FROM {{source('raw_data','raw_movie_details')}})


SELECT 
	id :: INT AS movie_id,
	release_date,
	title :: VARCHAR,
	status,
	NULLIF(runtime::INT,0) AS runtime_minutes,
	NULLIF(budget,0) AS budget,
	NULLIF(revenue::BIGINT,0) AS revenue
FROM raw_data