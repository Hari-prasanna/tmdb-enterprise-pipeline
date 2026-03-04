
WITH raw_data AS (SELECT *
FROM {{source('raw_data','raw_movie_details')}}),


cleaned AS (SELECT 
	id :: INT AS movie_id,
	release_date,
	title :: VARCHAR,
	status,
	NULLIF(runtime::INT,0) AS runtime_minutes,
	NULLIF(budget,0) AS budget,
	NULLIF(revenue::BIGINT,0) AS revenue,
	(loaded_at)::TIMESTAMP AS loaded_at
FROM raw_data),

deduplicated AS (SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY movie_id ORDER BY loaded_at DESC) AS row_num
FROM cleaned

{% if is_incremental() %}
WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %})


SELECT
	movie_id,
	release_date,
	title,
	status,
	runtime_minutes,
	budget,
	revenue,
	loaded_at
FROM deduplicated
WHERE row_num = 1


