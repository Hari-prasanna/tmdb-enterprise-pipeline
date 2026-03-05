{{ config(
    materialized='incremental',
    unique_key='movie_id'
) }}

WITH stg_raw_movie_details AS (SELECT *
FROM {{ ref('stg_raw_movie_details') }}),


cleaned AS (SELECT
	movie_id,
    budget,
    revenue,
    (revenue - budget) AS profit,
     CASE 
     	WHEN budget > 0 THEN ROUND(((revenue - budget)::NUMERIC / budget) * 100, 2)
        ELSE NULL 
     END AS roi_percentage
from stg_raw_movie_details)


SELECT * FROM cleaned

-- The Bouncer: Only process new financial data!
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

