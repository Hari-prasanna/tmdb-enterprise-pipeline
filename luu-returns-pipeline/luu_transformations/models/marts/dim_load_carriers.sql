WITH staging AS (SELECT *
FROM {{ref('stg_inbound_packages')}}),

final AS
(SELECT
        load_carrier_id,
        dispatch_date,
        carrier_status,
        comments,
        site,
        type,
        inbound_date,
        transit_duration, 
        count(*) as total_items,
        sum(weight_kg) as total_weight_kg 
FROM staging
GROUP BY 1,2,3,4,5,6,7,8)


SELECT *
FROM final