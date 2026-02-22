WITH staging as (SELECT *
FROM {{ref('stg_inbound_packages')}}),

final as (
SELECT
    item_id,              -- Primary Key
    load_carrier_id,      -- Foreign Key
    source_channel,
    source_quality, 
    condition,
    destination_quality,
    destination_channel,
    weight_kg
FROM staging)

SELECT *
FROM final
