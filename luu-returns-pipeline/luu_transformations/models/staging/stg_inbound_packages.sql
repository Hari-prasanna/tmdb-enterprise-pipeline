{{ config(materialized = 'view') }}

with source as (
    select * from {{ source('raw_data', 'inbound_packages') }}
),

cleaned as (
    select
        load_carrier_id,
        dispatch_timestamp as dispatch_date,
        carrier_status,
        nullif(trim(lower(carrier_comments)), '') as comments,
        nullif(trim(origin_site), '') as site,
        site_type as type,
        item_id,
        item_category as source_channel,
        incoming_quality as source_quality,
        nullif(trim(lower(quality_note)), '') as condition,
        assessed_grade as destination_quality,
        routed_destination as destination_channel,
        case 
            when arrived_timestamp < dispatch_timestamp then null
            else arrived_timestamp
        end as inbound_date,
        abs(weight_kg) as weight_kg
    from source
),

deduplicated as (
    select
        *,
        row_number() over(partition by load_carrier_id, item_id order by dispatch_date desc) as ran,
        inbound_date::timestamp - dispatch_date::timestamp as transit_duration
    from cleaned
)

select
    load_carrier_id,
    dispatch_date,
    carrier_status,
    comments,
    site,
    type,
    item_id,
    source_channel,
    source_quality,
    condition,
    destination_quality,
    destination_channel,
    weight_kg,
    inbound_date,
    transit_duration
from deduplicated
where ran = 1