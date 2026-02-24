{%snapshot scd_load_carriers%}

{{
    config(
        target_schema='snapshots',
        unique_key='load_carrier_id',
        strategy='check',
        check_cols=['carrier_status', 'comments', 'inbound_date']
    )
}}
select 
    load_carrier_id,
    dispatch_date,
    carrier_status,
    comments,
    site,
    inbound_date
from {{ ref('stg_inbound_packages') }}
-- We only need one row per pallet for this snapshot, so we group it
group by 1, 2, 3, 4, 5, 6

{%endsnapshot%}



