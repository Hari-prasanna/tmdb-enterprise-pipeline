{%snapshot movies_snapshot%}


{{ config(
    target_schema='snapshot',
    unique_key='movie_id',
    strategy='check',
    check_cols=['popularity','status','release_date']
)}}


SELECT *
FROM {{ref('dim_movie')}}

{% endsnapshot %}