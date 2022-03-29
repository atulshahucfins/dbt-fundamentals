{{config(
    materialized = 'view'
)}}

select * from {{ source('sch_stg', 'writingcompany') }}