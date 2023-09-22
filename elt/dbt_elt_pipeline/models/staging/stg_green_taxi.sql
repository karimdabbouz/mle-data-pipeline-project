{{ config(materialized='table', schema='stg') }}

select
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge
from {{ source('src_green_taxi', 'source_data') }}