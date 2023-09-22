{{ config(materialized='table', schema='fct') }}


with summed_data as (
    select
        date(dropoff_datetime) as day,
        sum(fare_amount) as sum_fare_amount,
        sum(extra) as sum_extra,
        sum(mta_tax) as sum_mta_tax,
        sum(tip_amount) as sum_tip_amount,
        sum(tolls_amount) as sum_tolls_amount,
        sum(improvement_surcharge) as sum_improvement_surcharge,
        sum(total_amount) as sum_total_amount,
        sum(congestion_surcharge) as sum_congestion_surcharge
    from
        {{ ref('stg_green_taxi') }}
    group by
        date(dropoff_datetime)
)

select
    *
from summed_data