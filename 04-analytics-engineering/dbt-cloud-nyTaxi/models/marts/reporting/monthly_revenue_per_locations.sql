with trips as (

    select *
    from {{ ref('fct_trips') }}

),

zones as (

    select *
    from {{ ref('dim_zones') }}

),

joined as (

    select
        date_trunc(pickup_datetime, month) as revenue_month,
        pickup_locationid,
        z.zone,
        z.borough,

        count(*) as total_trips,
        sum(total_amount) as total_revenue,
        sum(fare_amount) as total_fare_amount

    from trips t
    left join zones z
        on t.pickup_locationid = z.location_id  

    group by 1,2,3,4

)

select *
from joined
order by revenue_month, total_revenue desc
