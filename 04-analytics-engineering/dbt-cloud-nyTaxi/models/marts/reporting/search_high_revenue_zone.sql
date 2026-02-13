with trips as (

    select *
    from {{ ref('fct_trips') }}
    where service_type = 'Green'
      and extract(year from pickup_datetime) = 2020

),

zones as (

    select *
    from {{ ref('dim_zones') }}

),

joined as (

    select
        z.zone,
        sum(t.total_amount) as total_revenue
    from trips t
    left join zones z
        on t.pickup_locationid = z.location_id
    group by z.zone

)

select *
from joined
order by total_revenue desc
limit 1
