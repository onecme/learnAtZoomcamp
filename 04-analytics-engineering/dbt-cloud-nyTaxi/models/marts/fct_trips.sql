with trips as (

    select *
    from {{ ref('int_trips_unioned') }}

),

add_row_num as (

    select 
        *,
        row_number() over (
            partition by vendorid, pickup_datetime, dropoff_datetime,
                        pickup_locationid, dropoff_locationid
            order by vendorid
        ) as row_num
    from trips

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'vendorid',
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_locationid',
            'dropoff_locationid',
            'row_num'
        ]) }} as trip_id,

        vendorid,
        service_type,
        pickup_datetime,
        dropoff_datetime,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount,
        payment_type,

        {{ get_payment_type_description('payment_type') }} 
            as payment_type_description

    from add_row_num

)

select *
from final
