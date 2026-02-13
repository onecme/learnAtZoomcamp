with trips_unioned as(
    SELECT * FROM {{ ref('int_trips_unioned') }}
),

vendors as (
    SELECT
        DISTINCT vendorid,
        {{ get_vendor_names('vendorid') }} as vendor_name
    FROM trips_unioned
)

SELECT * FROM vendors
