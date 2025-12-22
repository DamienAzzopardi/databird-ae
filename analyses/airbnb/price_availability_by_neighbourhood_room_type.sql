with listings as (

    select *from {{ ref('stg_airbnb__listings') }}

),

base as (
    select
        neighbourhood_cleansed,
        room_type,
        count(*) as listing_count,
        countif(id is null) as null_id_count,
        avg(price) as avg_price,
        approx_quantiles(price, 100)[offset(50)] as median_price,
        avg(availability_365) as avg_availability_365,
        approx_quantiles(availability_365, 100)[offset(50)] as median_availability_365,
        avg(number_of_reviews) as avg_reviews,
        avg(minimum_nights) as avg_minimum_nights
    from listings
    group by 1, 2
)

select * from base
order by listing_count desc, neighbourhood_cleansed asc, room_type asc
