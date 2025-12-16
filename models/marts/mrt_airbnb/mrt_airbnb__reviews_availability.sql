
with

listings as (

    select
        neighbourhood_cleansed,
        room_type,
        number_of_reviews,
        availability_365
    from {{ ref('stg_airbnb__listings') }}

),

reviews as (

    select
        neighbourhood_cleansed,
        room_type,
        sum(number_of_reviews) as total_reviews
    from listings
    group by
        neighbourhood_cleansed,
        room_type

),

final as (

    select
        neighbourhood_cleansed,
        room_type,
        total_reviews
    from reviews

)

select * from final
