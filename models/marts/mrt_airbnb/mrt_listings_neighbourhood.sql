{{
    config(
        tag='daily'
    ) 
}}

with neighbourhood_prices as (

    select
        host_id, -- hosts can have multiple listings
        neighbourhood_cleansed,
        room_type,
        count(id) as total_listings,
        round(avg(minimum_nights), 2) as avg_minimum_nights

    from {{ ref('stg_airbnb__listings') }}

    group by host_id, neighbourhood_cleansed, room_type

)

select * from neighbourhood_prices
