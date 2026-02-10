{{ 
    config(
        tags = 'daily'
    ) 
}}

with

neighbourhood_prices as (

    select
        neighbourhood_cleansed,
        count(id) as total_listings,
        round(avg(price), 2) as avg_price
    from {{ ref('stg_airbnb__listings') }}
    group by
        neighbourhood_cleansed

),

final as (

    select
        neighbourhood_cleansed,
        total_listings,
        avg_price
    from neighbourhood_prices

)

select * from final
