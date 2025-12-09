with

source as (

    select * from {{ source('spotify', 'listening_data') }}

),

renamed as (

    select
        song_id,
        cast(listen_date as date) as listen_date,
        coalesce(minutes_listened, 0) as minutes_listened
    from source

)

select * from renamed
