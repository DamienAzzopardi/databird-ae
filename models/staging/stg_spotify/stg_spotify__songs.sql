with

source as (

    select * from {{ source('spotify', 'songs') }}

),

renamed as (

    select
        song_id,
        album,
        release_year,
        upper(title) as title,
        upper(artist) as artist,
        coalesce(genre, 'Unknown') as genre
    from source

)

select * from renamed
