{{ config(
    materialized = 'table'
) }}

with

artist_minutes as (

    select
        artist,
        sum(minutes_listened) as total_minutes_listened
    from {{ ref('int_spotify__song_listening') }}
    group by artist

),

ranked_artists as (

    select
        artist,
        total_minutes_listened,
        row_number() over (
            order by total_minutes_listened desc
        ) as artist_rank
    from artist_minutes

),

final as (

    select
        artist,
        total_minutes_listened
    from ranked_artists
    where artist_rank <= 20
    order by total_minutes_listened desc

)

select * from final
