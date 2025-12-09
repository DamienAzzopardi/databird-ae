with

listening_data as (

    select *
    from {{ ref('stg_spotify__listening_data') }}

),

songs as (

    select *
    from {{ ref('stg_spotify__songs') }}

),

final as (

    select
        l.song_id,
        s.artist,
        l.listen_date,
        l.minutes_listened
    from listening_data as l
    left join songs as s
        on l.song_id = s.song_id
    where l.listen_date >= date_sub(current_date(), interval 2 year)

)

select * from final
