with
song_listening as (

    select
        l.song_id,
        s.artist,
        l.listen_date,
        l.minutes_listened

    from {{ ref("stg_spotify__listening_data") }} as l

    left join {{ ref("stg_spotify__songs") }} as s on l.song_id = s.song_id

    where {{ filter_last_n_periods("l.listen_date", "year", 2) }}

)

select *
from song_listening
