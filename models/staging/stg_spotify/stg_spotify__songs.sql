with
cleaned_song as (

    select
        song_id,
        album,
        release_year,
        upper(title) as title,
        upper(artist) as artist,
        coalesce(genre, 'Unknown') as genre

    from {{ source("spotify", "songs") }}

)

select *
from cleaned_song