class SqlQueries:
    songfact_table_insert = ("""
        SELECT
                md5(events.sessionid || cast(extract(epoch from events.start_time) as text)) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM prepared_events
            WHERE page='NextSong') events
            LEFT JOIN prepared_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    users_table_insert = ("""
        SELECT distinct on (userid) userid, firstname, lastname, gender, level
        FROM prepared_events
        WHERE page='NextSong'
    """)

    songs_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM prepared_songs
    """)

    artists_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM prepared_songs
    """)

    time_table_insert = ("""
        SELECT distinct on (start_time) start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(dow from start_time)
        FROM songfact
    """)
