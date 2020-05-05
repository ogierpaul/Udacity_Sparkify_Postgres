SELECT song_id, artist_id FROM
    temp_song_select AS t
    LEFT JOIN (SELECT song_id, title, artist_id, duration FROM songs) s
    USING (title, duration)
    LEFT JOIN (SELECT artist_id, name FROM artists) a
    USING( artist_id, name);