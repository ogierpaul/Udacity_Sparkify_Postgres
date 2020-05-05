SELECT * FROM test_foo;
DELETE FROM test_foo;



SELECT songs.song_id, artists.artist_id
                    FROM songs
                    JOIN artists USING (artist_id)
                    WHERE
                    songs.title=(%s) AND
                    songs.duration=(%s) AND
                    artists.name=(%s);




