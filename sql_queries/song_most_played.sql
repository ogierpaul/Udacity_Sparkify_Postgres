-- This query shows the top 10 most played songs in 2019
SELECT a.name, s.title, COUNT(*) as n_played FROM (
    songplays LEFT JOIN (SELECT song_id, title FROM songs) AS s USING (song_id)
    LEFT JOIN (SELECT artist_id, name FROM artists) AS a USING (artist_id)
    LEFT JOIN (SELECT start_time, year FROM time) AS t USING (start_time))
WHERE t.year = 2019
GROUP BY (a.name, s.title)
ORDER BY n_played DESC
LIMIT 10;
