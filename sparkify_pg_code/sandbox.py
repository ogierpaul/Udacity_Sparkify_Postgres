from sparkify_pg_code.utils import connection_sparkifydb
conn = connection_sparkifydb()
cur = conn.cursor()
cur.execute("'SELECT * FROM songs")
r = pd