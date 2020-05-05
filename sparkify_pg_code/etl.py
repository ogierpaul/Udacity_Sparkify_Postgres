from sparkify_pg_code.sql_queries import *
import pandas as pd
from sparkify_pg_code.utils import prepare_data, connection_sparkifydb, get_all_files, sanitize_inputs, bulk_copy
import psycopg2


def process_song_file(cur, filepath, bulk=False):
    """
    Update the song and artist table from the song file
    Read the json, extract the relevant info, rename and sanitize it.
    Insert it into the artists and songs tables.
    Args:
        cur (psycopg2.cursor): cursor
        filepath (str): path of file to process
        bulk (bool): If true, will use copy from instead of insert

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    process_song_data(df=df, cur=cur, bulk=bulk)
    process_artist_data(df=df, cur=cur, bulk=bulk)
    return None


def bulk_select_song_info(song_info, cur):
    """
    From the songs.title, songs.duration, artists.name information, return the song_id and artist_id information
    - Create a temp table called temp_song_select
    - Delete all rows from temp_song_select
    - Do a left join on the songs table on (title, duration) \
        and another left join on the artists table on (artist_id, name)
    - Select from the results the song_id and artist_id
    - Drop the table
    - Return the information as a DataFrame
    Args:
        song_info (pd.DataFrame): contains the columns ['title', 'duration', 'name']
        cur (psycopg2.cursor): connection object

    Returns:
        pd.DataFrame: with columns
    """

    try:
        # CREATE temp TABLE
        cur.execute("""
        CREATE TABLE IF NOT exists temp_song_select
        (title VARCHAR(256),
        duration DOUBLE PRECISION,
        NAME VARCHAR(256));
        """)

        # REMOVE rows from temp table
        cur.execute("DELETE FROM temp_song_select;")

        # COPY rows from data into the temp table
        bulk_copy(df=song_info, cur=cur, tablename='temp_song_select', pkey=None)

        # SELECT
        query_join = """
        SELECT song_id, artist_id FROM
        temp_song_select AS t
        LEFT JOIN (SELECT song_id, title, artist_id, duration FROM songs) s
        USING (title, duration)
        LEFT JOIN (SELECT artist_id, name FROM artists) a
        USING( artist_id, name);
        """
        cur.execute(query_join)

        # Get the results in a DataFrame
        r = cur.fetchall()
        df = pd.DataFrame(data=r, columns=['song_id', 'artist_id'], index=song_info.index)

        # Drop the table
        query_drop = """
        DROP TABLE temp_song_select;
        """
        cur.execute(query_drop)
        return df
    except psycopg2.Error as e:
        print(e)
        return None


def process_song_data(df, cur, bulk=False):
    """
    Update the songs table from the song file
    - Select the columns
    - Sanitize the inputs
    - Update the table
    Args:
        df (pd.DataFrame): Song data file
        cur (psycopg2.cursor): Cursor
        bulk (bool):  If true, will use copy from instead of insert

    Returns:
        None
    """
    # Select the columns
    songs_cols = pd.Series(
        index=['song_id', 'title', 'artist_id', 'year', 'duration'],
        data=['song_id', 'title', 'artist_id', 'year', 'duration'])

    # Sanitize the inputs
    song_data = prepare_data(df=df, usecols=songs_cols, pkey='song_id')

    # Update the table
    if bulk:
        bulk_copy(df=song_data, cur=cur, tablename='songs', pkey='song_id', upsert=True)
    else:
        for (i, r) in song_data.iterrows():
            cur.execute(song_table_insert, r)
    return None


def process_artist_data(df, cur, bulk=False):
    """
    Update the artists table from the song file
    - Select the columns
    - Sanitize the inputs
    - Update the table
    Args:
        df (pd.DataFrame): Song data file
        cur (psycopg2.cursor): Cursor
        bulk (bool):  If true, will use copy from instead of insert

    Returns:
        None
    """
    # Select the columns
    artist_cols = pd.Series(
        index=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'],
        data=['artist_id', 'name', 'location', 'latitude', 'longitude'])

    # Sanitize the inputs
    artist_data = prepare_data(df=df, usecols=artist_cols, pkey='artist_id')

    # Update the table
    if bulk:
        bulk_copy(df=artist_data, cur=cur, tablename='artists', pkey='artist_id', upsert=True)
    else:
        for (i, r) in artist_data.iterrows():
            cur.execute(artist_table_insert, r)
    return None


def process_time_data(df, cur, bulk=False):
    """
    Update the time table from the log file
    - convert timestamp column to datetime
    - Prepare the time dimensions
    - Update the table
    Args:
        df (pd.DataFrame): Log file
        cur (psycopg2.cursor): Cursor
        bulk (bool):  If true, will use copy from instead of insert

    Returns:
        None
    """
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    assert isinstance(t, pd.Series)
    t.drop_duplicates(inplace=True)
    t.dropna(inplace=True)

    # Prepare the time dimensions
    time_df = pd.DataFrame(index=t.index)
    time_df['start_time'] = t
    time_df['hour'] = t.dt.hour
    time_df['day'] = t.dt.day
    time_df['week'] = t.dt.weekofyear
    time_df['month'] = t.dt.month
    time_df['year'] = t.dt.year
    time_df['weekday'] = t.dt.weekday
    usecols = pd.Series(data=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    usecols.index = usecols.values
    time_df = prepare_data(df=time_df, usecols=usecols, pkey=['start_time'])

    # Update the table
    if bulk:
        bulk_copy(df=time_df, tablename='time', cur=cur, pkey='start_time', upsert=True)
    else:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    return None


def process_user_data(df, cur, bulk=False):
    """
    Update the user table from the log file
    - Select the columns
    - Remove incorrect UserId rows and clean the data
    - Update the table
    Args:
        df (pd.DataFrame): Log file
        cur (psycopg2.cursor): Cursor
        bulk (bool):  If true, will use copy from instead of insert

    Returns:
        None
    """
    # Select cols
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # Remove incorrect UserId rows and clean the data
    user_df = user_df.loc[~user_df['userId'].isnull()]
    user_df = user_df.loc[~(user_df['userId'] == 0)]
    user_df = user_df.loc[user_df['userId'].astype(str).str.len() > 0]
    user_cols = pd.Series(
        index=['userId', 'firstName', 'lastName', 'gender', 'level'],
        data=['user_id', 'first_name', 'last_name', 'gender', 'level'])
    user_df = prepare_data(df=user_df, usecols=user_cols, pkey=['user_id'])

    # Update the table
    if bulk:
        bulk_copy(df=user_df, tablename='users', cur=cur, pkey='user_id')
    else:
        for (i, r) in user_df.iterrows():
            cur.execute(user_table_insert, r)
    return None


def process_songplays_data(df, cur, bulk=False):
    """
    Update the songplays table from the log file
    - Select the columns
    - Convert ts to datetime
    - Do a join with song and artist table to return the song_id and artist_id
    - Update the table
    Args:
        df (pd.DataFrame): Log file
        cur (psycopg2.cursor): Cursor
        bulk (bool):  If true, will use copy from instead of insert

    Returns:
        None
    """
    if bulk:
        # Select the columns
        songplay_df = df[
            ['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'song', 'artist', 'length']].copy()
        # Convert to datetime
        songplay_df['start_time'] = pd.to_datetime(songplay_df['ts'], unit='ms')

        # Do a join with song and artist table to return the song_id and artist_id
        add_info = bulk_select_song_info(song_info=songplay_df[['song', 'length', 'artist']], cur=cur)
        songplay_df['song_id'] = add_info['song_id']
        songplay_df['artist_id'] = add_info['artist_id']

        # Prepare data
        usecols = pd.Series(
            index=['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'],
            data=['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'userAgent'])
        songplay_df = prepare_data(df=songplay_df, usecols=usecols, pkey=['start_time', 'user_id'])

        # Update the table
        bulk_copy(df=songplay_df, cur=cur, tablename='songplays', pkey=['start_time', 'user_id'])

    else:
        # insert songplay records
        for index, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # Prepare the data
            start_time = pd.Timestamp(row['ts'], unit='ms')
            songplay_data = (
                start_time, row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'],
                row['userAgent'])
            songplay_data = [sanitize_inputs(c) for c in songplay_data]
            # insert songplay record
            cur.execute(songplay_table_insert, songplay_data)


def process_log_file(cur, filepath, bulk=False):
    """
    Update the time, user and songplays table from the log file
    Read the json, extract the relevant info, rename and sanitize it.
    Insert it into the time, user, and songplays tables.
    Args:
        cur (psycopg2.cursor): cursor
        filepath (str): path of file to process
        bulk (bool): If true, will use copy from instead of insert

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # Process time data
    process_time_data(df=df, cur=cur, bulk=bulk)
    process_user_data(df=df, cur=cur, bulk=bulk)
    process_songplays_data(df=df, cur=cur, bulk=bulk)
    return None


def process_data(cur, conn, filepath, func, bulk=False):
    """
    Process (Update) the data for each of the files detected in filepath.
    Args:
        cur (psycopg2.cursor): cursor
        conn (psycopg2.connection): connection
        filepath (str): filepath of root folder for files
        func: transformation func, either from log_file or song_file
        bulk (bool): If true, will use copy from instead of insert

    Returns:
        None
    """
    all_files = get_all_files(filepath)
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, bulk=bulk)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    return None


def main():
    """
    Main return
    ETL update the data
    Returns:
        None
    """
    conn = connection_sparkifydb()
    cur = conn.cursor()

    process_data(cur, conn, filepath='../data/song_data', func=process_song_file, bulk=True)
    process_data(cur, conn, filepath='../data/log_data', func=process_log_file, bulk=True)

    conn.close()
    return None


if __name__ == "__main__":
    main()
