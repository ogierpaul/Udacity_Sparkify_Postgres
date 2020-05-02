from code.sql_queries import *
import pandas as pd
from code.utils import prepare_data, connection_sparkifydb, get_all_files, sanitize_inputs

def process_song_file(cur, filepath):
    """
    Update the song and artist table from the song file
    Read the json, extract the relevant info, rename and sanitize it.
    Insert it into the artists and songs tables.
    Args:
        cur (psycopg2.cursor): cursor
        filepath (str): path of file to process

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    songs_cols = pd.Series(
        index=['song_id', 'title', 'artist_id', 'year', 'duration'],
        data=['song_id', 'title', 'artist_id', 'year', 'duration'])
    song_data = prepare_data(df=df, usecols=songs_cols, key='song_id')
    for (i, r) in song_data.iterrows():
        cur.execute(song_table_insert, r)
    
    # insert artist record
    artist_cols = pd.Series(
        index=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'],
        data=['artist_id', 'name', 'location', 'latitude', 'longitude'])
    artist_data = prepare_data(df=df, usecols=artist_cols, key='artist_id')
    for (i, r) in artist_data.iterrows():
        cur.execute(artist_table_insert, r)
    return None



def process_log_file(cur, filepath):
    """
    Update the song and artist table from the song file
    Read the json, extract the relevant info, rename and sanitize it.
    Insert it into the artists and songs tables.
    Args:
        cur (psycopg2.cursor): cursor
        filepath (str): path of file to process

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    assert isinstance(t, pd.Series)
    t.drop_duplicates(inplace=True)
    t.dropna(inplace=True)

    # insert time data records
    time_df = pd.DataFrame(index=t.index)
    time_df['start_time'] = t
    time_df['hour'] = t.dt.hour
    time_df['day'] = t.dt.day
    time_df['week'] = t.dt.weekofyear
    time_df['month'] = t.dt.month
    time_df['year'] = t.dt.year
    time_df['weekday'] = t.dt.weekday

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.loc[~user_df['userId'].isnull()]
    user_df = user_df.loc[~(user_df['userId'] == 0)]
    user_df = user_df.loc[user_df['userId'].astype(str).str.len() > 0]
    user_df.drop_duplicates(subset=['userId'], inplace=True)
    users_cols = pd.Series(
        index=['userId','firstName','lastName','gender','level'],
        data=['user_id', 'first_name', 'last_name', 'gender', 'level'])
    user_data = prepare_data(df=user_df, usecols=users_cols)
    for (i, r) in user_data.iterrows():
        cur.execute(user_table_insert, r)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.Timestamp(row['ts'], unit='ms')

        songplay_data = (start_time, row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        songplay_data = [sanitize_inputs(c) for c in songplay_data]
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """

    Args:
        cur (psycopg2.cursor): cursor
        conn (psycopg2.connection): conneciotn
        filepath (str): filepath of root folder for files
        func: transformation func

    Returns:
        None
    """
    all_files = get_all_files(filepath)
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))



def main():
    """
    Main return
    ETL update the data
    Returns:
        None
    """
    conn = connection_sparkifydb()
    cur = conn.cursor()

    process_data(cur, conn, filepath='../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()