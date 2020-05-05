import glob
import os
import bleach
import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import pathlib
import datetime

def connection_sparkifydb():
    """

    Returns:
        psycopg2.connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    return conn


def get_all_files(filepath):
    """

    Args:
        filepath: path to explore

    Returns:
        list: list of path of files to open
    Examples:
        ['/Users/paulogier/80-PythonProjects/Udacity_Sparkify_Postgres/data/song_data/A/A/TRAAABD128F429CF47.json']
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def order_cols(df, usecols):
    """
    - Select the columns to be inserted
    - Rename them
    - Re-order the attributes as in the destination table

    Args:
        df (pd.DataFrame): data to be loaded
        usecols (pd.Series): ordered dict containing as value the list of output cols needed, as index their name \
        in the source data

    Returns:
        pd.DataFrame
    """
    assert isinstance(usecols, pd.Series)
    old_cols = usecols.index
    new_cols = usecols.values
    df2 = df[old_cols].copy()  # select interesting cols from raw data
    df2 = df2.rename(columns=usecols)  # rename them
    df2 = df2[new_cols]  # re-order cols
    return df2


def sanitize_inputs(i):
    """
    Sanitize the input to prevent JS injection attack with bleach.
    Args:
        i: value to be cleaned
    """
    if i is None:
        return None
    if isinstance(i, str):
        return bleach.clean(i)
    else:
        return i


def primary_key_check(df, key):
    """
    Remove from the dataframe all rows where the primary key is null
    And where the key is duplicate
    Args:
        df (pd.DataFrame): input data
        key (str/list): name of the primary key column (or list)

    Returns:
        pd.DataFrame
    """
    if type(key) == str:
        key_cols = [key]
    else:
        key_cols = key
    return df.dropna(axis=0, subset=key_cols).drop_duplicates(subset=key_cols)


def prepare_data(df, usecols=None, pkey=None):
    """
    - If usecols is provided: select, rename and order the columns
    - If key is provided: drop rows where the primary key has a null value (in case of composite key, any key that contains a null will be dropped)
    - Sanitize the inputs with bleach
    Args:
        df (pd.DataFrame): data to be loaded
        usecols (pd.Series): ordered dict containing as value the list of output cols needed, as index their name \
        in the source data

    Returns:
        pd.DataFrame
    """
    assert isinstance(df, pd.DataFrame)
    if not usecols is None:
        df2 = order_cols(df=df, usecols=usecols).copy()
    else:
        df2 = df.copy()
    if not pkey is None:
        df2 = primary_key_check(df=df2, key=pkey)

    df2 = df2.applymap(lambda v: sanitize_inputs(v))  # sanitize clean inputs with bleach
    return df2

def bulk_copy(df, cur, tablename, pkey=None, filename=None, upsert=False):
    """
    Bulk import into PostgreSql
    - Write the data as a csv file into the csvpath directory. (without the index)\
    If no primary key is provided:
        - execute  a COPY FROM query
        - delete the file
    Else:
        - Create a temporary empty temp_tablename with same structure as tablename
        - COPY FROM the input data to a temp_tablename
        - INSERT / ON CONFLICT DO NOTHING between temp_tablename and tablename
        - DROP temp_tablename
    Args:
        df (pd.DataFrame): Data to import. All the columns must be in the same order. Index will not be copied.
        cur (psycopg2.cursor): cursor object
        tablename (str): table name to import
        filename (str): name of the file. If none, will use timestamp of the time when the function is called
        pkey(str/list): primary key or list. If provided, will allow upsert.

    Returns:
        None
    """
    if filename is None:
        filename = tablename + '_' + datetime.datetime.now().strftime("%Y-%b-%d-%H-%M-%S") + '.csv'
    # csvdir = os.path.dirname(sys.path[0]) + '/data/csv_sync'  # csvdir (str): path of directory for csv import
    csvdir = os.path.abspath('../data/csv_sync')
    filepath = csvdir + '/' + filename
    df.to_csv(path_or_buf=filepath, encoding='utf-8', sep='|', index=False)
    # Preventing SQL injections thanks to https://github.com/psycopg/psycopg2/issues/529

    if pkey is None:
        query = sql.SQL("""
            COPY {tablename} FROM STDIN WITH CSV HEADER ENCODING 'UTF-8' DELIMITER '|'
            """).format(tablename=sql.Identifier(tablename))
        with open(filepath, 'r') as f:
            cur.copy_expert(query, f)
    else:
        # COPIED FROM https://www.postgresql.org/message-id/464F7A31.6020501@autoledgers.com.au
        # Preventing SQL injections thanks to https://realpython.com/prevent-python-sql-injection/
        temp_tablename = 'temp_' + tablename
        query_create = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {temp_tablename} AS SELECT * FROM {tablename} WHERE 1=0;
        TRUNCATE TABLE {temp_tablename};""").format(
            temp_tablename = sql.Identifier(temp_tablename),
            tablename=sql.Identifier(tablename)
        )
        cur.execute(query_create)
        query_delete = sql.SQL("""DELETE FROM {temp_tablename};""").format(temp_tablename=sql.Identifier(temp_tablename))
        cur.execute(query_delete)

        query_copy = sql.SQL("""
        COPY {temp_tablename} FROM STDIN WITH 
        DELIMITER AS '|' ENCODING 'UTF-8' CSV HEADER;
        """).format(temp_tablename = sql.Identifier('temp_' + tablename))
        with open(filepath, 'r') as f:
            cur.copy_expert(query_copy, f)
        if isinstance(pkey, str):
            pkey_s = sql.Identifier(pkey)
        else:
            pkey_s = sql.SQL(', ').join([sql.Identifier(c) for c in pkey])
        query_upsert = sql.SQL("""
        INSERT INTO {tablename}
            (
                SELECT DISTINCT ON ({pkey_s}) *
                FROM {temp_tablename}
                WHERE ({pkey_s}) is not null
            )
        ON CONFLICT ({pkey_s})
        DO NOTHING;
        """).format(tablename=sql.Identifier(tablename),
                    temp_tablename=sql.Identifier(temp_tablename),
                    pkey_s=pkey_s)
        # cur.execute(query_upsert, {'temp_tablename': 'temp_' + tablename, 'tablename': tablename, 'primary_key': _format_pkey(pkey)})
        cur.execute(query_upsert)
        query_drop = sql.SQL("""DROP TABLE {temp_tablename};""").format(temp_tablename=sql.Identifier(temp_tablename))
        cur.execute(query_drop)
    os.remove(filepath)
    return None

def _format_pkey(pkey):
    """
    to insert in the sql query
    Args:
        pkey (str/list):

    Returns:
        str: pkey: column name or list of column names separated by commar
    """
    if isinstance(pkey, str):
        pkey_s = pkey
    else:
        pkey_s = ', '.join(pkey)
    return pkey_s


def copy_from(cur, filepath, tablename, usecols, key):
    df = pd.read_json(filepath, lines=True)
    df = prepare_data(df=df, usecols=usecols, pkey=key)
    bulk_copy(df=df, cur=cur, tablename=tablename)
    pass


