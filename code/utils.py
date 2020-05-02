import glob
import os
import bleach
import pandas as pd
import psycopg2


def connection_sparkifydb():
    """

    Returns:
        psycopg2.connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    return conn


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
    return df.dropna(axis=0, subset=key_cols)

def prepare_data(df, usecols=None, key=None):
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
        df2 = order_cols(df=df).copy()
    else:
        df2 = df.copy()
    if not key is None:
        df2 = primary_key_check(df=df2, key=key)

    df2 = df2.applymap(lambda v: sanitize_inputs(v))  # sanitize clean inputs with bleach
    return df2


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
