import glob
import os
import bleach
import pandas as pd
import psycopg2

def prepare_data(df, usecols):
    """
    - Select the columns to be inserted
    - Rename them
    - Sanitize the inputs with bleach
    - Re-order the attributes as in the destination table
    Args:
        df (pd.DataFrame):
        usecols (pd.Series):

    Returns:
        pd.DataFrame
    """

    assert isinstance(df, pd.DataFrame)
    assert isinstance(usecols, pd.Series)
    old_cols = usecols.index
    new_cols = usecols.values
    df2 = df[old_cols].copy()  # select interesting cols from raw data
    df2 = df2.rename(columns=usecols)  # rename them
    df2 = df2.applymap(lambda v: sanitize_inputs(v))  # sanitize clean inputs with bleach
    df2 = df2[new_cols]  # re-order cols
    return df2


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