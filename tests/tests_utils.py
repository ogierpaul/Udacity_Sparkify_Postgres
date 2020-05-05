import pandas as pd
import psycopg2
import pytest

from sparkify_pg_code.utils import connection_sparkifydb, sanitize_inputs, primary_key_check, bulk_copy


def test_conn():
    conn = connection_sparkifydb()
    cur = conn.cursor()
    cur.execute('SELECT 1 as res')
    result = cur.fetchone()[0]
    assert result == 1


def test_sanitize_inputs():
    for a in ['foo', 'bar', None, 1, 1.0]:
        assert sanitize_inputs(a) == a
    a = "an <script>evil()</script> example"
    assert sanitize_inputs(a) != a
    print(sanitize_inputs(a))


def test_primary_key_check():
    data = [['foo', 'bar'],
            ['foo2', None]
            ]
    df = pd.DataFrame(data=data, columns=['foo', 'bar'])
    assert primary_key_check(df=df, key='foo').shape[0] == 2
    assert primary_key_check(df=df, key='bar').shape[0] == 1
    assert primary_key_check(df=df, key=['foo', 'bar']).shape[0] == 1


def test_bulk_copy():
    data = [[1, 'foo', 'bar'],
            [2, 'foo2', None]
            ]
    df = pd.DataFrame(data=data, columns=['id', 'foo', 'bar'])
    df['id'] = df['id'].astype(int)

    conn = connection_sparkifydb()
    cur = conn.cursor()
    try:
        cur.execute('DROP TABLE test_foo')
    except psycopg2.Error as e:
        print("Error: Could not drop table test_foo")
        print(e)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS test_foo (
    id INTEGER,
    foo VARCHAR(10),
    bar VARCHAR(10),
    PRIMARY KEY (id))
    """)
    #UPLOAD once
    bulk_copy(df=df, tablename='test_foo', cur=cur, pkey='id')
    df2 = pd.read_sql('SELECT * FROM test_foo', con=conn)
    assert df2.shape[0] == 2

    #UPLOAD with UPSERT
    df_more = df.copy()
    row_more = pd.DataFrame(pd.Series(data=[3, 'mars', 'bon'], index=['id', 'foo', 'bar'])).transpose()
    assert row_more.shape[0] == 1
    df_more = pd.concat([df_more, row_more], axis=0)
    assert df_more.shape[0] == 3
    bulk_copy(df=df_more, tablename='test_foo', cur=cur, pkey='id')
    df2 = pd.read_sql('SELECT * FROM test_foo', con=conn)
    assert df2.shape[0] == 3
    #THE shape of df2 should be 3 because the first two rows are already existing

    cur.execute('DROP TABLE test_foo')
    conn.close()
