import pandas as pd
import psycopg2

from code.utils import connection_sparkifydb, sanitize_inputs, primary_key_check, bulk_copy


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

    bulk_copy(df=df, tablename='test_foo', cur=cur, filename='test.csv')

    df2 = pd.read_sql('SELECT * FROM test_foo', con=conn)
    assert df2.shape[0] == 2
    cur.execute('DROP TABLE test_foo')

    conn.close()
