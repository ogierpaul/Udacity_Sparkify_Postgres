import pandas as pd


from code.utils import connection_sparkifydb, sanitize_inputs, primary_key_check

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
    data = [    ['foo', 'bar'],
                ['foo2', None]
    ]
    df = pd.DataFrame(data=data, columns=['foo', 'bar'])
    assert primary_key_check(df=df, key='foo').shape[0] == 2
    assert primary_key_check(df=df, key='bar').shape[0] == 1
    assert primary_key_check(df=df, key=['foo', 'bar']).shape[0] == 1

