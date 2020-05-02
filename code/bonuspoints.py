import pandas as pd
import os
import glob
import psycopg2
import bleach
from code.utils import connection_sparkifydb

def etl_with_copy():
    conn = connection_sparkifydb()
    cur = conn.cursor()
    #TODO: COMPLETE
    conn.close()
    pass

