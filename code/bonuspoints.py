import pandas as pd
import os
import glob
import psycopg2
import bleach
from code.utils import connection_sparkifydb, bulk_copy

#TODO:
# - Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
# - Add data quality checks
# - Create a dashboard for analytic queries on your new database



