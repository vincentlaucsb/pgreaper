''' Functions with interacting with live PostgreSQL databases '''

from sqlify.core.tabulate import Tabulate
from .conn import postgres_connect

from psycopg2.sql import SQL as SQLStr
import psycopg2


@alias_kwargs
def get_schema_pg(database=None, username=None, password=None, host=None):
    ''' Get a database schema from Postgres in a Table '''
    
    conn = postgres_connect(database, username, password, host)
    
    cur = conn.cur()
    
    cur.execute(SQLStr('''
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema LIKE '%public%'
    '''))
    
    return cur.fetchall()