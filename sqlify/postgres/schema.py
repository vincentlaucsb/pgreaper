from sqlify.core._core import alias_kwargs
from sqlify.config import PG_DEFAULTS

from psycopg2 import sql

@alias_kwargs
def table_exists(table, conn=None, engine=None,
    database=PG_DEFAULTS['database'], username=PG_DEFAULTS['username'],
    password=PG_DEFAULTS['password'], host=PG_DEFAULTS['host']):
    
    '''
    If a Table exists, return its column names and types.
    Otherwise, return None.
    
    Arguments:
     * Option 1:    Pass in psycopg2 connection or SQLAlchemy Engine via conn
     * Option 2:    Pass in connection options with database, username, etc.    
    '''
    
    if engine:
        conn = engine.connect()
        dbapi_conn = conn.connection
        cur = dbapi_conn.cursor()
    else:
        if not conn:
            conn = postgres_connect(database, username, password, host)
            
        cur = conn.cursor()
    
    # Credit: https://stackoverflow.com/questions/20582500/how-to-check-if-a-table-exists-in-a-given-schema
    cur.execute(sql.SQL("\
        SELECT column_name, data_type\
            FROM information_schema.columns\
            WHERE table_schema LIKE 'public'\
            AND table_name LIKE '{}';").format(
        sql.SQL(table))
    )
    
    return cur.fetchall()