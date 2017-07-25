''' Functions with interacting with live PostgreSQL databases '''

from sqlify.core._core import alias_kwargs
from sqlify.core import PgTable
from .conn import postgres_connect

from collections import deque
from psycopg2 import sql
import psycopg2

class BulkRunner(object):
    ''' Mass executes SQL statements that have the same general structure '''
    
    def __init__(self, sql, union=False,
        database=None, username=None, password=None, host=None):
        '''
         * sql:      A SQL statement with Python string formatting tokens
         * union:    Create a single statement with multiple UNION clauses
        '''
        
        self.sql = sql + "\n"
        
        # Stores database connection settings
        self.db = {
            'database': database,
            'username': username,
            'password': password,
            'host': host
        }
        
        self.queue = []
        
    def load(self, *args, **kwargs):
        self.queue.append({
            'args': args,
            'kwargs': kwargs
        })
        
    def create_query(self):
        ''' Create a SQL query '''
        sql_query = "UNION\n".join(self.sql.format(*args['args'], **args['kwargs']) for args in self.queue)

        return sql_query
        
    def print_query(self):
        ''' Print the SQL query generated '''
        
        print(self.create_query())
        
def agg_database(list_, *args):
    '''
    Generate a query that aggregates many columns from many tables in a database
    
    Arguments:
     * list_:     A list of columns to apply aggregate functions to
        * Should be in this format
        
        +------------------+----------------------------+
        | table name       | column name                |
        +------------------+----------------------------+
        
     * args:     List of aggregate functions to apply
    '''
    
    select_queries = []
    
    for table, col in list_:
        # Expand list of aggregate functions
        agg_part = ", ".join(['{func}({col})'.format(
            func=f, col=col) for f in args])
        
        # Add select query    
        select_queries.append("SELECT {0} FROM {1}\n\tGROUP BY {2}".format(
            agg_part, table, col))
    
    return '\nUNION\n'.join(select_queries)
    
@alias_kwargs
def get_schema(database=None, username=None, password=None, host=None):
    ''' Get a database schema from Postgres in a Table '''
    
    conn = postgres_connect(database, username, password, host)
    cur = conn.cursor()
    
    cur.execute(sql.SQL('''
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema LIKE '%public%'
    '''))
    
    return PgTable(
        name="{} Schema".format(database),
        col_names=["Table Name", "Column Name", "Data Type"],
        row_values=[i for i in cur.fetchall()])
        
def table_exists(table_name,
    conn=None, engine=None,
    database=None, username=None, password=None, host=None):
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
        sql.SQL(table_name))
    )
    
    return cur.fetchall()