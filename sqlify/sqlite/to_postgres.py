'''
.. currentmodule:: sqlify

SQLite to PostgreSQL Conversion
--------------------------------

.. autofunction:: sqlite_to_postgres

'''

from sqlify.core.table import Table
from sqlify.postgres.conn import postgres_connect
from sqlify.postgres import table_to_pg

import sqlite3
import psycopg2

def get_schema(database, table):
    ''' Get the schema of a SQLite table
    
    Arguments:
     * database:    Name of a file containing a SQLite database
     * table:       Name of a SQL table
    
    Returns a dictionary:
     * col_names:   List of column names
     * col_types:   List of column types
    '''
    
    col_names = []
    col_types = []
    
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    
    schema_query = "PRAGMA table_info({0})".format(table)
    schema_results = conn.execute(schema_query).fetchall()
        
    for row in schema_results:
        col_names.append(row['name'])
        col_types.append(row['type'])

    return {'col_names': col_names, 'col_types': col_types}

@postgres_connect
def sqlite_to_postgres(sqlite_db, name, conn=None, **kwargs):
    '''
    Parameters
    -----------
     * sqlite_db:   Name of the SQLite database
     * name:        Name of the table
     * host:        Host of the Postgres database
     * username:    Username for Postgres database
     * password:    Password for Postgres database
     
    The original SQLite table schema will be used for the new PostgreSQL table, and original SQLite data types will be converted according to this conversion table:

    +----------------+--------------------+
    | SQLite Type    | PostgreSQL Type    |
    +================+====================+
    | TEXT           | TEXT               |
    +----------------+--------------------+
    | INTEGER        | BIGINT             |
    +----------------+--------------------+
    | REAL           | DOUBLE PRECISION   |
    +----------------+--------------------+
    '''
    
    sqlite_schema = get_schema(database=sqlite_db, table=name)
    col_names = sqlite_schema['col_names']
    
    # Connect to SQL databases
    with sqlite3.connect(sqlite_db) as sqlite_conn:       
        sqlite_data = sqlite_conn.execute("SELECT * FROM {0}".format(name))

        while True:
            data_chunk = Table(dialect='postgres',
                name=name, col_names=col_names,
                # Apparently without a row argument fetchmany only 
                # gets 1 row at a time
                row_values=sqlite_data.fetchmany(10000))
                
            if data_chunk:
                table_to_pg(data_chunk, conn=conn)
            else:
                break