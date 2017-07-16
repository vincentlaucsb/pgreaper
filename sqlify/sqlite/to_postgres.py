'''
.. currentmodule:: sqlify

SQLite to PostgreSQL Conversion
--------------------------------

.. autofunction:: sqlite_to_postgres

'''

from sqlify.core import Table, PgTable
from sqlify.core._core import convert_schema
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

def sqlite_to_postgres(sqlite_db, pg_db, name,
    host="localhost", username=None, password=None):
    '''
    Arguments:
     * sqlite_db:   Name of the SQLite database
     * pg_db:       Name of the Postgres database
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
    pg_col_types = convert_schema(sqlite_schema['col_types'])
    
    # Connect to SQL databases
    with sqlite3.connect(sqlite_db) as sqlite_conn:       
        sqlite_data = sqlite_conn.execute("SELECT * FROM {0}".format(name))

        while True:
            data_chunk = PgTable(
                name=name,
                col_names=col_names,
                col_types=pg_col_types,
                
                # Apparently without a row argument fetchmany only 
                # gets 1 row at a time
                row_values=sqlite_data.fetchmany(10000))
                
            if data_chunk:
                table_to_pg(data_chunk, database=pg_db)
            else:
                break