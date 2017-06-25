''' Loads a table from a SQLite database into a Postgres database '''

from sqlify.postgres.table import PgTable
from sqlify.postgres.loader import table_to_pg

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
    
def convert_schema(types):
    '''  
    Convert SQLite column types to Postgres column types
    
    Argument can either be a string of a list of strings
    '''
    
    def convert_type(type):
        ''' Takes in a SQLite data type (string) and returns Postgres equiv. '''
        
        convert = {
            'integer': 'bigint',
            'real':    'double precision'
        }
        
        try:
            return convert[type]
        except KeyError:
            return type
    
    if isinstance(types, str):
        return convert_type(type)
    elif isinstance(types, list):
        return [convert_type(i) for i in types]
    else:
        raise ValueError('Argument must either be a string or a list of strings.')
    
    return types

def sqlite_to_postgres(sqlite_db, pg_db, name,
    host="localhost", username=None, password=None):
    '''
    Arguments:
     * sqlite_db:   Name of the SQLite database
     * pg_db:       Name of the Postgres database
     * name:        Name of the table
     * host:        Host of the Postgres database
     * username:        Username for Postgres database
     * password:    Password for Postgres database
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