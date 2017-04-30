from sqlify.settings import *
from sqlify.postgres import postgres_connect_default
from sqlify.helpers import _sanitize_table, _preprocess
from sqlify.table import Table, subset, type_check
from sqlify.readers import yield_table

import sqlite3
import psycopg2

from psycopg2.extras import execute_batch

# Store a Table object in SQL database
def table_to_sql(obj, database, name='', **kwargs):
    # Create multiple tables based on name dictionary
    if isinstance(name, dict):
        for table_name, columns in zip(name.keys(), name.values()):
            if isinstance(columns, list):
                new_tbl = subset(obj, *columns, name=table_name)
            else:
                new_tbl = subset(obj, columns, name=table_name)
            
            _sanitize_table(new_tbl)
            single_table_to_sql(obj=new_tbl, database=database, **kwargs)

    else:
        _sanitize_table(obj)
        single_table_to_sql(obj, database, **kwargs)

# Store a single Table object in SQL database
def single_table_to_sql(obj, database, engine='sqlite', **kwargs):
    '''
    Arguments:
     * obj:        Table object
     * database:   Database file
     * check_type: Print out warnings if data entries do not match
                   column type
    '''
    
    if engine == 'sqlite':
        single_table_to_sqlite(obj, database)
    elif engine == 'postgres':
        single_table_to_postgres(obj, database)
    else:
        raise(ValueError, "Please select either 'sqlite' or 'postgres' as your database engine.")

def single_table_to_sqlite(obj, database):
    conn = sqlite3.connect(database)
        
    # Create the table
    table_name = obj.name
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    conn.execute(create_table)    
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))

    conn.executemany(insert_into, obj)
    
    conn.commit()
    conn.close()

def single_table_to_postgres(obj, database, username=None, password=None):
    # Connect to default Postgres database to create a new database
    base_conn = postgres_connect_default()
    
    # Create database if not exists
    try:
        base_conn.execute('CREATE DATABASE {0}'.format(database))
    except psycopg2.ProgrammingError:
        pass

    if not username:
        username = POSTGRES_DEFAULT_USER
    if not password:
        password = POSTGRES_DEFAULT_PASSWORD
    
    conn = psycopg2.connect("dbname={0} user={1} password={2}".format(
        database, username, password))
    cur = conn.cursor()
    
    # Create the table
    table_name = obj.name
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    cur.execute(create_table)
    
    # Prepare
    cur.execute('''
        PREPARE massinsert ({col_types}) AS
            INSERT INTO {table_name} VALUES({values});
        '''.format(
            table_name = table_name,
            col_types = ",".join(i.replace(' PRIMARY KEY', '') for i in obj.col_types),
            values = ",".join(['$' + str(i) for i in range(1, num_cols + 1)])
            )
    )
    
    # Insert columns
    insert_into = "EXECUTE massinsert({values})".format(
        values=",".join(['%s' for i in range(0, num_cols)]))
    
    execute_batch(cur, insert_into, obj)
    
    # Remove prepared statement
    cur.execute("DEALLOCATE massinsert")
    
    conn.commit()
    
    cur.close()
    conn.close()
        
# Convert text file to SQL
@_preprocess
def text_to_sql(file, database, *args, **kwargs):
    '''
    Arguments:
     * file:      Data file
     * database:  sqlite3 database to store in
     * name:      Name of the SQL table (default: name of the file)
      * Alternatively, name can be a dictionary where keys are table names and values are column names or indices
     * p_key:     Specifies column index to be used as a used as a primary
                  key for all tables
     * header:    True if first row is a row of column names
     * delimiter: Delimiter of the data
     * col_types: Column types
    '''
    
    for tbl in yield_table(file=file, *args, **kwargs):
        table_to_sql(obj=tbl, database=database, **kwargs)

# Convert CSV file to SQL
@_preprocess
def csv_to_sql(file, database, *args, **kwargs):
    for tbl in yield_table(file, database, type='csv', *args, **kwargs):
        table_to_sql(obj=tbl, database=database, **kwargs)
        
# Load entire text file to Table object
@_preprocess
def text_to_table(file, name, **kwargs):
    temp = yield_table(file, name, chunk_size=None, **kwargs)

    return_tbl = None
    
    # Only "looping" once to retrieve the only Table
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl
        
# Load entire CSV file to Table object
@_preprocess
def csv_to_table(file, name, **kwargs):
    temp = yield_table(file, name, type='csv', chunk_size=None, **kwargs)
    
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl