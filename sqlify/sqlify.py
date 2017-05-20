from sqlify.settings import *
from sqlify.postgres.fast_loader import PgLoader, text_to_postgres, \
    csv_to_postgres
from sqlify.helpers import _sanitize_table, _preprocess
from sqlify.table import Table, subset
from sqlify.readers import yield_table, head_table

import sqlite3

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
        # import pdb; pdb.set_trace()
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
    
    # import pdb; pdb.set_trace()
    
    if engine == 'sqlite':
        single_table_to_sqlite(obj, database)
    elif engine == 'postgres':
        single_table_to_postgres(obj, database)
    else:
        raise ValueError("Please select either 'sqlite' or 'postgres' as your database engine.")

def single_table_to_sqlite(obj, database):
    '''
    Notes:
     * Fails if there are blank entries in primary key column
    '''
    
    conn = sqlite3.connect(database)
        
    # Create the table
    table_name = obj.name
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    # import pdb; pdb.set_trace()
    
    # TO DO: Strip "-" from table names
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    conn.execute(create_table)    
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))

    # import pdb; pdb.set_trace()
        
    conn.executemany(insert_into, obj)
    
    conn.commit()
    conn.close()
        
# Convert text file to SQL
def text_to_sql(file, database, name, engine='sqlite', *args, **kwargs):
    '''
    Arguments:
     * file:      Data file
     * database:  sqlite3 database to store in
     * name:      Name of the SQL table (default: name of the file)
      * Alternatively, name can be a dictionary where keys are table names and values are column names or indices
     * p_key:     Specifies column index to be used as a used as a primary
                  key for all tables
     * header:    True if first row is a row of column names
     * skip_lines: The number of lines to skip
     * delimiter: Delimiter of the data
     * col_types: Column types
    '''
    
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ' '
        
    file_to_sql(file=file, database=database, name=name,
        engine=engine, type='text', **kwargs)
            
# Convert CSV file to SQL
def csv_to_sql(file, database, name, engine='sqlite', *args, **kwargs):
    ''' Arguments: See text_to_sql() comments '''
    
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','
    
    file_to_sql(file=file, database=database, name=name,
        engine=engine, type='csv', **kwargs)
        
# Helper function for text_to_sql() and csv_to_sql()
def file_to_sql(file, database, delimiter, type, engine, 
    skip_lines=None, **kwargs):
    ''' Arguments: See text_to_sql() comments '''

    if engine == 'sqlite':
        # Use the lazy loader
        for tbl in yield_table(file=file, type=type, delimiter=delimiter,
            **kwargs):
            table_to_sql(obj=tbl, database=database, **kwargs)
    elif engine == 'postgres':
        if type == 'text':
            loader_func = text_to_postgres
        elif type == 'csv':
            loader_func = csv_to_postgres
    
        # Use the Postgres COPY command
        PgLoader(file=file,
                 database=database,
                 type=type,
                 delimiter=delimiter,
                 skip_lines=skip_lines,
                 **kwargs).load_data(loader_func)
    else:
        raise ValueError("Please select either 'sqlite' or 'postgres' as your database engine.")
        
# Load entire text file to Table object
def text_to_table(file, **kwargs):
    temp = yield_table(file, chunk_size=None, **kwargs)

    return_tbl = None
    
    # Only "looping" once to retrieve the only Table
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl
        
# Load entire CSV file to Table object
def csv_to_table(file, **kwargs):
    temp = yield_table(file, type='csv', chunk_size=None, **kwargs)
    
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl

# Alias for Postgres commands
def text_to_pg(*args, **kwargs):
    text_to_sql(*args, **kwargs, engine='postgres')
    
def csv_to_pg(*args, **kwargs):
    csv_to_sql(*args, **kwargs, engine='postgres')