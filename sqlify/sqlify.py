from sqlify.settings import *
from sqlify.helpers import _sanitize_table, _preprocess

from sqlify.sqlite.loader import file_to_sqlite, table_to_sqlite
from sqlify.postgres.loader import file_to_pg, table_to_pg

# Convert text file to SQL
def text_to_sql(file, database, name, engine='sqlite', *args, **kwargs):
    '''
    Arguments:
     * file:      Data file
     * database:  sqlite3 database to store in
     * name:      Name of the SQL table (default: name of the file)
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
        file_to_sqlite(file=file, database=database, type=type,
                   delimiter=delimiter, **kwargs)
    elif engine == 'postgres':
        file_to_pg(file=file, database=database, type=type,
                   delimiter=delimiter, **kwargs)
    else:
        raise ValueError("Please select either 'sqlite' or 'postgres' as your database engine.")
        
# Alias for Postgres commands
def text_to_pg(*args, **kwargs):
    text_to_sql(*args, **kwargs, engine='postgres')
    
def csv_to_pg(*args, **kwargs):
    csv_to_sql(*args, **kwargs, engine='postgres')