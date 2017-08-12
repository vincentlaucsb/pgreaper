'''
.. currentmodule:: sqlify
.. autofunction:: file_to_sqlite
'''

# SQLite Uploaders

from sqlify.zip import ZipReader, open
from sqlify.core import chunk_file, assert_table, sanitize_names
from sqlify.core.schema import DialectSQLite

import sqlite3

def file_to_sqlite(file, database, delimiter, **kwargs):
    '''
    Loads a file via mass-insert statements.
    
    Parameters
    ------------
    file:           str
                    Name of the file                                      
    database:       str
                    Name of the SQLite database. If it doesn't exist, it  
                    will be created.                                      
    header:         int
                    The line number of the header row
                     - Default: 0 (as in, line zero is the header)      
                     - All lines beyond header are skipped   
    skip_lines:     str 
                    How many lines after header to skip
    delimiter:      str
                    How entries in the file are separated                 
                     - Defaults to '\\t' when using text_to or             
                     - ',' when using csv_to
    '''
    
    for table in chunk_file(file=file, delimiter=delimiter, **kwargs):
            table_to_sqlite(table, database=database, **kwargs)
            
@assert_table(dialect=DialectSQLite())
@sanitize_names
def table_to_sqlite(table, database, name=None, **kwargs):
    '''
    Load a Table into a SQLite database

    Parameters
    -----------
    table:      Table
    database:   str
                Name of SQLite database
    name:       str
                Name of SQLite table (default: table name)

    .. note:: Fails if there are blank entries in primary key column
    '''
    
    conn = sqlite3.connect(database)
        
    # Create the table
    if name:
        table_name = name
    else:
        table_name = table.name
        
    num_cols = len(table.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(table.col_names, table.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    conn.execute(create_table)    
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))

    conn.executemany(insert_into, table)
    
    conn.commit()
    conn.close()