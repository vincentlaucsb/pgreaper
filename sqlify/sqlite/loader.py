'''
.. currentmodule:: sqlify
.. autofunction:: file_to_sqlite
'''

# SQLite Uploaders

from sqlify.core import yield_table
from sqlify.core._core import sanitize_names

import sqlite3

def file_to_sqlite(file, database, type, delimiter, col_types=None, **kwargs):
    '''
    Loads a file via mass-insert statements.
    
    +--------------+------------------------------------------------------+
    | Arguments    | Description                                          |
    +==============+======================================================+
    | file         | Name of the file                                     |
    +--------------+------------------------------------------------------+
    | database     | Name of the SQLite database. If it doesn't exist, it |
    |              | will be created.                                     |
    +--------------+------------------------------------------------------+
    | header       | * The line number of the header row.                 |
    |              |    * Default: 0 (as in, line zero is the header)     |
    |              | * `header=True` is equivalent to `header=0`          |
    |              | * No header should be specified with `header=False`  |
    |              |   or `header=None`                                   |
    |              |    * **If `header > 0`, all lines before header are  |
    |              |      skipped**                                       |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | skip_lines   | How many of the first n lines of the file to skip    |
    |              |  * Works independently of the **header** argument    |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | delimiter    | How entries in the file are separated                |
    |              |  * Defaults to '\\t' when using text_to or            |
    |              |  * ',' when using csv_to                             |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | col_types    | * A list of column types                             |
    |              | * If not specified, automatic type inference will    |
    |              |    be used                                           |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | type         | * Type of the file ('text' or 'csv')                 |
    |              | * Should be passed in from csv_to_sqlite(), etc...   |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    
    '''
    
    for tbl in yield_table(file=file, type=type, delimiter=delimiter,
    **kwargs):
        if not col_types:
            # Guess column types if not provided
            col_types = tbl.guess_type()
            tbl.col_types = col_types

        table_to_sqlite(obj=tbl, database=database, **kwargs)    

@sanitize_names
def table_to_sqlite(obj, database, name=None, **kwargs):
    '''
    Load a Table into a SQLite database

    ==========  ===========================================
    Arguments   Description
    ==========  ===========================================
    obj         A Table object
    database    Name of SQLite database
    name        Name of SQLite table (default: table name)
    ==========  ===========================================

    .. note:: Fails if there are blank entries in primary key column
    '''
    
    conn = sqlite3.connect(database)
        
    # Create the table
    if name:
        table_name = name
    else:
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