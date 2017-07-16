from sqlify.core import yield_table, Table, PgTable
from sqlify.core._core import sanitize_names
from sqlify.core.tabulate import Tabulate
from .conn import *

import functools
import psycopg2
import re
import os

def _assert_pgtable(func):
    ''' Makes sure the object is a Table or PgTable '''
    
    @functools.wraps(func)
    def inner(obj, *args, **kwargs):
        # args[0]: Table object      
        if not isinstance(obj, PgTable):
            if isinstance(obj, Table):
                obj = Tabulate.as_pgtable(obj)
            else:
                raise ValueError('This function only works for Table or PgTable objects.')
                
        return func(obj, *args, **kwargs)
        
    return inner

def file_to_pg(file, database, type, delimiter, col_types=None, **kwargs):
    '''
    Reads a file in separate chunks (to conserve memory) and 
    loads it via the COPY FROM protocol
    
    Necessary connection arguments such as:
     * username
     * password
     * database
     * host
     
    should be specified as well in addition to arguments below.
    Only `database` is required. For all others,
    the default settings in `sqlify.settings()` will be used.
    
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
    |              |  * Defaults to '\\t' when using text_to_pg or         |
    |              |  * ',' when using csv_to_pg                          |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | col_types    | * A list of column types                             |
    |              | * If not specified, automatic type inference will    |
    |              |    be used                                           |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    | type         | * Type of the file ('text' or 'csv')                 |
    |              | * Should be passed in from text_to_pg or csv_to_pg() |
    |              |                                                      |
    +--------------+------------------------------------------------------+
    
    '''
        
    # Table of rejects
    reject_tbl = None

    for tbl in yield_table(file=file, type=type, delimiter=delimiter,
        engine='postgres', **kwargs):
    
        # import pdb; pdb.set_trace()
        
        if not col_types:
            # Guess column types if not provided
            col_types = tbl.guess_type()
            tbl.col_types = col_types
        
        try_to_load = table_to_pg(obj=tbl, database=database, **kwargs)
        
        ''' If unsuccessful, then try_to_load is equal to the index of
            the erroneous line'''
        while try_to_load >= 0:
            if not reject_tbl:
                reject_tbl = PgTable(
                    name=tbl.name + "_reject", col_names=tbl.col_names,
                    col_types='TEXT')
            
            # Load non-erroneous lines
            table_to_pg(obj=tbl[:try_to_load], database=database, **kwargs)
            
            # Add erroneous line to list of rejects
            reject_tbl.append(tbl[try_to_load])
            
            # Try to load lines after reject line
            tbl = tbl[try_to_load + 1:]
            try_to_load = table_to_pg(obj=tbl, database=database, **kwargs)
            
    # Load rejects (if there are any)
    if reject_tbl:
        table_to_pg(obj=reject_tbl, database=database)

@_assert_pgtable
@sanitize_names
def table_to_pg(
    obj, database, null_values=None, name=None,
    username=None, password=None, host="localhost", **kwargs):
    
    '''
    Load a Table into a PostgreSQL database.
    
    Arguments:
     * obj:         A table object
     * database:    Name of a PostgreSQL database
     * null_values: String representing null values
     * username
     * password
     * host:        Default: localhost
    '''

    conn = postgres_connect(database, username, password, host)
    cur = conn.cursor()
    
    # Create the table
    if name:
        table_name = name
    else:
        table_name = obj.name
        
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
        
    for col_name, type in cols_zip:
        cols.append("{0} {1}".format(col_name, type))
        
    # Create table
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
        table_name, ", ".join(cols))
    
    cur.execute(create_table)
    
    try:
        # Insert Table via copy_from()
        if null_values:
            cur.copy_expert(
                "COPY {0} FROM STDIN (DELIMITER '\t', NULL '{1}')".format(table_name, null_values),
                file=obj)
        else:
            cur.copy_from(obj, table_name, sep='\t')
            
        conn.commit()
        return -1
        
    except psycopg2.DataError as e:
        ''' Return line number where error occurred
            (Subtract 1 because SQL line numbers are not zero-indexed)      
        '''
        return int(re.search('COPY .* line (?P<lineno>[0-9]+)\, column',
            str(e)).group('lineno')) - 1