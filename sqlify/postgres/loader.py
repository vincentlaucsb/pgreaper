from sqlify.core import YieldTable, Table
from sqlify.core._core import alias_kwargs, sanitize_names
from sqlify.core.schema import DialectPostgres
from sqlify.core.tabulate import Tabulate
from sqlify.zip import ZipReader
from .conn import *

import functools
import psycopg2
import re
import os

def _assert_pgtable(func):
    ''' Makes sure the object is a Table object with dialect Postgres'''
    
    @functools.wraps(func)
    def inner(obj, *args, **kwargs):
        # args[0]: Table object      
        if not isinstance(obj, Table):
            raise ValueError('This function only works for Table objects.')
        else:
            if str(obj.dialect) == 'sqlite':
                # This also automatically converts the schema
                obj.dialect = DialectPostgres()
                
        return func(obj, *args, **kwargs)
        
    return inner

def file_to_pg(file, database, delimiter, verbose=True, **kwargs):
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
    | verbose      | * Print progress report                              |
    +--------------+------------------------------------------------------+
    '''
    
    def load_file():
         # Table of rejects
        reject_tbl = None
    
        file_chunker = YieldTable(file=file, io=infile, delimiter=delimiter,
            engine='postgres', **kwargs)
    
        for tbl in file_chunker:
            rejects = tbl.find_reject()
            good_table = tbl.copy_attr(tbl,
                row_values=[row for i, row in enumerate(tbl) if i not in rejects]
            )
            table_to_pg(obj=good_table, database=database, **kwargs)
            
            while rejects:
                if not reject_tbl:
                    reject_tbl = Tabulate.factory(engine='postgres',
                        name=tbl.name + "_reject",
                        col_names=tbl.col_names, col_types='TEXT')
                
                reject_tbl.append(tbl[rejects.pop()])
                
            if verbose:
                print('Loaded {} records so far.'.format(file_chunker.line_num))
                
        # Load rejects (if there are any)
        if reject_tbl:
            table_to_pg(obj=reject_tbl, database=database)

    if isinstance(file, str):
        with open(file, mode='r') as infile:
            load_file()
    elif isinstance(file, ZipReader):
        with file as infile:
            load_file()
    
@_assert_pgtable
@sanitize_names
@alias_kwargs
def table_to_pg(
    obj, database=None, null_values=None, name=None,
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
    
    # Insert Table via copy_from()
    if null_values:
        cur.copy_expert(
            "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',', NULL '{1}')".format(table_name, null_values),
            file=obj.to_string())
    else:
        cur.copy_expert(
            "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',')".format(table_name, null_values),
            file=obj.to_string())
        
    conn.commit()
    return -1
        
    # except psycopg2.DataError as e:
        # ''' Return line number where error occurred
            # (Subtract 1 because SQL line numbers are not zero-indexed)      
        # '''

        # return int(re.search('COPY .* line (?P<lineno>[0-9]+)\, column',
            # str(e)).group('lineno')) - 1