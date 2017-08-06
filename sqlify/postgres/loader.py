from sqlify._globals import SQLIFY_PATH
from sqlify.core import YieldTable
from sqlify.core._core import alias_kwargs, sanitize_names
from sqlify.core.table import Table
from sqlify.core.schema import DialectPostgres
from sqlify.zip import ZipReader
from .conn import *
from .schema import get_schema, get_pkey

import functools
import psycopg2
import re
import os

# Load Postgres reserved keywords
with open(os.path.join(
    SQLIFY_PATH, 'data', 'pg_keywords.txt'), mode='r') as PG_KEYWORDS:
    PG_KEYWORDS = set([kw.replace('\n', '').lower() for kw in PG_KEYWORDS.readlines()])

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
                    reject_tbl = Table(dialect='postgres',
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
@sanitize_names(reserved=PG_KEYWORDS)
@postgres_connect
def table_to_pg(obj, name=None, null_values=None, reorder=False, conn=None, **kwargs):
    '''
    Load a Table into a PostgreSQL database.
    
    Parameters
    ----------
    obj:            Table
                    The Table to be loaded
    database:       str
                    Name of a PostgreSQL database
    null_values:    str
                    String representing null values
    conn:           psycopg2.extensions.connection
    '''
    
    cur = conn.cursor()
    
    # Create the table
    if name:
        table_name = name
    else:
        table_name = obj.name
        
    # Reorder the table if needed
    if reorder:
        correct_order = get_schema(conn=conn)
        correct_order = correct_order.groupby('Table Name')[table_name]['Column Name']
        obj = obj.reorder(*correct_order)
        
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
            
@postgres_connect
def upsert_pg(obj, table=None, conn=None, *args, **kwargs):
    '''
    Upsert a Table to Postgres by:
     1. Identifying which rows are already in the database (via primary key)
     2. Splitting table into COPY and UPDATE tables
    '''
    
    if table is None:
        table = obj.name
    else:
        table = table
    
    try:
        p_key = psycopg2.sql.Identifier(
            get_pkey(table, database=database).column)
            
        # Temporary
        p_key_orig = get_pkey(table, database=database).column
    except AttributeError:
        raise ValueError('UPSERTs are only supported for tables with' +
            'primary keys.')
    
    cur = conn.cursor()
    cur.execute(psycopg2.sql.SQL('''
        SELECT {} FROM {}
        ''').format(p_key, psycopg2.sql.Identifier(table)))
        
    current_ids = set([i[0] for i in cur.fetchall()])
    
    # Split table into COPY and UPSERT tables
    copy_table = obj.copy_attr(obj)
    upsert_table = obj.copy_attr(obj)
    
    p_key_index = obj.p_key
    
    for row in obj:
        if row[p_key_index] in current_ids:
            upsert_table.append(row)
        else:
            copy_table.append(row)
            
    # Load COPY table
    table_to_pg(copy_table, name=table, database=database)
    
    unnest_base = 'unnest(ARRAY{}::{type}[])'
    unnest = []  # List of unnest statements
    set_base = '{col} = excluded.{col}'
    set_ = []
    
    i = 1
    
    for col, col_type in zip(upsert_table.col_names, upsert_table.col_types):
        unnest.append(unnest_base.format(
            upsert_table[col], type=col_type.replace(' PRIMARY KEY', '')))
        set_.append(set_base.format(col=col))
        
        i += 1
    
    sql_query = '''INSERT INTO {table_name}({col_names})
        SELECT {unnest}
        ON CONFLICT ({p_key}) DO UPDATE
        SET {set_}'''.format(
        table_name=table,
        col_names = ','.join(i for i in obj.col_names).replace(' PRIMARY KEY', ''),
        unnest = ','.join(unnest),
        p_key = p_key_orig,
        set_ = ','.join(set_)
    )

    cur.execute(sql_query)
    conn.commit()
    
    return current_ids