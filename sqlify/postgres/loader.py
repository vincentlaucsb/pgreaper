from sqlify._globals import SQLIFY_PATH, arg_parse
from sqlify.core import ColumnList, YieldTable, Table, assert_table
from sqlify.core._core import alias_kwargs, preprocess, sanitize_names
from sqlify.core.schema import DialectPostgres
from sqlify.zip import open, ZipReader

from .conn import *
from .database import PG_KEYWORDS, add_column, create_table, get_schema, \
    get_table_schema, get_pkey, get_primary_keys

from psycopg2 import sql as sql_string
from typing import Type
import functools
import psycopg2
import re
import os

def _find_rejects(func):
    ''' Makes wrapped function return a Table of rejected rows '''

    @functools.wraps(func)
    def inner(table, *args, **kwargs):
        rejects = table.find_reject()
        good_table = table.copy_attr(table, 
            row_values=[row for i, row in enumerate(table) if i not in rejects])
        reject_table = table.copy_attr(table)

        for i in rejects:
            sql_uploader.reject_table.append(table[i])
        
        func(table, *args, **kwargs)
        return reject_table
        
    return inner
    
def simple_copy(table, conn, null_values=None):
    '''
    Copy a Table into a Postgres database
     * Does not create table (should be done beforehand)
     * Does not make sure the schemas match (should be done beforehand)
     * Does not auto-commit
     
    Parameters
    -----------
    table:          Table
    null_values:    str
                    String representing null values
    conn:           psycopg2 connection
    '''
    
    if null_values:
        copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',', NULL '{1}')".format(
            table.name, null_values)
    else:
        copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',')".format(table.name)
        
    conn.cursor().copy_expert(copy_from, file=table.to_string())
    
def _split_table(func):
    '''
    Split table into a COPY and UPSERT table
     * Call simple_copy() on COPY table
     * Pass UPSERT table to function     
    '''

    @functools.wraps(func)
    def inner(table, conn, *args, **kwargs):
        current_ids = get_primary_keys(table.name, conn=conn)
        copy_table = table.copy_attr(table)
        upsert_table = table.copy_attr(table)
        p_key_index = table.p_key
        
        for row in table:
            if row[p_key_index] in current_ids:
                upsert_table.append(row)
            else:
                copy_table.append(row)

        simple_copy(copy_table, conn, *args, **arg_parse(simple_copy, kwargs))
        return func(upsert_table, conn, *args, **arg_parse(func, kwargs))
            
    return inner
    
@_split_table
def simple_upsert(table, conn, null_values=None, on_p_key=None):
    '''
    Like simple_copy() but performs an UPSERT
     * Gets list of primary keys
     * Splits Table into a COPY and UPSERT table
      * Call simple_copy() for COPY table
      * UPSERT the other table
      
    Parameters
    ------------
    on_p_key:   str, list, or None
                'nothing'     --> INSERT... ON CONFLICT DO NOTHING
                'replace'     --> Replace all columns of existing entries
                list of column names --> Replace all columns in list
    '''
    
    cur = conn.cursor()
    
    # Parse on_p_key argument
    if on_p_key == 'replace':
        on_p_key = table.col_names
    
    unnest_base = 'unnest(ARRAY{}::{type}[])'
    unnest = []  # List of unnest statements
    set_base = '{col} = excluded.{col}'
    set_ = []
    
    i = 1
    for col, col_type in zip(table.col_names, table.col_types):
        try:
            unnest.append(unnest_base.format(
                table[col], type=col_type.replace(' primary key', '')))
        except:
            import pdb; pdb.set_trace()
            
        # Determines the behavior of replacing existing rows
        if isinstance(on_p_key, list):
            if col in on_p_key:
                set_.append(set_base.format(col=col))
        i += 1
        
    # Generate UPSERT statement
    if on_p_key == 'nothing':
        upsert_statement = '''INSERT INTO {table_name}
            SELECT {unnest}
            ON CONFLICT DO NOTHING'''.format(
            table_name = table.name,
            unnest = ','.join(unnest))
    elif (on_p_key == 'replace') or (isinstance(on_p_key, list)):
        upsert_statement = '''INSERT INTO {table_name}
            SELECT {unnest}
            ON CONFLICT ({p_key}) DO UPDATE SET {set_}'''.format(
            table_name = table.name,
            unnest = ','.join(unnest),
            p_key = get_pkey(table.name, conn=conn).column,
            set_ = ','.join(set_))
    else:
        raise ValueError("'on_p_key' should be 'replace', a list, or None.")

    cur.execute(upsert_statement)
        
@preprocess
@postgres_connect
def file_to_pg(file, name, delimiter, verbose=True, conn=None,
    null_values=None, **kwargs):
    '''
    Reads a file in separate chunks (to conserve memory) and 
    loads it via the COPY FROM protocol
    
    Parameters
    -----------
    file:           str
                    Name of the file
    name:           str
                    Name of the table
    database:       str
                    Name of the PostgreSQL database.
                    If it doesn't exist, it will be created.
    header:         int
                     * The line number of the header row.                 
                        * Default: 0 (as in, line zero is the header)     
                     * `header=True` is equivalent to `header=0`          
                     * No header should be specified with `header=False`  
                       or `header=None`                                   
                        * **If `header > 0`, all lines before header are  
                          skipped**                                       
    skip_lines:     int
                    How many of the first n lines of the file to skip     
                     * Works independently of the **header** argument     
    delimiter:      str
                    How entries in the file are separated                 
                     * Defaults to '\\t' when using text_to_pg or          
                     * ',' when using csv_to_pg                           
    col_types:      list
                     * A list of column types                              
                     * If not specified, automatic type inference will     
                        be used
    verbose:        boolean
                    Print progress report
    '''
    
    with open(file, mode='r') as infile:
        for table in YieldTable(file=file, io=infile,
            delimiter=delimiter, engine='postgres', **kwargs):
            table_to_pg(table, name, null_values, conn=conn, commit=False,
                append=True, **kwargs)
            
    conn.commit()
    conn.close()

def _modify_tables(table, sql_cols, reorder=False,
    expand_input=False, expand_sql=False, conn=None):
    '''
    Performs the necessary operations to make the two table schemas the same
    
    Parameters
    -----------
    table:      Table
    sql_cols:   ColumnList
    conn:       psycopg2 connection
    '''
    
    def expand_table(table: Type[Table], final_cols: Type[ColumnList]):
        if expand_input:
            for col in (final_cols - table_cols):
                table.add_col(col, fill=None, type='text')

            return table.reorder(*final_cols.col_names)
        else:
            raise ValueError('')
    
    def expand_sql_table(final_cols): 
        if expand_sql:
            for name, type in (final_cols - sql_cols).as_tuples():
                conn.cursor().execute(
                    add_column(table.name, name, type))
        else:
            raise ValueError('')
    
    # TEMPORARY: In future, make Table objects have a ColumnList attribute
    table_cols = ColumnList(table.col_names, table.col_types)
    
    # Case 0: Do Nothing
    if (table_cols == sql_cols) == 2:
        return table
        
    # Case 1: Need to Reorder
    elif (table_cols == sql_cols) == 1:
        if reorder:
            table = table.reorder(*sql_cols.col_names)
        else:
            raise ValueError('')
        
    # Case 2: Need to Expand
    elif ((table_cols == sql_cols) == 0) and sql_cols:
        if table_cols < sql_cols:
            # Case 2a: Expand Table with NULLs
            table = expand_table(table, final_cols = sql_cols)
        elif table_cols > sql_cols:
            # Case 2b: Need to expand SQL table
            expand_sql_table(final_cols=table_cols)
        else:
            # Case 2c: Need to expand both
            # Note: Might fail due to column type mismatch
            final = sql_cols + table_cols
            table = expand_table(table, final)  # Expand Table
            expand_sql_table(final)                   # Expand SQL Table

    # Don't commit (See case 2c --> might need to rollback column additions)   
    return table
    
@assert_table(dialect=DialectPostgres())
@sanitize_names(reserved=PG_KEYWORDS)
@postgres_connect
def table_to_pg(
    table: Type[Table], name=None, null_values=None, conn=None, commit=True,
    on_p_key='nothing', append=False, reorder=False,
    expand_input=False, expand_sql=False,
    *args, **kwargs):
    '''
    Load a Table into a PostgreSQL database
    
    Parameters
    ----------
    table:          Table
                    The Table to be loaded
    database:       str
                    Name of a PostgreSQL database
    null_values:    str
                    String representing null values
    conn:           psycopg2.extensions.connection
    commit:         bool
                    Commit transaction and close connection
    on_p_key:       str
                    What to do if row conflicts with primary key constraint
    append:         bool
                    Simply use COPY to append to the table
    reorder:        bool
                    If input Table and SQL table have the same column names,
                    but in different orders, reorder input to match SQL table
                    
    Schema Modification
    --------------------
    expand_input:   bool
                    If input's columns are a subset of SQL table's columns,
                    should Table be "expanded" to fit
    expand_sql:     bool
                    If input has columns output does not have, should SQL table
                    be "expanded" to fit
    '''
    
    '''
    Load a Table into a PostgreSQL database by calling the necessary functions
     1. Gather schema data to determine if schemas match or will need to be modified
     2. Call the right functions to do said modifications
     3. Call either simple_copy() or simple_upsert()
    '''
    
    cur = conn.cursor()
    
    # Use table.name if name not provided
    if not name:
        name = table.name
    else:
        table.name = name
        
    # Check schemas
    schema = get_table_schema(name, conn=conn)
    p_key = get_pkey(name, conn=conn)
    
    # Modify Table and or SQL table if necessary
    table = _modify_tables(table, schema, reorder,
        expand_input=expand_input, expand_sql=expand_sql, conn=conn)
        
    # Create table if necessary
    if (not schema) or (not p_key) or append:
        cur.execute(create_table(table))
        simple_copy(table, conn, null_values)
    else:
        simple_upsert(table, conn, null_values, on_p_key=on_p_key, **kwargs)
        
    if commit:
        conn.commit()
        conn.close()