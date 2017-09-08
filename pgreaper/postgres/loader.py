from pgreaper._globals import SQLIFY_PATH, preprocess
from pgreaper.core import assert_table, ColumnList, Table
from pgreaper.io.csv_reader import clean_line
from pgreaper.io.zip import open, ZipReader
from .conn import *
from .database import add_column, create_table, get_schema, \
    get_table_schema, get_pkey, get_primary_keys

from psycopg2 import sql as sql_string
import psycopg2
import json
import csv
import io

####################
# Helper Functions #
####################

def _read_stringio(io_obj, table_obj):
    '''
    Reads a StringIO object into a Table
    
    Parameters
    ----------
    io_obj:     StringIO
    table_obj:  Table    
    '''
    
    reader = csv.reader(io_obj)
    
    for i in reader:
        clean_line(i, table_obj)

def simple_copy(data, conn, name=None, null_values=None):
    '''
    Copy a Table into a Postgres database
     * Does not create table (should be done beforehand)
     * Does not make sure the schemas match (should be done beforehand)
     * Does not auto-commit
     
    Parameters
    -----------
    data:           Table or StringIO
    name:           str
                    Name of the Table to COPY to
    null_values:    str
                    String representing null values
    conn:           psycopg2 connection
    '''
    
    if isinstance(data, Table):
        name = data.name
        stringio_ = data.to_string()
    elif isinstance(data, io.StringIO):
        stringio_ = data
    else:
        raise ValueError("'data' argument must either be a Table or StringIO object.")
        
    if null_values:
        copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',', NULL '{1}')".format(name, null_values)
    else:
        copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',')".format(name)
        
    stringio_.seek(0)
    conn.cursor().copy_expert(copy_from, file=stringio_)

def _unnest(table):
    '''
    Given a Table create a list of unnest() statements, like
    
    col1  col2
    ----- -----
    a     c
    b     d
    
    becomes
    
    SELECT unnest(ARRAY['a', 'b']), unnest(ARRAY['c', 'd']);    
    '''
    
    base = "unnest(ARRAY{}::{type}[])"
    dict_encoder = json.JSONEncoder()
    unnest = []
    
    for col, col_type in zip(table.col_names, table.col_types):
        col_type = col_type.replace(' primary key', '')
    
        if col_type in ['jsonb']:
            unnest.append(base.format(
                [dict_encoder.encode(i) for i in table[col]],
                type=col_type))
        elif col_type == 'datetime':
            unnest.append(base.format(
                [psycopg2.extensions.adapt(i) for i in table[col]]),
                type=col_type)
        elif col_type == 'text':
            # Deal with embedded quotes like "Ted O'Donnell"
            unnest.append(base.format(
                [i.replace("'", "''") if i else i for i in table[col]],
                type=col_type).replace('"', "'"))
        else:
            unnest.append(base.format(table[col], type=col_type))
                    
    return unnest
    
def simple_upsert(table, conn, null_values=None, on_p_key='nothing'):
    '''
    Like simple_copy() but performs an UPSERT
      
    Parameters
    ------------
    on_p_key:   str, list, or None
                'nothing'     --> INSERT... ON CONFLICT DO NOTHING
                'replace'     --> Replace all columns of existing entries
                list of column names --> Replace all columns in list
    conn:       psycopg2 Connection
    '''
    
    cur = conn.cursor()
    
    # Parse on_p_key argument
    if on_p_key == 'replace':
        on_p_key = table.col_names
    
    unnest = _unnest(table)  # List of unnest statements
    set_base = '{col} = excluded.{col}'
    set_ = []
    
    for col, col_type in zip(table.col_names, table.col_types):
        # Determines the behavior of replacing existing rows
        if isinstance(on_p_key, list):
            if col in on_p_key:
                set_.append(set_base.format(col=col))
        
    # Generate UPSERT statement
    # TODO: Make it so this branch doesn't even need to be called
    # Should just have _split_table delete the COPY table
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

    cur.execute(upsert_statement.replace('None', 'null'))

def _modify_tables(table, sql_cols, reorder=False,
    expand_input=False, expand_sql=False, alter_types=False, conn=None):
    '''
    Performs the necessary operations to make the two table schemas the same
    
    Parameters
    -----------
    table:      Table
    sql_cols:   ColumnList
    conn:       psycopg2 connection
    '''
    
    def expand_table(table, final_cols):
        '''
        Parameters
        ------------
        table:      Table
        final_cols  ColumnList
        '''
        
        if expand_input:
            for col in (final_cols - table_cols):
                table.add_col(col, fill=None)

            return table.reorder(*final_cols.col_names)
        else:
            raise ValueError('Need to expand input, but expand input is false.')
    
    def expand_sql_table(final_cols): 
        if expand_sql:
            for name, type in (final_cols - sql_cols).as_tuples():
                conn.cursor().execute(
                    add_column(table.name, name, type))
        else:
            raise ValueError("The input table has columns that the SQL table does not. "
            "If you would like to add the extra columns, please set "
            "'expand_sql=True'.")
    
    table_cols = table.columns.sanitized
    
    # Alter data types of existing columns if necessary
    diff = table_cols/sql_cols
    if diff:
        if alter_types:
            for name, type in diff.as_tuples():
                conn.cursor().execute(alter_column_type(table, name,
                    type))
        else:
            raise ValueError('Incompatible data types.')
    
    # Case 0: Same column names in same order
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
            # Case 2a: Input has less columns
            table = expand_table(table, final_cols = sql_cols)
        elif table_cols > sql_cols:
            # Case 2b: Need to expand SQL table
            expand_sql_table(final_cols=table_cols)
        else:
            # Case 2c: Need to expand both
            # Note: Might fail due to column type mismatch
            final = sql_cols + table_cols
            table = expand_table(table, final)  # Expand Table
            expand_sql_table(final)             # Expand SQL Table

    # Don't commit (See case 2c --> might need to rollback column additions)   
    return table
    
@assert_table(dialect='postgres')
@postgres_connect
def copy_table(
    table, name=None, null_values=None, conn=None, commit=True,
    on_p_key='nothing', append=False, reorder=False,
    expand_input=False, alter_types=False, expand_sql=False,
    *args, **kwargs):
    '''
    Load a Table into a PostgreSQL database. Although the function has the word
    "copy" in it, it actually automatically performs an INSERT OR REPLACE or UPSERT
    if the destination table already exists. Read on for more details.
    
    .. note:: While PGReaper can easily add new columns for you, it
       will never modify existing schema unless you explictly choose to
    
    Limitations:
        If Table already exists, does not modify column types if they conflict
    
    Parameters:
        table:          Table
                        The Table to be loaded
        null_values:    str (default: None)
                        String representing null values
        conn:           psycopg2.extensions.connection
                        A connection created by `psycopg2.connect()`.
                        Alternatively, you can specify one or more of 
                        `dbname, host, user, and password`.
        commit:         bool (default: True)
                        Commit transaction and close connection once finished
                        
    Input Modification:
        reorder:        bool (default: False)
                        If input Table and SQL table have the same column names,
                        but in different orders, reorder input to match SQL table
        expand_input:   bool (default: False)
                        If input's columns are a subset of SQL table's columns,
                        fill in missing columns with nulls
                        
    SQL Schema Modification:
        expand_sql:     bool (default: False)
                        Add new columns to SQL table if necessary
        alter_types:    bool (default: False)
                        Change SQL table column types if necessary to load table        
                    
    COPY Arguments:
        append:         bool (default: False)
                        If the destination table already exists, use `COPY` to
                        upload append to it. This may fail due to primary key
                        conflicts and other constraints.
                    
    INSERT OR REPLACE and UPSERT Arguments:
        on_p_key:       'nothing', 'replace' or list[str] (default: 'nothing')
                        What to do if a record conflicts with primary key constraint
                         * nothing: INSERT... ON CONFLICT DO NOTHING
                         * replace: INSERT OR REPLACE
                         * list[str]: A list of column names to update (INSERT... ON CONFLICT SET...)
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
        
    table.guess_type()
        
    # Check schemas
    schema = get_table_schema(name, conn=conn)
    p_key = get_pkey(name, conn=conn)
        
    # Create table if necessary
    if not schema:
        cur.execute(create_table(table))
    else:
        # Modify Table and or SQL table if necessary
        table = _modify_tables(
            table, schema, reorder=reorder,
            expand_input=expand_input, expand_sql=expand_sql,
            alter_types=alter_types, conn=conn)
        
    # COPY or UPSERT
    if (not schema) or (not p_key) or append:
        simple_copy(table, conn=conn, null_values=null_values)
    else:
        simple_upsert(table, conn=conn, null_values=null_values,
            on_p_key=on_p_key)
        
    if commit:
        conn.commit()
        conn.close()
        
def table_to_pg(*args, **kwargs):
    ''' Alias for `copy_table()` '''
    copy_table(*args, **kwargs)