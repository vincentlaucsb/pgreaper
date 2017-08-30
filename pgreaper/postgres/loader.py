from pgreaper._globals import SQLIFY_PATH
from pgreaper.core import assert_table, ColumnList, Table, preprocess, sanitize_names
from pgreaper.core._from_text import clean_line
from pgreaper.zip import open, ZipReader

from .conn import *
from .database import add_column, create_table, get_schema, \
    get_table_schema, get_pkey, get_primary_keys

from psycopg2 import sql as sql_string
import psycopg2
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
    
    unnest_base = 'unnest(ARRAY{}::{type}[])'
    unnest = []  # List of unnest statements
    set_base = '{col} = excluded.{col}'
    set_ = []
    
    i = 1
    for col, col_type in zip(table.col_names, table.col_types):
        unnest.append(unnest_base.format(
            table[col], type=col_type.replace(' primary key', '')))
            
        # Determines the behavior of replacing existing rows
        if isinstance(on_p_key, list):
            if col in on_p_key:
                set_.append(set_base.format(col=col))
        i += 1
        
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

    # TEMPORARY: In the future, just exclude unnecessary columns from modification
    cur.execute(upsert_statement.replace('None', 'null'))

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
def table_to_pg(
    table, name=None, null_values=None, conn=None, commit=True,
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
        
    # Get the correct schema
    try:
        table.guess_type()
    except AttributeError:
        # Strongly-typed Tables don't have or need type guessing
        pass
        
    # Check schemas
    schema = get_table_schema(name, conn=conn)
    p_key = get_pkey(name, conn=conn)

    # Modify Table and or SQL table if necessary
    table = _modify_tables(table, schema, reorder=reorder,
        expand_input=expand_input, expand_sql=expand_sql, conn=conn)
        
    # Create table if necessary
    if not schema:
        cur.execute(create_table(table))
        
    # COPY or UPSERT
    if (not schema) or (not p_key) or append:
        simple_copy(table, conn=conn, null_values=null_values)
    else:
        simple_upsert(table, conn=conn, null_values=null_values,
            on_p_key=on_p_key)
        
    if commit:
        conn.commit()
        conn.close()