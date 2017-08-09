''' Functions with interacting with live PostgreSQL databases '''

from sqlify._globals import SQLIFY_PATH
from sqlify.core import ColumnList
from sqlify.core._core import alias_kwargs
from sqlify.core.table import Table
from .conn import postgres_connect

from collections import deque, namedtuple
from psycopg2 import sql
import psycopg2
import os
import sys
import csv

# Load Postgres reserved keywords
with open(os.path.join(
    SQLIFY_PATH, 'data', 'pg_keywords.txt'), mode='r') as PG_KEYWORDS:
    PG_KEYWORDS = set([kw.replace('\n', '').lower() for kw in PG_KEYWORDS.readlines()])

def add_column(table_name, name, type):
    ''' Generate a ALTER TABLE ADD COLUMN statement '''
    return "ALTER TABLE {} ADD COLUMN {} {}".format(
        table_name, name, type)

def _create_table(table_name, col_names, col_types):
    cols = ["{0} {1}".format(col_name, type) for col_name, type in zip(col_names, col_types)]
    
    return "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
        table_name, ", ".join(cols))
    
def create_table(*args, **kwargs):
    ''' Generate a create_table statement '''
    
    if isinstance(args[0], Table):
        table = args[0]
    
        return _create_table(table.name, table.col_names, table.col_types)
    else:
        return _create_table(*args, **kwargs)

@postgres_connect
def get_schema(conn=None, **kwargs):
    '''
    Get a database schema from Postgres in a Table
    
    Returns a Table with columns:
     1. Table Name
     2. Column Name
     3. Data Type
    '''
    
    cur = conn.cursor()
    cur.execute(sql.SQL('''
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema LIKE '%public%'
    '''))
    
    return Table(
        dialect='postgres',
        name="Schema",
        col_names=["Table Name", "Column Name", "Data Type"],
        row_values=[list(i) for i in cur.fetchall()])
        
@postgres_connect
def get_pkey(table, conn=None, **kwargs):
    '''
    Return the primary key column for a table as a named tuple with fields
    "column" and "type"
    
    If no primary key, return None
    
    Ref: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    '''
    
    p_key = namedtuple('PrimaryKey', ['column', 'type'])
    cur = conn.cursor()
    
    try:
        cur.execute(sql.SQL('''
            SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                 AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = {}::regclass
            AND    i.indisprimary;
        ''').format(
            sql.Literal(table)))
        
        data = cur.fetchall()[0]
        return p_key(column=data[0], type=data[1])
    except IndexError:
        return None
    except (psycopg2.ProgrammingError, psycopg2.InternalError) as e:
        conn.rollback()
        return None
        
@postgres_connect
def get_primary_keys(table, conn):
    '''
    Return a set of primary keys from table
    
    Parameters
    -----------
    table:      str
                Name of table    
    conn:       psycopg2 connection   
    '''

    pkey_name = get_pkey(table, conn=conn).column
    cur = conn.cursor()
    cur.execute('''SELECT {} FROM {}'''.format(
        pkey_name, table))
    return set([i[0] for i in cur.fetchall()])

# Check if a table exists
# Ref: https://stackoverflow.com/questions/20582500/how-to-check-if-a-table-exists-in-a-given-schema
    
@postgres_connect
def get_table_schema(table, conn=None, **kwargs):
    '''
    Get table schema or None if table DNE
     * Returns a ColumnList object
    '''
    
    sql_schema = get_schema(conn=conn).groupby('Table Name')
    sql_schema = sql_schema[table]
    
    if sql_schema['Column Name']:
        return ColumnList(
            col_names=sql_schema['Column Name'],
            col_types=sql_schema['Data Type'])
    else:
        return ColumnList()

@postgres_connect
def pg_to_csv(name, file=None, verbose=True, conn=None, **kwargs):
    ''' Export a Postgres table to CSV '''
    
    if not file:
        file = name + '.csv'
    
    cur = conn.cursor()
    
    with open(file, mode='w') as outfile:
        try:
            # Use this for regular tables
            cur.copy_expert('''
                COPY {} TO STDOUT WITH CSV HEADER
                '''.format(name),
                file=outfile)
        except psycopg2.ProgrammingError:
            # Need to use this form of COPY TO for views
            conn.rollback()
            cur.copy_expert('''
                COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER
                '''.format(name),
                file=outfile)
        
    if verbose:
        print('Done exporting {} to {}.'.format(name, file))