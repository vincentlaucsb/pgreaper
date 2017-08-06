from sqlify.core._core import alias_kwargs
from sqlify.core.table import Table
from .conn import postgres_connect

from collections import namedtuple
from psycopg2 import sql
import psycopg2

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

# Check if a table exists
# Ref: https://stackoverflow.com/questions/20582500/how-to-check-if-a-table-exists-in-a-given-schema