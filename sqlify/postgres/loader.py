from sqlify._globals import SQLIFY_PATH
from sqlify.core import YieldTable
from sqlify.core._core import alias_kwargs, preprocess, sanitize_names
from sqlify.core.table import Table
from sqlify.core.schema import DialectPostgres
from sqlify.core.uploader import SQLUploader
from sqlify.zip import open, ZipReader
from .conn import *
from .schema import get_schema, get_pkey
from .database import DBPostgres

from psycopg2 import sql as sql_string
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

def _find_rejects(func):
    @functools.wraps(func)
    def inner(sql_uploader, table):
        # import pdb; pdb.set_trace()
        if sql_uploader.find_rejects:
            rejects = table.find_reject()
            good_table = table.copy_attr(table, 
                row_values=[row for i, row in enumerate(table) if i not in rejects])
                
            if not sql_uploader.reject_table:
                sql_uploader.reject_table = table.copy_attr(table)

            for i in rejects:
                sql_uploader.reject_table.append(table[i])
    
            return func(sql_uploader, good_table)
        else:
            return func(sql_uploader, table)
    return inner
    
class PostgresUploader(SQLUploader):
    def __init__(self, conn, table, name=None, find_rejects=True, null_values=None,
        *args, **kwargs):
        '''
        Parameters
        -----------
        conn:           psycopg2 connection
        table:          Table
        find_rejects:   boolean
                        Should rejects be removed before uploading
        null_values:    str
                        A string representing null values (Default: None)
        copy_params:    dict
                        A dict of arguments passed into copy_expert()
        '''
        
        super(PostgresUploader, self).__init__(
            dialect=DBPostgres, conn=conn, table=table, name=name)
            
        self.find_rejects = find_rejects
        self.null_values = null_values
        self.reject_table = None
    
    @_find_rejects
    def _copy(self, table):
        ''' Copy the table '''
        table = self._modify_table(table)
        
        # import pdb; pdb.set_trace()
        
        # Insert Table via copy_from()
        # copy_from = sql_string("COPY {} FROM STDIN (FORMAT")
        if self.null_values:
            copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',', NULL '{1}')".format(
                self.name, self.null_values)
        else:
            copy_from = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER ',')".format(self.name)
            
        self.conn.cursor().copy_expert(copy_from, file=table.to_string())
        
    @_find_rejects
    def _upsert(self, table):
        ''' Upsert the table '''
        table = self._modify_table(table)
        cur = self.conn.cursor()
        
        # Set of primary keys
        current_ids = self.dialect.get_primary_keys(table)
        
        # Split table into COPY and UPSERT tables
        copy_table = table.copy_attr(table)
        upsert_table = table.copy_attr(table)
        
        p_key_index = table.p_key
        for row in obj:
            if row[p_key_index] in current_ids:
                upsert_table.append(row)
            else:
                copy_table.append(row)
                
        # Load COPY table
        self._copy(copy_table)
        
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
        
@preprocess
@postgres_connect
def file_to_pg(file, name, delimiter, verbose=True, conn=None, commit=True, null_values=None, **kwargs):
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
        uploader = None
        file_chunker = YieldTable(file=file, io=infile, delimiter=delimiter,
            engine='postgres', **kwargs)
    
        for tbl in file_chunker:
            if not uploader:
                uploader = PostgresUploader(conn=conn, table=tbl, name=name, null_values=null_values)
            uploader.load(tbl)
            
        if commit:
            uploader.commit()
    
@_assert_pgtable
@postgres_connect
def table_to_pg(
    table, name=None, null_values=None, conn=None, commit=True,
    *args, **kwargs):
    '''
    Load a Table into a PostgreSQL database.
    
    Parameters
    ----------
    table:          Table
                    The Table to be loaded
    database:       str
                    Name of a PostgreSQL database
    null_values:    str
                    String representing null values
    conn:           psycopg2.extensions.connection
    commit:         Automatically commit transaction
    '''
    
    if name:
        table_name = name
    else:
        table_name = table.name
        
    uploader = PostgresUploader(conn=conn, table=table, name=table_name,
        null_values=null_values, *args, **kwargs)
    uploader.load(table)
    
    if commit:
        uploader.commit()