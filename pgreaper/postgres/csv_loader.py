from pgreaper._globals import preprocess
from pgreaper.core import Table, ColumnList
from pgreaper.io import zip
from .conn import postgres_connect
from .database import _create_table, get_table_schema

from csvmorph import to_csv, dtypes
import psycopg2
import csv
import os

def copy_text(*args, **kwargs):
    '''
    Uploads a TXT file to PostgreSQL
    
    .. note:: This merely calls `copy_csv()` with `delimiter='\t'`
    '''
    copy_csv(delimiter='\t', *args, **kwargs)

@preprocess
@postgres_connect
def copy_csv(file, name, header=0, delimiter=',', subset=None, verbose=True, conn=None,
    compression=None, skip_lines=0, null_values=None, **kwargs):
    '''
    Uploads a CSV file to PostgreSQL
    
    **Basic Usage:**
     >>> import pgreaper
     >>> pgreaper.copy_csv('slim_shady.txt',
     ...    dbname='stan_db',
     ...    delimiter='\t',
     ...    name='slim_shady',
     ...    username='hailie',
     ...    password='ithinkmydadiscrazy'
     ...    host='localhost')
     
    .. note:: The name argument here was unnecessary, because the
       filename without the extension is used as a fallback for the 
       table name.
    
    Postgres Connection Args: Specify one of the following:
        dbname, user, password, and host:
                        Method 1: If any of the parameters in this group are
                        omitted, the values from the default settings are used.
        conn:           psycopg2 Connection
                        Method 2: Manually pass in a connection created with
                        `psycopg2.connect()`
    
    Args:
        file:           str
                        Name of the file
        name:           str
                        Name of the table
        compression:    'gzip', 'bz2', or 'lzma' (default: None)
                        The algorithm used to compress the file
        subset:         list[str] (default: [])
                        A list of column name to upload
        header:         int (default: 0, i.e. first line is the header)
                         * `header=True` is equivalent to `header=0`          
                         * No header should be specified with `header=False`  
                           or `header=None`                                   
                            * **If `header > 0`, all lines before header are  
                              skipped**                                       
        skip_lines:     int (default: 0)
                        How many lines after the header to skip  
        delimiter:      str (default: comma)
                        How entries in the file are separated                 
        null_values:    str or None (default)
                        String representing null values
    '''
    
    cur = conn.cursor()
    
    # Get schema information
    schema = dtypes(
        filename=file,
        compression=compression,
        header=header)
        
    col_types = []
        
    for count in schema:
        if count['str']:
            col_types.append('text')
        elif count['float']:
            col_types.append('double precision')
        elif count['int']:
            col_types.append('bigint')
        else:
            col_types.append('text')
        
    # COPY statement
    copy_stmt = "COPY {0} FROM STDIN (FORMAT csv, DELIMITER '{1}', NULL '')".format(
        name, delimiter)
    
    # Open the file
    with zip.open(file, mode='r') as infile:
        reader = csv.reader(infile, delimiter=delimiter)
        
        while header:
            next(reader)
            header -= 1
        else:
            col_names = next(reader)

        while skip_lines:
            next(reader)
            skip_lines -= 1
            
        # Get position of reader when data begins
        # begin_data = infile.tell()
            
        # Clean column names and create table
        cols = ColumnList(col_names, col_types)
        cur.execute(_create_table(
            name, col_names=cols.sanitize(), col_types=col_types))
        cur.execute("SAVEPOINT pgreaper_upload")
        
        try:
            cur.copy_expert(copy_stmt, infile)
        except (psycopg2.DataError, psycopg2.extensions.QueryCanceledError):
            cur.execute("ROLLBACK TO pgreaper_upload")
            
            # Clean the CSV
            to_csv(filename=file, output=file + '_temp.csv')
            with open(file + '_temp.csv', mode='rb') as infile2:
                cur.copy_expert(copy_stmt, infile2)
            os.remove(file + '_temp.csv')
    
    conn.commit()
    conn.close()