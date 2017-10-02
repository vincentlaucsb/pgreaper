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
def copy_csv(file, name, encoding=None, header=0, delimiter=',', subset=[],
    verbose=True, conn=None, compression=None, skiplines=0, **kwargs):
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
                        A list of column names to upload
        header:         int (default: 0, i.e. first line is the header)
                         * `header=True` is equivalent to `header=0`          
                         * No header should be specified with `header=False`  
                           or `header=None`                                   
                            * **If `header > 0`, all lines before header are  
                              skipped**                                       
        skiplines:     int (default: 0)
                        How many lines after the header to skip  
        delimiter:      str (default: comma)
                        How entries in the file are separated
    '''
    
    cur = conn.cursor()

    # COPY statement
    if encoding:
        copy_stmt2 = ("COPY {0} FROM STDIN (FORMAT csv,"
                      "HEADER, DELIMITER ','{1})").format(
            name, ", ENCODING '{}'".format(encoding))
    else:
        copy_stmt2 = ("COPY {0} FROM STDIN (FORMAT csv,"
                      "HEADER, DELIMITER ',')").format(name)
    
    # Clean the CSV and calculate statistics
    csv_meta = to_csv(filename=file, output=file + '_temp.csv', header=header,
        columns=subset, skiplines=skiplines)
    col_names = csv_meta['col_names']
    schema = csv_meta['dtypes']

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
    
    with zip.open(file + '_temp.csv', mode='rb') as temp_file:
        # Clean column names and create table
        cols = ColumnList(col_names, col_types)
        cur.execute(_create_table(
            name, col_names=cols.sanitize(), col_types=col_types))

        # COPY
        cur.copy_expert(copy_stmt2, temp_file)
    
    os.remove(file + '_temp.csv')    
    conn.commit()
    conn.close()