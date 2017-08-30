from pgreaper.core import preprocess, sanitize_names, sample_file, chunk_file, Table
from pgreaper.zip import open, ZipReader

from .conn import postgres_connect
from .database import get_table_schema
from .loader import _read_stringio, simple_copy, table_to_pg

import psycopg2

@preprocess
@postgres_connect
def copy_csv(file, name, delimiter=',', subset=None, verbose=True, conn=None,
    null_values=None, **kwargs):
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
    
    Parameters
    -----------
    file:           str
                    Name of the file
    name:           str
                    Name of the table
    database:       str
                    Name of the PostgreSQL database.
                    If it doesn't exist, it will be created.
    subset:         list[str]
                    A list of columns to keep
    header:         int
                     * The line number of the header row.                 
                        * Default: 0 (as in, line zero is the header)     
                     * `header=True` is equivalent to `header=0`          
                     * No header should be specified with `header=False`  
                       or `header=None`                                   
                        * **If `header > 0`, all lines before header are  
                          skipped**                                       
    skip_lines:     int
                    How many lines fater the header to skip  
    delimiter:      str
                    How entries in the file are separated                 
                     * Defaults to '\\t' when using text_to_pg or          
                     * ',' when using csv_to_pg
    verbose:        boolean
                    Print progress report
    '''
    
    # Sample the first 7500 rows to infer schema   
    for chunk in sample_file(file=file, name=name, delimiter=delimiter,
        chunk_size=7500, engine='postgres', **kwargs):
        sample = chunk
        break
        
    sample_table = sample['table']
    if subset:
        sample_table.subset(subset)
    
    # Load a sample Table
    table_to_pg(sample_table, name, null_values, conn=conn, commit=False,
        **kwargs)
        
    # Create a Table to filter out rows which mismatch with the schema
    # created above. For efficiency, this only gets used in case of
    # a DataError
    reject_filter = Table(name=name, columns=sample_table.columns,
        dialect='postgres', strong_type=True)
        
    # Load files using StringIO
    for chunk in chunk_file(subset=subset, **sample):
        cur = conn.cursor()
        cur.execute('SAVEPOINT pgreaper_upload')
        
        # Faster Approach
        try:
            simple_copy(chunk, name=name, conn=conn, null_values=null_values)
        except psycopg2.DataError:
            # Schema mismatch
            chunk.seek(0)
            cur.execute('ROLLBACK TO SAVEPOINT pgreaper_upload')
            _read_stringio(chunk, reject_filter)
            
            # Load non-rejects
            table_to_pg(reject_filter, name, null_values, conn=conn, commit=False, **kwargs)
            reject_filter.clear()
            
    conn.commit()
            
    # Load Rejects
    if reject_filter.rejects:
        rejects = Table(name=name + '_reject', col_names=sample_table.col_names,
            dialect='postgres')
        for i in reject_filter.rejects:
            rejects.append(i)
        table_to_pg(rejects, conn=conn)
        
    conn.close()