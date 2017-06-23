from sqlify.config import POSTGRES_DEFAULT_USER, POSTGRES_DEFAULT_PASSWORD
from sqlify.helpers import _sanitize_table
from sqlify.readers import yield_table, PgTable

from sqlify.postgres.conn import *

import psycopg2
import re
import os

def file_to_pg(file, database, type, delimiter, **kwargs):
    ''' Reads a file in separate chunks (to conserve memory) and 
        loads it via the COPY FROM protocol '''

    # Table of rejects
    reject_tbl = None

    for tbl in yield_table(file=file, type=type, delimiter=delimiter,
        engine='postgres', **kwargs):
    
        # import pdb; pdb.set_trace()
        try_to_load = table_to_pg(obj=tbl, database=database, **kwargs)
        
        ''' If unsuccessful, then try_to_load is equal to the index of
            the erroneous line'''
        while try_to_load >= 0:
            if not reject_tbl:
                reject_tbl = PgTable(
                    name=tbl.name + "_reject", col_names=tbl.col_names,
                    col_types='TEXT')
            
            # Load non-erroneous lines
            table_to_pg(obj=tbl[:try_to_load], database=database, **kwargs)
            
            # Add erroneous line to list of rejects
            reject_tbl.append(tbl[try_to_load])
            
            # Try to load lines after reject line
            tbl = tbl[try_to_load + 1:]
            try_to_load = table_to_pg(obj=tbl, database=database, **kwargs)
            
    # Load rejects (if there are any)
    if reject_tbl:
        table_to_pg(obj=reject_tbl, database=database)

def table_to_pg(
    obj, database, null_values=None, name=None,
    username=None, password=None, host="localhost", **kwargs):
    
    '''
    Arguments:
     * null_values: String representing null values
     * drop:        Drop existing table
    '''

    conn = postgres_connect(database, username, password, host)
    cur = conn.cursor()
    
    # Create the table
    if name:
        table_name = name
    else:
        table_name = obj.name
        
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
        
    for col_name, type in cols_zip:
        cols.append("{0} {1}".format(col_name, type))
    
    # import pdb; pdb.set_trace()
    
    # TO DO: Strip "-" from table names
    
    # Create table
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
        table_name, ", ".join(cols))
    
    cur.execute(create_table)
    
    # if "reject" in obj.name:
        # import pdb; pdb.set_trace()
    
    try:
        # Insert Table via copy_from()
        if null_values:
            cur.copy_expert(
                "COPY {0} FROM STDIN (DELIMITER '\t', NULL '{1}')".format(table_name, null_values),
                file=obj)
        else:
            cur.copy_from(obj, table_name, sep='\t')
            
        conn.commit()
        return -1
        
    except psycopg2.DataError as e:
        ''' Return line number where error occurred
            (Subtract 1 because SQL line numbers are not zero-indexed)      
        '''
        return int(re.search('COPY .* line (?P<lineno>[0-9]+)\, column',
            str(e)).group('lineno')) - 1