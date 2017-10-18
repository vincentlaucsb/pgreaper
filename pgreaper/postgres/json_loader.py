'''
.. currentmodule:: pgreaper
.. autofunction:: copy_json
'''

from pgreaper._globals import preprocess
from pgreaper.io import JSONStreamingDecoder, zip
from .conn import postgres_connect
from .database import load_sql, get_table_schema
from .loader import copy_table

from io import BytesIO
import psycopg2
import json

def _is_ndjson(file, compression=None):
    '''
    Determine if a file is NDJSON (newline-delimited JSON)
    by seeing if parsing it like a NDJSON works
    '''
    
    with zip.open(file, compression=compression, mode='rb') as infile:
        scan_rows = 100
    
        for line in infile:
            if not scan_rows:
                break
        
            try:
                # Technically still NDJSON, but not something this script 
                # can handle
                if not isinstance(json.loads(line), dict):
                    return False
                    
                scan_rows -= 1
            except json.decoder.JSONDecodeError:
                return False
               
    # No parse errors --> Return True
    return True

@preprocess
@postgres_connect
def copy_json(file, name, compression=None,
    flatten=None, conn=None, null_values=None, **kwargs):
    '''
    Stream a JSON and load it to Postgres
    
    Args:
        file:           str or os.path
                        File to upload
        name:           str
                        Name of the table
        compression:    str (default: None)
                        Compression algorithm to use                     
    '''
    
    cur = conn.cursor()
        
    with zip.open(file, compression=compression, mode='rb') as infile:
        # Determine whether to (a) send JSON straight to Postgres or
        # (b) use JSON streamer
        #  - Use option (a if JSON is actually newline-delimited JSON
        #  - Use option (b) otherwise
        
        cur.execute("CREATE TABLE IF NOT EXISTS {0} (json_data jsonb)".format(name))
        copy_stmt = "COPY {0} FROM STDIN (FORMAT TEXT)".format(name)
        
        if _is_ndjson(file, compression=compression):
            cur.copy_expert(copy_stmt, infile)
        else:
            tbl = BytesIO()
            streamer = JSONStreamingDecoder(source=infile)
            
            for line in streamer:
                tbl.write(line + b'\n')
                
            tbl.seek(0)
            cur.copy_expert(copy_stmt, tbl)

    if flatten == 'outer':
        load_sql('sanitize_name', conn)
        load_sql('flatten_json', conn)
        cur.execute("SELECT flatten_json('{0}')".format(name))
            
    conn.commit()
    conn.close()