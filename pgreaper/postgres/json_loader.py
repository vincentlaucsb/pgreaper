from pgreaper._globals import preprocess
from pgreaper.io import JSONStreamingDecoder, zip
from .conn import postgres_connect
from .database import load_sql, get_table_schema
from .loader import copy_table

from io import BytesIO
import psycopg2

@preprocess
@postgres_connect
def copy_json(file, name, compression=None,
    filter=[], flatten=None, conn=None, null_values=None, **kwargs):
    ''' Stream a JSON and load it to Postgres '''
    
    tbl = BytesIO()
    cur = conn.cursor()
    
    with zip.open(file, compression=compression, mode='rb') as infile:
        streamer = JSONStreamingDecoder(source=infile)
        
        for line in streamer:
            tbl.write(line + b'\n')
            
        copy_stmt = "COPY {0} FROM STDIN (FORMAT TEXT)".format(name)

        cur.execute("CREATE TABLE IF NOT EXISTS {0} (json_data jsonb)".format(name))
        
        tbl.seek(0)
        cur.copy_expert(copy_stmt, tbl)
        
        if flatten == 'outer':
            load_sql('sanitize_name', conn)
            load_sql('flatten_json', conn)
            cur.execute("SELECT flatten_json('{0}')".format(name))
                
    conn.commit()
    conn.close()