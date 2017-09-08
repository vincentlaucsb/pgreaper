from pgreaper._globals import preprocess
from pgreaper.core import Table
from pgreaper.io import JSONStreamingDecoder, read_json, zip
from pgreaper.io.json_loads_hooks import *
from .conn import postgres_connect
from .database import get_table_schema
from .loader import copy_table

from functools import partial
import psycopg2
import json

@preprocess
@postgres_connect
def copy_json(file, name, compression=None,
    filter=[], flatten='outer', conn=None, null_values=None, **kwargs):
    ''' Stream a JSON and load it to Postgres '''
    
    if filter:
        tbl = Table(name=name, col_names=filter)
    else:
        tbl = Table(name=name)
    
    # Specify the correct function for parsing JSON objects
    if filter:       
        # Enclose all keys in brackets
        for i, key in enumerate(filter):
            if not (key.startswith('[') and key.endswith(']')):
                filter[i] = "['{}']".format(key)
                
        # A list of statements like d['key'] which we will eval()
        eval_keys = ['dict_{}'.format(k) for k in filter]
        loads = partial(json.loads,
            object_hook=partial(filter_keys, eval_keys=eval_keys))
    else:
        if flatten == 'all':
            loads = partial(json.loads, object_hook=flatten_dict)
        else:
            loads = json.loads
    
    with zip.open(file, compression=compression, binary=True) as infile:
        streamer = JSONStreamingDecoder(source=infile, loads=loads)
        
        for line in streamer:
            if filter:
                tbl.append(line)
            else:
                tbl.add_dict(line)
            
            if len(tbl) > 10000:
                try:
                    copy_table(tbl, name, append=True, reorder=True,
                        expand_sql=True, expand_input=True,
                        commit=False, conn=conn)
                    tbl.clear()
                except:
                    pdb.set_trace()
        
        # Load remaining JSON
        copy_table(tbl, name, append=True, reorder=True,
            expand_sql=True, expand_input=True, commit=False, conn=conn)
                
    conn.commit()
    conn.close()