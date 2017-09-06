from pgreaper.core import preprocess, Table
from pgreaper.io import PyJSONStreamer, read_json

from .conn import postgres_connect
from .database import get_table_schema
from .loader import copy_table

import psycopg2
import json

@postgres_connect
def copy_json(file, name, flatten=1, conn=None, null_values=None, **kwargs):
    ''' Stream a JSON and load it to Postgres '''
    
    streamer = PyJSONStreamer()
    
    tbl = None
    
    with open(file, mode='rb') as infile:       
        while True:
            streamer.feed_input(infile.read(100000))
            data = [json.loads(i) for i in streamer.get_json()]
            if not data: break
            
            if not tbl:
                tbl = read_json(data, flatten=flatten)
            else:
                tbl += read_json(data, flatten=flatten)
                
    copy_table(tbl, name, conn=conn)