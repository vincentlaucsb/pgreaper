'''
.. currentmodule:: pgreaper
.. autofunction:: read_json
'''

from pgreaper.core.table import Table
from .json_tools import PyJSONStreamer

from collections import defaultdict, deque
from json import loads, JSONDecoder
import csv

class JSONStreamingDecoder(PyJSONStreamer):
    def __init__(self, source=None, loads=loads):
        '''
        Args:
            source:     File-like object
                        BytesIO, or some other binary file stream
            decoder:    JSONDecoder
                        Object for decoding JSON objects
            loads:      function
                        Function for loading JSON data
        '''
        
        self._streamer = PyJSONStreamer()
        self.source = source
        self.loads = loads
        self.queue = deque()
        
    def __iter__(self):
        return self
        
    def __next__(self):
        ''' Return decoded JSON objects one at a time '''
        if not self.queue:
            data = self.source.read(100000)
            if not data:
                self.source.close()
                raise StopIteration
            else:
                self._streamer.feed_input(data)
                
            for i in self._streamer.get_json():
                self.queue.append(self.loads(i))
                
        return self.queue.popleft()

def read_json(json_data=None, file=None, name=None, filter=[],
    flatten='outer', extract=None, column_name='json'):
    '''
    Args:
        json_data:  str, list
                    JSON string, or list of JSON dicts
        file:       str
                    Name of JSON file
        filter:     list
                    Create a new Table with the specified keys as the columns
                     - Other keys are dumped
                     - If filter is specified, flatten and extract are ignored
                     - Nested keys can be specified with [] notation
        flatten:    None, 'outer', or 'all' (default: 'outer')
                    Level of JSON flattening to perform
                     - None:    Perform no flattening
                     - 'outer': Turn outermost keys into columns
                     - 'all':   Pull out all nested nodes
        extract:    dict
                    In addition to flattening, pull out records according to extract
                    Should be in this format:
                     - {'new_column_name': 'key -> key -> key'}
    '''
    
    # Parse extract dict
    new_extract = {}
    
    if extract:
        for col, path in zip(extract.keys(), extract.values()):
            new_extract[col] = path.split('->')
    
    # Parse json argument
    if isinstance(json_data, str):
        json_data = loads(json_data)
    elif isinstance(json_data, dict):
        json_data = [json_data]
    elif not (isinstance(json_data, list) or json_data is None):
        raise ValueError("'Argument 'json_data' should either be a filename,"
            "a dict, or a list.")
    
    if file:
        with open(file, mode='r') as json_file:
            json_data = loads(json_file.read())
            
    if filter:
        return _json_filter(json_data, name, filter)
    else:
        if flatten is None:
            table = _json_flatten_0(json_data, name, new_extract, column_name)
        elif flatten == 'outer':
            table = _json_flatten_1(json_data, name, new_extract)
        elif flatten == 'all':
            table = _json_flatten_all(json_data, name)
        else:
            raise ValueError('Unknown argument for "value". See help('
                             'pgreaper.read_json) for options.')
    
    table.guess_type()
    return table
    
def _json_filter(json, name, filter):
    '''
    Return a new table by filtering out keys
    
    Args:
        json:       list
                    A list of JSON records
        name:       str
                    Name of the Table
        filter:     list
                    List of keys to filter by
    '''
    
    # Enclose all keys in brackets
    for i, key in enumerate(filter):
        if not (key.startswith('[') and key.endswith(']')):
            filter[i] = "['{}']".format(key)
            
    # A list of statements like d['key'] which we will eval()
    eval_keys = ['dict_{}'.format(k) for k in filter]
    
    rows = []
    
    for dict_ in json:
        row = []
        
        for col in eval_keys:
            try:
                row.append(eval(col))
            except (KeyError, IndexError):
                row.append(None)
                
        rows.append(row)
                
    return Table(name=name,
        col_names=[i[1:-1].replace('[', '').replace(
            ']', '.').replace("'", '') for i in filter],
        row_values=rows)
    
def _json_flatten_0(json, name, extract, column_name):
    '''
    Parse a JSON into a Table without flattening except for extract
    
    Parameters
    -----------
    json:           list
                    A list of JSON data
    name:           str
                    Table name
    column_name:    json
                    Column name for JSON
    extract:        dict
                    Extract dict
    '''
    
    col_values = defaultdict(list)
    first_row = True
    
    for i in json:
        # Extract values according to extract dict
        for col, path in zip(extract.keys(), extract.values()):
            try:
                value = i
                for k in path:
                    value = value[k]
                    
                col_values[col].append(value)
            except (KeyError, IndexError) as e:
                col_values[col].append(None)
                
        # Add unflattened JSON
        col_values[column_name].append(i)

    return Table(dialect='postgres',
        name=name,
        col_names=col_values.keys(),
        col_values=list(col_values.values()))
    
def _json_flatten_1(json, name, extract):
    x = Table(name=name, dialect='postgres')
    x.add_dicts(json, extract=extract)
    return x
    
def _json_flatten_all(json, name):
    x = Table(name=name)
    x.add_dicts([flatten_dict(d) for d in json])
    return x