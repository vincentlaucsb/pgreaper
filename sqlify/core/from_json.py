from sqlify.core.schema import DialectPostgresJSON
from sqlify.core.table import Table

from collections import defaultdict
from io import StringIO
import csv
import json

def read_json(json_data, name=None, flatten=1, extract=None,
    column_name='json'):
    '''
    Arguments:    
     * json:    File or list of JSON dicts
     * flatten: Level of flattening to perform
     * extract: In addition to flattening, pull out records according to extract
                Should be in this format:
                 * {'new_column_name': 'key -> key -> key'}
     
    Adds JSON after performing one level of flattening
    '''
    
    # Parse extract dict
    new_extract = {}
    
    if extract:
        for col, path in zip(extract.keys(), extract.values()):
            new_extract[col] = path.split('->')
    
    # Parse json argument
    if isinstance(json_data, str):
        with open(json_data, mode='r') as json_file:
            json_str = ''
            for i in json_file:
                json_str += i
        json_data = json.loads(json_str)
    elif isinstance(json_data, dict):
        json_data = [json_data]
    elif not isinstance(json_data, list):
        raise ValueError("'Argument 'json_data' should either be a filename,"
            "a dict, or a list.")

    if flatten == 0:
        table = _json_flatten_0(json_data, name, new_extract, column_name)
    elif flatten == 1:
        table = _json_flatten_1(json_data, name, new_extract)
    else:
        raise NotImplementedError('Flattening is only supported up to one level.')
    
    # Error with just one column
    table.guess_type()
    return table
    
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

    return Table(dialect=DialectPostgresJSON(),
        name=name,
        col_names=col_values.keys(),
        col_values=list(col_values.values()))
    
def _json_flatten_1(json, name, extract):
    x = Table(name=name, dialect=DialectPostgresJSON())
    x._add_dicts(json, extract=extract)

    return x
    
# def _json_to_table(json, name, extract):
    # ''' Should return a properly formatted Table flattened out one level '''

    # col_values = defaultdict(list)
    # first_row = True
    
    # # Needs to create lists of uniform length for each column
    # for i in json:
        # keys = set(i.keys()).union(set(col_values.keys())).difference(
            # set(extract.keys()))
    
        # for k in keys:
            # # Did column previously exist... if not, fill in
            # if not col_values[k] and not first_row:
                # n_rows = len(list(col_values.values())[0])
                # col_values[k] = [None] * n_rows
        
            # try:
                # col_values[k].append(i[k])
            # except KeyError:
                # col_values[k].append(None)
                
        # # Extract values according to extract dict
        # for col, path in zip(extract.keys(), extract.values()):
            # try:
                # value = i
                # for k in path:
                    # value = value[k]
                    
                # col_values[col].append(value)
            # except (KeyError, IndexError) as e:
                # col_values[col].append(None)
            
        # if first_row:
            # first_row = False

    # return Table(dialect=DialectPostgresJSON(),
        # name=name,
        # col_names=col_values.keys(),
        # col_values=list(col_values.values()))