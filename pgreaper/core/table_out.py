'''
.. currentmodule:: pgreaper
.. autofunction:: table_to_csv
.. autofunction:: table_to_html
.. autofunction:: table_to_json
.. autofunction:: table_to_md
'''

from pgreaper._globals import SQLIFY_PATH

import csv
import json
import os
import re
import functools

def _default_file(file_ext):
    ''' Provide a default filename given a Table object '''
    
    def decorator(func):
        @functools.wraps(func)
        def inner(obj, file=None, *args, **kwargs):
            if not file:
                file = obj.name.lower()
            
                # Strip out bad characters
                for char in ['\\', '/', ':', '*', '?', '"', '<', '>', '|', 
                '\t']:
                    file = file.replace(char, '')
                    
                # Remove trailing and leading whitespace in name
                file = re.sub('^(?=) *|(?=) *$', repl='', string=file)
                    
                # Replace other whitespace with underscore
                file = file.replace(' ', '_')
                    
                # Add file extension
                file += file_ext
                
            return func(obj, file, *args, **kwargs)
        return inner
        
    return decorator

def _create_dir(func):
    ''' Creates directory if it doesn't exist. Also modifies file argument 
        to include folder. '''

    @functools.wraps(func)
    def inner(obj, file, dir=None, *args, **kwargs):
        if dir:
            file = os.path.join(dir, file)
            os.makedirs(dir, exist_ok=True)
        return func(obj, file, dir, *args, **kwargs)
        
    return inner
    
@_default_file(file_ext='.csv')
@_create_dir
def table_to_csv(obj, file=None, dir=None, header=True, delimiter=','):
    '''
    Dump the contents of a Table to CSV
    
    Args:
        obj:        Table
                    Table object to be converted
        file:       str
                    Name of the file (default: Table name)
        header:     bool
                    Include the column names as the first row (default: True)
        dir:        str or os.path
                    Directory to save to (default: None --> Current dir)
    '''
     
    with open(file, mode='w', newline='\n') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"')
        
        if header:
            csv_writer.writerow(obj.col_names)
        
        for row in obj:
            csv_writer.writerow(row)
            
@_default_file(file_ext='.json')
@_create_dir
def table_to_json(obj, file=None, dir=None):
    '''
    Dump the contents of a Table to JSON according to this specification

    +---------------------------------+--------------------------------+
    | Original Table                  | JSON Output                    |
    +=================================+================================+
    |                                 |                                |
    |                                 | .. code-block:: python         |
    |                                 |                                |
    | +---------+---------+--------+  |    [{'col1': 'Wilson',         |
    | | col1    | col2    | col3   |  |      'col2': 'Sherman',        |
    | +=========+=========+========+  |      'col3': 'Lynch'           |
    | | Wilson  | Sherman | Lynch  |  |     },                         |
    | +---------+---------+--------+  |     {'col1': 'Brady',          |
    | | Brady   | Butler  | Edelman|  |      'col2': 'Butler',         |
    | +---------+---------+--------+  |      'col3': 'Edelman'         |
    |                                 |     }]                         |
    +---------------------------------+--------------------------------+
    
    Args:
        obj:        Table
                    Table object to be converted
        file:       str
                    Name of the file (default: Table name)
        dir:        str or os.path
                    Directory to save to (default: None --> Current dir)
    '''

    new_json = []
    
    for row in obj:
        json_row = {}
        
        for i, item in enumerate(row):
            json_row[obj.col_names[i]] = item
            
        new_json.append(json_row)
        
    with open(file, mode='w') as outfile:
        outfile.write(json.dumps(new_json))
        
@_default_file(file_ext='.html')
@_create_dir
def table_to_html(obj, file=None, dir=None, to_var=False, plain=False):
    with open(os.path.join(
        SQLIFY_PATH, 'core', 'table_template.html')) as template:
        template = ''.join(template.readlines())

    if to_var:
        return obj._repr_html_(clean=True, plain=plain)
    else:
        with open(file, mode='w') as outfile:
            outfile.write(
                template.format(
                    name = obj.name,
                    table = obj._repr_html_(clean=True, plain=plain)))
        
@_default_file(file_ext='.md')
@_create_dir
def table_to_md(obj, file=None, dir=None):
    with open(file, mode='w') as outfile:
        outfile.write('|{}|\n'.format(
            '|'.join(' ' + str(i) + ' ' for i in obj.col_names)))
            
        outfile.write('|{}|\n'.format(
            '|'.join(' --- ' for i in obj.col_names)))
        
        for row in obj:
            outfile.write('|{}|\n'.format(
                '|'.join(' ' + str(i) + ' ' for i in row)))