'''
Functions signed to convert input sources to Python (Table) objects
'''

from sqlify.helpers import _preprocess, _strip
from sqlify.table import Table

import csv
import os

# Deals with the "business logic" for yield_table()
def _file_read_defaults(func):
    def inner(*args, **kwargs):
        # Pick a default delimiter if none specified
        
        def pick_delim():
            if kwargs['type'] == 'csv':
                return ','
            else:
                return ' '
        
        try:
            if not kwargs['delimiter']:
                kwargs['delimiter'] = pick_delim()
        except KeyError:
            kwargs['delimiter'] = pick_delim()
                
        return func(*args, **kwargs)

    return inner

# Lazy load files
@_file_read_defaults
@_preprocess
def yield_table(
    file,
    name,
    delimiter=' ',
    type='text',
    header=0,
    na_values=None,
    skip_lines=None,
    chunk_size=10000,
    **kwargs):
    
    '''
    Arguments:
     * file:       Name of the file
     * type:       Type of file ('text' or 'csv')
     * header:     Number of the line that contains a header (None if no header)
     * skip_lines: Skip the first n lines of the text file
     * delimiter:  How the file is separated
     * na_values:  How missing values are encoded (yield_table will replace them with None)
     * chunk_size: Maximum number of rows to read at a time
      * Set to None to load entire file into memory
    '''
    
    # import pdb; pdb.set_trace()
    
    # So header row doesn't end up being included in data
    if skip_lines == None:
        # Skip lines = line number of header + 1
        skip_lines = header + 1
    
    # Split one line according to delimiter
    def split_line(line):
        line = line.replace('\n', '')
    
        if delimiter:
            line = line.split(delimiter)
        
        return line
        
    # Replace null values
    def na_rm(val):
        if val == na_values:
            return None
        return val

    with open(file, 'r') as infile:
        if type == 'csv':
            infile = csv.reader(infile, delimiter=delimiter)
    
        line_num = 0
        col_names = None
        row_values = None
    
        for line in infile:
            # For text files, split line along delimiter
            if type == 'text':
                line = split_line(line)
        
            if not col_names:
                '''
                Use header not None because if header = 0, 
                then bool(header) = False
                '''
                if header is not None:
                    if header == line_num:
                        col_names = line
                        row_values = Table(name, col_names=col_names, **kwargs)
                else:
                    col_names = ['col' + str(i) for i in range(0, len(line))]
                
            # Write values
            if line_num + 1 > skip_lines:
                if na_values:
                    line = [na_rm(i) for i in line]

                row_values.append(line)
            
            # When len(row_values) = chunk_size: Save and dump values
            if chunk_size and line_num != 0 and (line_num % chunk_size == 0):
                yield row_values
      
                row_values = Table(name, col_names=col_names, **kwargs)
                
            line_num += 1
    
        # End of loop --> Dump remaining data
        if row_values:
            yield row_values