'''
Functions signed to convert input sources to Python (Table) objects
'''

from sqlify.helpers import _preprocess, _strip
from sqlify.table import Table

import csv

# Deals with the "business logic" for yield_table()
def _file_read_defaults(func):
    def inner(file, name=None, delimiter=None, *args, **kwargs):
        # Pick a default delimiter if none specified
        if not delimiter:
            if kwargs['type'] == 'csv':
                delimiter = ','
            else:
                delimiter = ' '
                
        # Use filename if name not specified
        if not name:
            name = _strip(file.split('.')[0])
            
        return func(file, name=name, delimiter=delimiter,
                    *args, **kwargs)

    return inner

# Lazy load files
@_file_read_defaults
def yield_table(file, name=None, delimiter=None, type='text', header=0,
    skip_lines=0, na_values=None, chunk_size=10000, **kwargs):
    '''
    Arguments:
     * file:       Name of the file
     * database:   sqlite3.Connection object
     * type:       Type of file ('text' or 'csv')
     * header:     Number of the line that contains a header (None if no header)
     * skip_lines: Skip the first n lines of the text file
     * delimiter:  How the file is separated
     * na_values:  How missing values are encoded (yield_table will replace them with None)
     * chunk_size: Maximum number of rows to read at a time
      * Set to None to load entire file into memory
    '''
    
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
            if chunk_size and (line_num % chunk_size == 0):
                yield row_values
      
                row_values = Table(name, col_names=col_names, **kwargs)
                
            line_num += 1
    
        # End of loop --> Dump remaining data
        if row_values:
            yield row_values