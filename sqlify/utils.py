''' Utility functions designed for end users '''

from .core import Table
from .core import yield_table

import csv

# View first few lines of a file (no delimiter)
    
# Return a list of raw strings
def preview(filename, n=10):
    first_lines = []
    line_num = 0

    with open(filename, 'r') as file:
        for line in file:
            if n <= 0:
                break
                
            first_lines.append(repr('[{0}] {1}'.format(line_num, line)))
            
            line_num += 1
            n -= 1

    return first_lines

# View first few lines of a file
def head(filename, delimiter, n=10, *args, **kwargs):
    '''
     * Set lim = 2 to skip the header row
     * Find a more elegant solution later
    '''
    
    lim = 2
    
    for i in yield_table(file=filename, delimiter=delimiter, chunk_size=10, *args, **kwargs):
        if lim == 0:
            break
            
        ret_tbl = i
        
        lim -= 1

    return ret_tbl
    
# Removes whitespace from entries in a column
def strip_whitespace(entry):
    return entry.replace(' ', '')