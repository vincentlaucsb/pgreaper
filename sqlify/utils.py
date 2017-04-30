''' Utility functions designed for end users '''

from .table import Table

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
def head(filename, n=10, delimiter=' '):
    if 't' in delimiter:
        delimiter = '\t'

    with open(filename, 'r') as infile:
        data = csv.reader(infile, delimiter=delimiter)
        row_values = []
        
        for line in data:
            if n > 0:
                row_values.append(line)
                n -= 1
            else:
                break
            
    col_names = [ "col_" + str(n) for n in range(0, len(row_values[0])) ]
            
    return Table(filename, col_names=col_names, row_values=row_values)

# Removes whitespace from entries in a column
def strip_whitespace(entry):
    return entry.replace(' ', '')