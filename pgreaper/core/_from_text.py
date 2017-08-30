''' Things that don't need to be optimized in Cython should be kept here '''

from io import StringIO
import csv

def clean_line(line, table):
    '''
    Take in a line of strings and cast them to the proper type
    
    Parameters
    -----------
    line:       list
                List of strings from CSV reader
    table:      Table
    '''

    new_line = []
    
    for i in line:
        k = i.replace(' ', '')
        try:
            if not k:
                # Empty string
                new_line.append(k)
            elif k.isnumeric():
                new_line.append(int(k))
            elif k.replace('-', '', 1).replace('.', '', 1).isnumeric():
                new_line.append(float(k))
            else:
                new_line.append(i)
        except ValueError:
            new_line.append(i)
                
    table.append(new_line)