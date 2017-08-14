''' Functions for loading data from text file formats Table objects '''

from sqlify.core.table import Table
from sqlify import zip
from ._from_text import clean_line
from ._core import preprocess

from functools import partial
from io import StringIO
import csv
import os
import sys

def sample_file(file, name=None, delimiter=' ', header=0, na_values=None,
    encoding='utf-8', skip_lines=0, chunk_size=7500, engine='sqlite',
    pk_index=True, col_names=None, **kwargs):
    '''
    Read the first n lines of a Table to determine column types, then return a dict of
     - Column types
     - The CSV reader object

    Parameters
    -----------
    file:           str
                    Name of the original file
    name:           str
                    Name of the output table
    header:         int
                    Number of the line that contains a header (None if no header)
    skip_lines:     int
                    How many lines after the header to skip (default: 0 or None)
    delimiter:      str
                    How the file is separated
    p_key:          str or int
                    Name or index of the primary key column
    pk_index:       bool
                    Build an index on the primary key
    chunk_size:     int 
                    Maximum number of rows to read at a time
                    Set to 0 to load entire file into memory
    '''
       
    # cdef vector[string] line
    cdef int line_num
    cdef int chunk_size_ = chunk_size
    line_num = 0
    col_types = None
       
    # Other case: file is already a file IO object
    if isinstance(file, str):
        infile = zip.open(file, mode='r', encoding=encoding)
    else:
        infile = file
    
    try:
        reader = csv.reader(infile, delimiter=delimiter)
        
        # Ignore lines until header
        if not col_names:
            while line_num + 1 < header:
                next(reader)
                line_num += 1
                
            col_names = next(reader)
            
        row_values = Table(dialect=engine, name=name, col_names=col_names, **kwargs)
            
        # Iterate over file
        while skip_lines:
            next(reader)
            skip_lines -= 1
        
        for line in reader:
            clean_line(line, row_values)
            line_num += 1
            
            if chunk_size_ and (line_num == chunk_size):
                return {'table': row_values, 'line_num': line_num, 'reader': reader,
                    'infile': infile, 'eof': False}
                
        # EOF: Dump rest of lines
        infile.close()
        return {'table': row_values, 'line_num': line_num, 'reader': reader,
            'infile': infile, 'eof': True}
    except:
        print(sys.exc_info())
        infile.close()
        
def text_to_table(file, **kwargs):
    # Load entire text file to Table object
    return sample_file(file, delimiter='\t', chunk_size=0, **kwargs)['table']
    
def csv_to_table(file, **kwargs):
    # Load entire CSV file to Table object
    return sample_file(file, delimiter=',', chunk_size=0, **kwargs)['table']

# Helper class for lazy loading files
def chunk_file(table, line_num, infile, reader, chunk_size=5000, **kwargs):
    '''
    Lazy load a file in separate chunks of StringIO objects
    '''
    
    cdef int line_num_ = line_num
    cdef int chunk_size_ = chunk_size
    
    string = StringIO()
    writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    
    try:
        for line in reader:
            line_num_ += 1
            writer.writerow(line)
            
            if (line_num_ != 0) and (line_num % chunk_size_ == 0):
                yield string
                string = StringIO()
                writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    except:
        infile.close()            
    yield string