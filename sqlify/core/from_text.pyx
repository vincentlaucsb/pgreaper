''' Functions for loading data from text file formats Table objects '''

from sqlify.core.table import Table
from sqlify.zip import open
from ._from_text import clean_line
from ._core import preprocess

from functools import partial
import multiprocessing as mp
import csv
import os

# Helper class for lazy loading files
def chunk_file(file, name=None, delimiter=' ', header=0, na_values=None,
    skip_lines=None, chunk_size=10000, engine='sqlite', pk_index=True, **kwargs):
    '''
    Lazy load a file in separate chunks

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
       
    with open(file, mode='r') as infile:
        reader = csv.reader(infile, delimiter=delimiter)
        
        # Ignore lines until header
        while line_num + 1 < header:
            reader.__next__()
            line_num += 1
            
        col_names = reader.__next__()
        row_values = Table(dialect=engine, name=name, col_names=col_names)
            
        # Iterate over file
        while skip_lines:
            reader.__next__()
            skip_lines -= 1
        
        for line in reader:
            clean_line(line, row_values)
            line_num += 1
            
            if chunk_size_:
                if (line_num != 0) and (line_num % chunk_size_ == 0):
                    row_values.guess_type()
                    yield row_values
                    row_values = Table(dialect=engine, name=name, col_names=col_names)

    # End of loop --> Dump remaining data
    row_values.guess_type()
    yield row_values
            
def text_to_table(file, **kwargs):
    # Load entire text file to Table object
    for i in chunk_file(file, delimiter='\t', chunk_size=0, **kwargs):
        return i
    
def csv_to_table(file, **kwargs):
    # Load entire CSV file to Table object
    for i in chunk_file(file, delimiter=',', chunk_size=0, **kwargs):
        return i