''' Functions for loading data from text file formats Table objects '''

from pgreaper.core.table import Table
from pgreaper import zip
from ._from_text import clean_line
from ._core import preprocess

from functools import partial
from io import StringIO
import csv
import os
import sys

__all__ = ['sample_file', 'chunk_file', 'read_text', 'read_csv']

def sample_file(file, name=None, delimiter=',', header=0,
    encoding='utf-8', skip_lines=0, chunk_size=7500,
    engine='sqlite', pk_index=True, **kwargs):
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
       
    cdef int line_num
    cdef int chunk_size_ = chunk_size
    line_num = 0
    col_names = None
    col_types = None

    # `file` can either be a filename (str) or ZipReader object
    with zip.open(file, mode='r', encoding=encoding) as infile:
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
                yield {'table': row_values, 'line_num': line_num,
                    'reader': reader, 'infile': infile}
                row_values.clear()
                
    # EOF: Dump rest of lines
    yield {'table': row_values, 'line_num': line_num, 'reader': reader,
            'infile': infile}
    return
        
# Helper class for lazy loading files
def chunk_file(table, line_num, infile, reader, subset=None, chunk_size=5000, **kwargs):
    '''
    Lazy load a file in separate chunks of StringIO objects
    
    Parameters
    -----------
    subset:     list[int]
                A list of columns to extract
    '''
    
    cdef int line_num_ = line_num
    cdef int chunk_size_ = chunk_size
    
    string = StringIO()
    writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)

    try:
        for line in reader:
            line_num_ += 1
            
            if subset:
                subset_indices = table.columns.map(*subset)
                writer.writerow([line[i] for i in subset_indices])
            else:
                writer.writerow(line)
            
            if (line_num_ != 0) and (line_num_ % chunk_size_ == 0):
                yield string
                string = StringIO()
                writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    except ValueError:
        print("File closed")
        # File has already been read entirely
        pass
                
    yield string
    
# File Output
def read_text(file, delimiter='\t', **kwargs):
    # Load entire text file to Table object
    for chunk in sample_file(file, delimiter=delimiter, chunk_size=0, **kwargs):
        return chunk['table']
    
def read_csv(file, delimiter=',', **kwargs):
    # Load entire CSV file to Table object
    for chunk in sample_file(file, delimiter=',', chunk_size=0, **kwargs):
        return chunk['table']