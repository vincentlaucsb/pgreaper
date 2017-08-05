''' Functions for loading data from text file formats Table objects '''

from sqlify.core.tabulate import Tabulate
from ._core import preprocess

import csv
import os

# Helper class for lazy loading files
class YieldTable(object):
    ''' Lazy loads files into Table objects'''
    
    @preprocess
    def __init__(self, file, io,
        name=None,
        delimiter=' ',
        type='text',
        header=0,
        col_rename={},
        col_names=None,
        col_types=None,
        na_values=None,
        skip_lines=None,
        chunk_size=10000,
        engine='sqlite',
        transform={},
        **kwargs):
        
        '''
        Arguments:
         * io:         A File I/O object
         * file:       Name of the original fine
         * name:       Name of the output table
         * header:     Number of the line that contains a header (None if no header)
         * skip_lines: Skip the first n lines of the text file
         * delimiter:  How the file is separated
         * col_rename: A dictionary of original column names to new names
         * na_values:  How missing values are encoded (yield_table will replace them with None)
         * chunk_size: Maximum number of rows to read at a time
          * Set to None to load entire file into memory
         * transform:  A dictionary of column indices to cleaning operations
         * mutate:     Like transform, but creates new columns from existing ones
        '''
        
        # Save user settings
        self.name = name
        self.delimiter = delimiter
        self.col_rename = col_rename
        self.na_values = na_values
        self.chunk_size = chunk_size
        self.type = type
        self.col_types = col_types
        self.kwargs = kwargs
        self.col_names = col_names
        self.engine = engine
        
        # Initalize iterator values
        self.line_num = 0
        self.stop_iter = False
        
        # Convert boolean values of header to appropriate numeric values
        if isinstance(header, bool):        
            if header:
                self.header = 0 # header = True --> header is on line zero
            else:
                self.header = None
        else:
            self.header = header
        
        # Determine number of lines to skip
        if (skip_lines == None) or (skip_lines == 0):
            # Skip lines = line number of header + 1
            self.skip_lines = header + 1
        else:
            self.skip_lines = skip_lines
            
        # Store the file IO object
        self.io = csv.reader(io, delimiter=delimiter)
        
        # Parse header and skip lines
        self.goto_body()
        
        self.transform = self._parse_transform(transform)
        # self._parse_mutate(mutate)
        
    def _parse_transform(self, transform):
        ''' Given a transform argument, parse it into the most efficient form '''
        
        # Assuming transform['all'] only has one value
        if 'all' in transform:
            n_cols = len(self.col_names)
            
            for i in range(0, n_cols):
                transform[i] = transform['all']
            
            del transform['all']
            
        return transform
        
    def parse_header(self, row):
        '''
         * Given a header row, parse it according to the user's specifications
          * row should be a list of headers
        '''
        
        # Begin rename     
        if self.col_rename:
            for name in self.col_rename:
                try:
                    col_names_new[row.index(name)] = self.col_rename[name]
                except ValueError:
                    raise ValueError(
                        "Can't find {col_name} in list of columns.".format(
                            col_name=name) \
                        + "(Column names are: {col_names})".format(
                            col_names = row))
        
        return row
    
    def goto_body(self):
        ''' Parse the header and skip requested lines '''
        
        # Get column names
        if (self.header is not None) and (not self.col_names):
            # import pdb; pdb.set_trace()
            while self.line_num < self.header:
                next(self.io)
                self.line_num += 1
                
            self.col_names = self.parse_header(next(self.io))
            self.line_num += 1
        else:
            # TO DO: Fix this
            self.col_names = ['col{}'.format(i) for i in range(0, len(line))]
            
        # Skip lines
        while self.line_num + 1 <= self.skip_lines:
            next(self.io)
            self.line_num += 1

    def __iter__(self):
        return self
            
    def __next__(self):
        if self.stop_iter:
            raise StopIteration
        else:
            return self._read_next()
            self.line_num += 1
            
    def _read_next(self):
        # Read next 10000 lines from file
        row_values = Tabulate.factory(
            engine=self.engine,
            name=self.name,
            col_names=self.col_names,
            col_types=self.col_types,
            **self.kwargs)
        
        for line in self.io:
            # Write values            
            if self.transform:
                new_line = []
            
                for i, item in enumerate(line):
                    # Assume dict values are function references
                    if i in self.transform:
                        new_line.append(self.transform[i](item))
                    else:
                        new_line.append(item)
                        
                row_values.append(new_line)
            else:
                row_values.append(line)
                
            self.line_num += 1

            # When len(row_values) = chunk_size: Save and dump values
            if self.chunk_size and self.line_num != 0 and \
                (self.line_num % self.chunk_size == 0):
                
                # Infer schema: Temporary 
                if not self.col_types: 
                    self.col_types = row_values.guess_type()
                    row_values.col_types = row_values.guess_type()
                
                return row_values
    
        # End of loop --> Dump remaining data
        if row_values:
            self.stop_iter = True
            return row_values
    
def text_to_table(file, **kwargs):
    # Load entire text file to Table object
    return_tbl = None
    
    with open(file, mode='r') as infile:
        for tbl in YieldTable(file, infile, chunk_size=None, **kwargs):
            # Only "looping" once to retrieve the only Table
            return_tbl = tbl
        
    return return_tbl

def csv_to_table(file, **kwargs):
    # Load entire CSV file to Table object
    with open(file, mode='r') as infile:
        for tbl in YieldTable(file, infile, delimiter=',', chunk_size=None, **kwargs):
            return_tbl = tbl
        
    return return_tbl