'''
.. currentmodule:: sqlify

Reading ZIP Files
==================
.. autofunction:: read_zip
.. autoclass:: sqlify.zip.ZipFile
'''

from io import StringIO
from collections import OrderedDict
import zipfile
import csv
import builtins

def open(file_or_path, *args, **kwargs):
    ''' Override default open() function '''

    # ZipReader object --> Return it
    if isinstance(file_or_path, ZipReader):
        return file_or_path
    else:
        return builtins.open(file_or_path, *args, **kwargs)

def read_zip(file):
    '''
    Reads a ZIP file and returns a `ZipFile` object
    
    Parameters
    -----------
    file:       Name of the ZIP file
    '''
    
    return ZipFile(file)
    
class ZipFile(object):
    '''
    Provides methods for interacting with zip files
    
    Step 1: Getting a List of Contents
     >>> from sqlify import read_zip, text_to_pg
     >>> zip_file = read_zip('launch_codes.zip')
     >>> zip_file
     [0] nuke_passwords.txt
     [1] team_america.mp4
     
    Step 2: Accessing Individual Files
     >>> my_file = zip_file['nuke_passwords.txt']
    
    Step 3: Converting Files
     >>> sqlify.text_to_pg(my_file, database='top_secret')
    '''
    
    def __init__(self, file):
        ''' Read the file and get a list of contents '''
        
        self.zip_file = file
        self.files = []
        
        with zipfile.ZipFile(file, mode='r') as infile:
            for info in infile.infolist():
                self.files.append(info.filename)
            
    def __repr__(self):
        ''' Return a list of file contents '''
        
        return_str = ""
        
        for i, file in enumerate(self.files):
            return_str += '[{}] {}'.format(i, file)
            
        return return_str

    def __getitem__(self, key):
        ''' Given a key, return a ZipReader for the corresponding file '''
        
        if isinstance(key, int):
            file = self.files[key]
        elif isinstance(key, str):
            try:
                self.files.index(key)
                file = key
            except ValueError:
                raise ValueError('There is no file named {}.'.format(key))
        else:
            raise ValueError('Please specify either an index or a filename.')
        
        return ZipReader(zip_file = self.zip_file, file = file)
        
class ZipReader(object):
    '''
    Converts a binary stream for use with YieldTable
     * Can be used as a context manager
    '''
    
    def __init__(self, zip_file, file, encoding='utf-8'):
        '''
        Arguments
        
         * zip_file:    Name of a ZIP file
         * file:        Name of file within zip
        '''
        
        self.zip_file = zip_file
        self.file = file
        self.encoding = encoding
        self.closed = False
        
    def __enter__(self):
        self.zip_file = zipfile.ZipFile(self.zip_file, mode='r')
        self.open_file = self.zip_file.open(self.file)
        return self
        
    def __exit__(self, *args):
        self.open_file.close()
        self.zip_file.close()
        self.closed = True
        
    def __iter__(self):
        return self
        
    def __next__(self):
        next = self.readline()
        
        if next:
            return next
        else:
            raise StopIteration
        
    def read(self, *args):
        if self.closed:
            raise ValueError('File is closed')
    
        ret = self.open_file.read(*args).decode(self.encoding)
        
        if ret:
            return ret
            
        # Empty string --> Close file
        self.__exit__()
    
    def readline(self, *args):
        if self.closed:
            raise ValueError('File is closed')
    
        ret = self.open_file.readline(*args).decode(self.encoding)
        
        if ret:
            return ret
            
        # Empty string --> Close file
        self.__exit__()