'''
.. currentmodule:: pgreaper

'''

from io import StringIO
from collections import OrderedDict
import builtins
import csv
import zipfile
import gzip
import bz2
import lzma

from pgreaper._globals import DEFAULT_ENCODING, ReusableContextManager

def open(file_or_path, compression=None, binary=False, *args, **kwargs):
    '''
    Override default open() function
     - This function only needs to be used by internal parts of pgreaper
    '''

    # Map compression argument to the correct Python library
    valid_compression = {
        'gz': gzip,
        'gzip': gzip,
        'bz2': bz2,
        'bzip': bz2,
        'lzma': lzma,
    }
    
    if isinstance(file_or_path, ZipReader):
        # ZipReader object --> Return it
        return file_or_path
    elif compression:
        try:
            comp_lib = valid_compression[compression]
        except KeyError:
            raise ValueError('Unsupported compression algorithm.'
                'Valid options are "gzip", "bz2", and "lzma".')
            
        return comp_lib.open(file_or_path, *args, **kwargs)
    else:
        # Regular file
        return builtins.open(file_or_path, *args, **kwargs)

def read_zip(file):
    '''
    Reads a ZIP file and returns a `ZipFile` object
    
    Args:
        file:       Name of the ZIP file
    '''
    
    return ZipFile(file)
    
class ZipFile(object):
    '''
    Provides methods for interacting with zip files
    
    Step 1: Getting a List of Contents
     >>> from pgreaper import read_zip, text_to_pg
     >>> zip_file = read_zip('launch_codes.zip')
     >>> zip_file
     [0] nuke_passwords.txt
     [1] team_america.mp4
     
    Step 2: Accessing Individual Files
     >>> my_file = zip_file['nuke_passwords.txt']
     
    Sepcifying an Encoding
     >>> my_file = zip_file['nuke_passwords.txt', 'cp1252']
    
    Step 3: Converting Files
     >>> pgreaper.copy_text(my_file, database='top_secret')
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
        encoding = None
        
        def get_by_index(key):
            return self.files[key]

        def get_by_name(key):
            try:
                self.files.index(key)
                return key
            except ValueError:
                raise ValueError('There is no file named {}.'.format(key))
        
        if isinstance(key, int):
            file = get_by_index(key)
        elif isinstance(key, str):
            file = get_by_name(key)
            
        # zip_file[<index or file name>, <encoding>]
        elif isinstance(key, tuple):
            encoding = key[1]
            if isinstance(key[0], int):
                file = get_by_index(key[0])
            else:
                file = get_by_name(key[0])                
        else:
            raise ValueError('Please specify either an index or a filename.')
            
        if not encoding: encoding = DEFAULT_ENCODING        
        return ZipReader(zip_file = self.zip_file, file = file, encoding=encoding)
        
class ZipReader(ReusableContextManager):
    '''
    Decodes a binary stream from a file within a ZIP
     - Should be created by ZipFile objects
    
    Usage as a Context Manager
    >>> with ZipReader as comp_file:
    >>>     data = comp_file.readlines()
    
    Note: This is a reusable context manager
     - See `keep_alive` parameter
     - Allows ZipReader to be passed between functions, but retains 
       an "auto-shutoff" mechanism
    '''
    
    def __init__(self, zip_file, file, encoding, keep_alive=0):
        '''
        Parameters
        -----------        
        zip_file        str
                        Name of a ZIP file
        file            str
                        Name of file within zip
        keep_alive      int
                        How many times this context manager can be exited
                        before finally closing off the file
        '''
        
        self.zip_file = zip_file
        self.file = file
        self.encoding = encoding
        self.keep_alive = keep_alive
        self.closed = False
        
    def __enter__(self):
        self.zip_file = zipfile.ZipFile(self.zip_file, mode='r')
        self.open_file = self.zip_file.open(self.file)
        return self

    def close(self):
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