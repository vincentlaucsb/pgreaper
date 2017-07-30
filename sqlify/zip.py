''' Support loading TXT and CSVs from ZIP files '''

from io import StringIO
from collections import OrderedDict
import zipfile
import csv

def read_zip(file):
    return ZipFile(file)
    
class ZipFile(object):
    ''' Methods for interacting with .zip files '''
    
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
        ret = self.open_file.read(*args).decode(self.encoding)
        
        if ret:
            return ret
        elif self.closed:
            raise ValueError('File is closed')
        else:
            # Empty string --> Close file
            self.__exit__()
    
    def readline(self, *args):
        ret = self.open_file.readline(*args).decode(self.encoding)
        
        if ret:
            return ret
        elif self.closed:
            raise ValueError('File is closed')
        else:
            # Empty string --> Close file
            self.__exit__()