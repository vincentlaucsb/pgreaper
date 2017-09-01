'''
.. currentmodule:: pgreaper

Uploading Delimiter-Separated File Formats
-------------------------------------------
This section covers uploading TXT, CSV, and similar files.

SQLite Uploaders
""""""""""""""""""
.. autofunction:: text_to_sqlite
.. autofunction:: csv_to_sqlite
.. autofunction:: pgreaper.file_to_sqlite
  
'''

from .sqlite import file_to_sqlite

# SQLite Uploaders
def text_to_sqlite(*args, **kwargs):
    '''
    Uploads a TXT file to SQLite. This function merely calls 
    file_to_sqlite() with type='text' argument. See `file_to_sqlite()`
    documentation for full list of arguments.
    
    Basic Usage:
     >>> import pgreaper
     >>> pgreaper.text_to_sqlite('zip_codes.txt',
     ...    database='data.db', name='zip_codes')
     
    '''
    
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'
    
    file_to_sqlite(*args, **kwargs)
    
def csv_to_sqlite(*args, **kwargs):
    '''
    Uploads a CSV file to SQLite. This function merely calls 
    file_to_sqlite() with type='csv' argument. See `file_to_sqlite()`
    documentation for full list of arguments.
    
    Basic Usage:
     >>> import pgreaper
     >>> pgreaper.csv_to_sqlite('zip_codes.csv',
     ...    database='data.db', name='zip_codes')
     
    '''

    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','

    file_to_sqlite(*args, **kwargs)