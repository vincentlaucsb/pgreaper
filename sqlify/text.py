'''
.. currentmodule:: sqlify

Uploading Delimiter-Separated File Formats
-------------------------------------------
This section covers uploading TXT, CSV, and similar files.

SQLite Uploaders
""""""""""""""""""
.. autofunction:: text_to_sqlite
.. autofunction:: csv_to_sqlite
.. autofunction:: sqlify.file_to_sqlite
  
PostgreSQL Uploaders
"""""""""""""""""""""
.. autofunction:: text_to_pg
.. autofunction:: csv_to_pg
.. autofunction:: sqlify.file_to_pg

'''

from .sqlite import file_to_sqlite
from .postgres import file_to_pg

# SQLite Uploaders
def text_to_sqlite(*args, **kwargs):
    '''
    Uploads a TXT file to SQLite. This function merely calls 
    file_to_sqlite() with type='text' argument. See `file_to_sqlite()`
    documentation for full list of arguments.
    
    Basic Usage:
     >>> import sqlify
     >>> sqlify.text_to_sqlite('zip_codes.txt',
     ...    database='data.db', name='zip_codes')
     
    '''
    
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'
    
    file_to_sqlite(type='text', *args, **kwargs)
    
def csv_to_sqlite(*args, **kwargs):
    '''
    Uploads a CSV file to SQLite. This function merely calls 
    file_to_sqlite() with type='csv' argument. See `file_to_sqlite()`
    documentation for full list of arguments.
    
    Basic Usage:
     >>> import sqlify
     >>> sqlify.csv_to_sqlite('zip_codes.csv',
     ...    database='data.db', name='zip_codes')
     
    '''

    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','

    file_to_sqlite(type='csv', *args, **kwargs)

# PostgreSQL Uploaders    
def text_to_pg(*args, **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'

    file_to_pg(type='text', *args, **kwargs)
    
def csv_to_pg(*args, **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','
        
    file_to_pg(type='csv', *args, **kwargs)