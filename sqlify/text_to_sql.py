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
Before uploading to Postgres, you may want to configure the default 
connection settings. If default settings are provided, SQLify can use 
these to create new databases so you won't have to create them manually.

.. autofunction:: sqlify.settings
.. autofunction:: text_to_pg
.. autofunction:: csv_to_pg
.. autofunction:: sqlify.file_to_pg

JSON to Postgres
-----------------
.. autofunction:: sqlify.json_to_pg

Details
--------
Type-Guessing
""""""""""""""
Columns types are inferred based on the data type of the entries in the first 100 lines of the file. When uploading to Postgres, the data type for a column is based on the type that accomodates all entries in that column. For example, if a column has 9999 integers and 1 string, then the column type is set to "text".

Rejected Rows
""""""""""""""
Because data is not perfect, there may be a few records which do not mesh
with the specified schema. Because Postgres is strongly-typed, these records
must either be discarded to stored elsewhere. SQLify handles this issue by 
storing rejected rows in a table where the columns have the same name as the 
original, but all types are set to "text". The name of this table is original 
table name followed by "_reject". For example, if a table is named "us_census"
then the table of rejected records is called "us_census_reject".

'''

from .core import read_json
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
    
    file_to_sqlite(*args, **kwargs)
    
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

    file_to_sqlite(*args, **kwargs)

# PostgreSQL Uploaders    
def text_to_pg(*args, **kwargs):
    '''
    Uploads a TXT file to PostgreSQL. This function merely calls 
    `file_to_pg()` with type='text' argument. See `file_to_pg()`
    documentation for full list of arguments.
    
    Basic Usage:
     >>> import sqlify
     >>> sqlify.text_to_pg('slim_shady.txt',
     ...    database='stan_db',
     ...    name='slim_shady',
     ...    username='hailie',
     ...    password='ithinkmydadiscrazy'
     ...    host='localhost')
     
    .. note:: The name argument here was unnecessary, because the
       filename without the extension is used as a fallback for the 
       table name.
    '''

    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'

    file_to_pg(*args, **kwargs)
    
def csv_to_pg(*args, **kwargs):
    '''
    Uploads a CSV file to PostgreSQL. This function merely calls 
    `file_to_pg()` with type='csv' argument. See `file_to_pg()`
    documentation for full list of arguments.
    
    **Basic Usage:**
     >>> import sqlify
     >>> sqlify.text_to_pg('fifty_cent.tsv',
     ...    database='gunit',
     ...    name='fifty_cent',
     ...    delimiter='\\t')
     
    .. note:: .tsv is the "tab-separated values" file format     
    '''

    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','
        
    file_to_pg(*args, **kwargs)
    
def json_to_pg(file, name=None, extract=None):
    '''
    Uploads a JSON file to PostgreSQL.
    '''
    
    with open(file, mode='r') as json_file:
        pass