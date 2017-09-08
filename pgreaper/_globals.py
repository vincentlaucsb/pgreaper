''' Global Constants and Helpers '''

from importlib import import_module
import functools
import locale
import re
import os
import sys

PGREAPER_PATH = os.path.dirname(__file__)
SQLIFY_PATH = os.path.dirname(__file__)
DEFAULT_ENCODING = locale.getpreferredencoding()

'''
Example
--------
>>> PYTHON_VERSION
3.6.0
'''
PYTHON_VERSION = sys.version_info[0] + 0.1 * sys.version_info[1] + \
    sys.version_info[2]

# Load Postgres reserved keywords
with open(os.path.join(
    SQLIFY_PATH, 'data', 'pg_keywords.txt'), mode='r') as PG_KEYWORDS:
    PG_KEYWORDS = set([kw.replace('\n', '').lower() for kw in PG_KEYWORDS.readlines()])
    
''' Optional Dependencies '''
def import_package(name):
    ''' Returns a package if it is installed, None otherwise '''
    try:
        return import_module(name)
    except ImportError:
        return None
        
''' Helper objects '''
class ReusableContextManager(object):
    '''
    A reusable context manager which
     - is meant to be passed between functions
     - keep_alive parameter should be no more than the number
       of times we need to pass something around
    '''
    
    def __exit__(self, *args):
        if self.keep_alive:
            self.keep_alive -= 1
        else:
            self.close()
            
''' Name cleaners '''
def strip(string):
    ''' Removes or fixes no-nos from potential table and column names '''
    
    '''
    Replace bad characters
    Ref: https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    '''
    
    offending_characters = set(
        ['/', '\\', '(', ')', '.', ',', '-', ':', ';', '$',
         '$', '@', '~', '|', '`', '?', '!',
         '=', '+', '-', '#', '<', '>', '*', '^',
         '[', ']'])
    new_str = ""
    
    for char in string:
        if char not in offending_characters:
            new_str += char
        else:
            new_str += '_'
    
    # Replace bad characters with sensible replacements
    new_str = new_str.replace('%', 'percent').replace('&', 'and').replace("'", '').replace('\t', '')
    
    # Remove leading and trailing whitespace
    new_str = re.sub('^(?=) *|(?=) *$', repl='', string=new_str)
    
    # Replace whitespace with underscore
    new_str = new_str.replace(' ', '_')
            
    # Add underscore if name starts with a number
    if new_str[0].isnumeric():
        new_str = "_" + new_str
        
    # Replace multiple underscores with just one
    new_str = re.sub('_{2,}|_', repl='_', string=new_str)
    
    # Remove trailing underscores
    new_str = re.sub('_*$', repl='', string=new_str)

    return new_str
    
def preprocess(func):
    ''' Provides a default table name if needed '''
    
    @functools.wraps(func)
    def inner(*args, **kwargs):
        # Get filename argument
        try:
            file = kwargs['file']
        except KeyError:
            if isinstance(args[0], str):
                file = args[0]
            else:
                # ZipReader
                file = args[0].file
    
        # Use filename as default value for table name
        try:
            # Strip out file extension
            if not kwargs['name']:
                kwargs['name'] = strip(file.split('.')[0])
        except KeyError:
            kwargs['name'] = strip(file.split('.')[0])
           
        return func(*args, **kwargs)
    
    return inner
    
def resolve_duplicate(headers):
    '''
    Renames duplicate column names
    
    Arguments:
     * headers: A row of column headers
    '''
    
    def rename_column(name, n=0):
        '''
        A recursive function which deals with the hopefully unlikely
        possibility of 2 or more columns with the same name
        '''
        
        new_name = name
        
        # Attach a number next to duplicate column names
        if n > 0:
            new_name = "{name}_{num}".format(name=name, num=n)
        
        if new_name in headers_set:
            return rename_column(name, n=n+1)
        else:
            headers_set.add(new_name)
            return new_name
    
    headers_set = set()
    
    return [rename_column(i) for i in headers]