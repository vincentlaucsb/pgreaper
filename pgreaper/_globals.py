''' Global Constants '''

from importlib import import_module
import locale
import os
import sys

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