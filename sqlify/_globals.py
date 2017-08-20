''' Global Constants '''

from inspect import signature
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

''' Optional Dependencies '''
def import_package(name):
    ''' Returns a package if it is installed, None otherwise '''
    try:
        return import_module(name)
    except ImportError:
        return None

''' Other Stuff '''

class Singleton(type):
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
        
def arg_parse(func, kwargs):
    '''
    A smart way to pass arbitrary keyword arguments to functions
    
    Purpose
    ------------
     * Takes in a list of kwargs
     * Looks at the function signature
     * Determines which keyword args should be passed    
    
    Parameters
    -----------
     * kwargs:     Keyword arguments to parse and pass
     * func:       Function to be called
     
    Return
    -------
     * Filtered list of keyword Arguments
    '''
    
    args = [arg for arg in signature(func).parameters]
    return {arg: kwargs[arg] for arg in kwargs if arg in args}