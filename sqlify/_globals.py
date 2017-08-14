''' Global Constants '''

from inspect import signature
import locale
import os

SQLIFY_PATH = os.path.dirname(__file__)
DEFAULT_ENCODING = locale.getpreferredencoding()

# Keyword arguments which indicate user wants to connect to a Postgres database
POSTGRES_CONN_KWARGS = set(['dbname', 'user', 'password', 'host'])

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