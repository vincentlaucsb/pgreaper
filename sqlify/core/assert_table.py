from functools import wraps
from inspect import signature

from .table import Table

def assert_table(func=None, dialect=None):
    '''
    Makes sure the 'table' argument is actually a Table
    
    Parameters
    -----------
    dialect:    Subclass of DialectPostgres
                Which dialect the Table should have    
    '''
    
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
        
            table_arg = signature(func).bind(*args, **kwargs).arguments['table']
            if not isinstance(table_arg, Table):
                raise TypeError('This function only works for Table objects.')
            else:
                if str(table_arg.dialect) != dialect:
                    table_arg.dialect = dialect # Should also convert schema
            return func(*args, **kwargs)
        return inner
        
    if func:
        return decorator(func)
    
    return decorator