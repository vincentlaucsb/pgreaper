''' General Helper Functions '''

import re

def alias_kwargs(func):
    ''' Creates aliases for common SQL keyword arguments '''
    
    # Dictionary of aliases and their replacements
    rep_key = {
        # Data file keywords
        'delim': 'delimiter',
        'sep': 'delimiter',
        'separator': 'delimiter',
    
        # Postgres connection keywords
        'db': 'database',
        'host': 'hostname',
        'user': 'username',
        # 'pass': 'password', -- Can't do that because it's a Python keyword
        'pw': 'password'
    }

    def inner(*args, **kwargs):
        for key in kwargs:
            if key in rep_key:
                new_key = rep_key[key]
                val = kwargs[key]
                
                # Swap values between old and new key
                kwargs[new_key] = val
                del kwargs[key]
    
        return func(*args, **kwargs)
        
    return inner

def sanitize_table(obj):
    '''
    Remove bad characters from column names
    
    Arguments:
     * obj = A Table object
    
    This function has no return value--it modifies Tables in place.
    '''
    
    new_col_names = [_strip(name) for name in obj.col_names]
    obj.col_names = new_col_names

def strip(string):
    ''' Removes or fixes no-nos from potential table and column names '''
    
    # Replace bad characters
    offending_characters = ['.', ',', '-', ';', "'", '$']
    new_str = ""
    
    for char in string:
        if char not in offending_characters:
            new_str += char
        else:
            new_str += '_'
            
    # Add underscore if name starts with a number
    numbers = list(range(0, 10))
    starts_with_number = bool(True in [string.startswith(str(n)) for n in numbers])
    
    if starts_with_number:
        new_str = "_" + new_str
    
    # Remove white space
    if ' ' in string:
        new_str = new_str.replace(' ','')
    
    return new_str
    
def preprocess(func):
    '''
    Performs similar things for text_to_table and csv_to_table
     * Provides a default table name if needed
     * Cleans up arguments passed in from command line
    '''
    
    def inner(*args, **kwargs):
        # Get filename argument
        try:
            file = kwargs['file']
        except KeyError:
            file = args[0]
    
        # Use filename as default value for table name
        try:
            # Strip out file extension
            if not kwargs['name']:
                kwargs['name'] = _strip(file.split('.')[0])
        except KeyError:
            kwargs['name'] = _strip(file.split('.')[0])

        '''
        Clean up delimiter argument passed in from command line, for example:
        
        >>> '\\t'
        '''

        try:
            if '\\t' in kwargs['delimiter']:
                kwargs['delimiter'] = '\t'
        except KeyError:
            pass
           
        return func(*args, **kwargs)
    
    return inner
    
def resolve_duplicate(headers):
    '''
    Renames duplicate column names
    
    Arguments:
     * headers: A row of column headers
    '''
    
    # import pdb; pdb.set_trace()
    
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