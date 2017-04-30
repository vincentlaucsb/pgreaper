import re

from sqlify.table import Table

# Remove bad characters from column names
def _sanitize_table(obj):
    '''
    Arguments:
     * obj = A Table object
    
    This function has no return value--it modifies Tables in place.
    '''
    
    new_col_names = [_strip(name) for name in obj.col_names]
    obj.col_names = new_col_names

# Removes or fixes no-nos from potential table and column names
def _strip(string):
    # Replace bad characters
    offending_characters = ['.', ',', '-', ';']
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
    
# Performs similar things for text_to_table and csv_to_table
def _preprocess(func):
    def inner(*args, **kwargs):
        # Use filename as default value for table name
        try:
            if not kwargs['name']:
                # Strip out file extension
                kwargs['name'] = _strip(kwargs['file'].split('.')[0])
        except KeyError:
            kwargs['name'] = _strip(kwargs['file'].split('.')[0])

        '''
        Clean up delimiter argument passed in from command line
         * Ex: '\\t'
        '''

        try:
            if '\\t' in kwargs['delimiter']:
                kwargs['delimiter'] = '\t'
        except KeyError:
            pass
           
        return func(*args, **kwargs)
    
    return inner