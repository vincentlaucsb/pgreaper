''' Functions for guessing data types '''

# Mapping of Python types to SQLite Types
PYTYPES = {
    'str': 'TEXT',
    'int': 'INTEGER',
    'float': 'REAL'
}

PYTYPES_PG = {
    'str': 'TEXT',
    'int': 'BIGINT',
    'float': 'REAL'
}

def guess_data_type(item):
    ''' Try to guess what data type a given string actually is '''
    
    if item is None:
        return 'INTEGER'
    elif isinstance(item, int):
        return 'INTEGER'
    elif isinstance(item, float):
        return 'REAL'
    else:
        # Strings and other types
        if item.isnumeric():
            return 'INTEGER'
        elif (not item.isnumeric()) and (item.replace('.', '', 1).isnumeric()):
            '''
            Explanation:
             * A floating point number, e.g. '3.14', in string will not be 
               recognized as being a number by Python via .isnumeric()
             * However, after removing the '.', it should be
            '''
            return 'REAL'
        else:
            return 'TEXT'
            
def guess_data_type_pg(item):
    if item is None:
        # Select option that would have least effect on choosing a type
        return 'BIGINT'
    elif isinstance(item, int):
        return 'BIGINT'
    elif isinstance(item, float):
        return 'DOUBLE PRECISION'
    else:
        # Strings and other types
        if item.isnumeric():
            return 'BIGINT'
        elif (not item.isnumeric()) and (item.replace('.', '', 1).isnumeric()):
            '''
            Explanation:
             * A floating point number, e.g. '3.14', in string will not be 
               recognized as being a number by Python via .isnumeric()
             * However, after removing the '.', it should be
            '''
            return 'DOUBLE PRECISION'
        else:
            return 'TEXT'

def compatible(a, b):
    ''' Return if type A can be stored in a column of type B '''
    
    if a == b or a == 'INTEGER':
        return True
    else:
        # Map of types to columns they CANNOT be stored in
        compat = {
            'REAL': ['INTEGER'],
            'TEXT': ['INTEGER', 'REAL'],
        }
        
        return bool(not(b in compat[a]))
            
def compatible_pg(a, b):
    ''' Return if type A can be stored in a column of type B '''
    
    if a == b or a == 'BIGINT':
        return True
    else:
        # Map of types to columns they CANNOT be stored in
        compat = {
            'DOUBLE PRECISION': ['BIGINT'],
            'TEXT': ['BIGINT', 'DOUBLE PRECISION'],
        }
        
        return bool(not(b in compat[a]))