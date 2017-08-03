''' Functions for inferring and converting schemas '''

from collections import defaultdict

PY_TYPES_SQLITE = {
    'str': 'TEXT',
    'int': 'INTEGER',
    'float': 'REAL'
}

PY_TYPES_POSTGRES = {
    'str': 'TEXT',
    'int': 'BIGINT',
    'float': 'DOUBLE PRECISION'
}

def convert_schema(types, from_, to_):
    '''
    Convert SQLite column types to Postgres column types or vice-versa
    
    Arguments:
     * types:           String or list of strings (types)
     * from_, to_:      "sqlite" to "postgres" or vice-versa
    '''
    
    # Use existing py_types dictionaries to avoid duplication of effort
    if from_ == 'sqlite':
        source_types = PY_TYPES_SQLITE
    elif from_ == 'postgres':
        source_types = PY_TYPES_POSTGRES
        
    if to_ == 'postgres':
        dest_types = PY_TYPES_POSTGRES
    if to_ == 'sqlite':
        dest_types = PY_TYPES_SQLITE        
    
    # Should map source dtypes to destination dtypes
    convert = { source_types[dtype].lower(): dest_types[dtype].lower() \
        for dtype in source_types.keys() }
    
    def convert_type(type):
        ''' Converts a single data type '''
        type = type.lower()
        
        try:
            return convert[type]
        except KeyError:
            return type
    
    if isinstance(types, str):
        return convert_type(type)
    elif isinstance(types, list):
        return [convert_type(i) for i in types]
    else:
        raise ValueError('Argument must either be a string or a list of strings.')
    
    return types

class SQLDialect(object):
    '''
    Should be placed as an attribute for Tables so they can properly infer schema
    
    Arguments:
     * py_types:    Mapping of Python types to SQL types
     * guesser:     Function for guessing data types
     * compatible:  A function for determining if two data types are compatible
     * table_exists:    A function for determining if a table exists
    '''
    
    def __init__(self, py_types, compatible):
        self.py_types = py_types
        
        # Dynamically add methods
        self.compatible = compatible
    
    def guess_type(self, table, sample_n):
        ''' Default column type guesser '''
        
        # Get dialect information
        py_types = table.dialect.py_types
        str_type = py_types['str']
        float_type = py_types['float']
        int_type = py_types['int']
        
        # Counter of data types per column
        data_types = [defaultdict(int) for col in table.col_names]
        check_these_cols = set([i for i in range(0, table.n_cols)])
        sample_n = min(len(table), sample_n)
        
        for i, row in enumerate(table):
            # Every 100 rows, check if TEXT is there already
            if i > sample_n:
                break
            if i%100 == 0:
                remove = [j for j in check_these_cols if data_types[j]['TEXT']]
                
                for j in remove:
                    check_these_cols.remove(j)
            if table.n_cols == 1:
                row = [row]
                
            # Loop over individual items
            for j in check_these_cols:
                data_types[j][self.guess_data_type(row[j])] += 1
        
        # Get most common type
        # col_types = [max(data_dict, key=data_dict.get) for data_dict in data_types]
        
        col_types = []
        
        for col in data_types:
            if col[str_type]:
                this_col_type = str_type
            elif col[float_type]:
                this_col_type = float_type
            else:
                this_col_type = int_type
            
            col_types.append(this_col_type)
            
        return col_types
        
class DialectSQLite(SQLDialect):
    def __init__(self):
        compatible = compatible_sqlite
    
        super(DialectSQLite, self).__init__(PY_TYPES_SQLITE, compatible)
        
    def __repr__(self):
        return "sqlite"
        
    @staticmethod
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
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.', it should be
                '''
                return 'REAL'
            else:
                return 'TEXT'
        
class DialectPostgres(SQLDialect):
    def __init__(self):
        compatible = compatible_pg

        super(DialectPostgres, self).__init__(PY_TYPES_POSTGRES, compatible)
        
    def __repr__(self):
        return "postgres"
        
    @staticmethod
    def guess_data_type(item):
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
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '-3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.' and '-', it should be
                '''
                return 'DOUBLE PRECISION'
            else:
                return 'TEXT'

def compatible_sqlite(a, b):
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