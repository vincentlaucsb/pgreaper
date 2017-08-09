''' Functions for inferring and converting schemas '''

from sqlify._globals import Singleton

from collections import defaultdict
from io import StringIO
import csv
import json

PY_TYPES_SQLITE = {
    'str': 'text',
    'int': 'integer',
    'float': 'real'
}

PY_TYPES_POSTGRES = {
    'str': 'text',
    'int': 'bigint',
    'float': 'double precision'
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

class SQLDialect(metaclass=Singleton):
    '''
    Should be placed as an attribute for Tables so they can properly infer schema
    
    Arguments:
     * name:        Name of the SQL dialect ('sqlite' or 'postgres')
     * py_types:    Mapping of Python types to SQL types
     * guesser:     Function for guessing data types
     * compatible:  A function for determining if two data types are compatible
     * table_exists:    A function for determining if a table exists
    '''
    
    def __init__(self, name, py_types, compatible):
        self.name = name
        self.py_types = py_types
        self.compatible = compatible
    
    def __eq__(self, other):
        ''' Make it so self == "name of SQL dialect" returns true '''
    
        if isinstance(other, str):
            if other == self.name:
                return True
            else:
                return False
        else:
            return super(SQLDialect, self).__eq__(other)
            
    def __str__(self):
        return self.name
            
    def __repr__(self):
        return self.name
        
    @staticmethod
    def to_string(table):
        string = StringIO()
        writer = csv.writer(string, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        
        for row in table:
            writer.writerow(row)
            
        string.seek(0)
        return string
    
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
            # Every 100 rows, check if text is there already
            if i > sample_n:
                break
            if i%100 == 0:
                remove = [j for j in check_these_cols if data_types[j]['text']]
                
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
    
        super(DialectSQLite, self).__init__(
            'sqlite', PY_TYPES_SQLITE, compatible)
        
    @staticmethod
    def guess_data_type(item):
        ''' Try to guess what data type a given string actually is '''
        
        if item is None:
            return 'integer'
        elif isinstance(item, int):
            return 'integer'
        elif isinstance(item, float):
            return 'real'
        else:
            # Strings and other types
            if item.isnumeric():
                return 'integer'
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.', it should be
                '''
                return 'real'
            else:
                return 'text'
        
class DialectPostgres(SQLDialect):
    def __init__(self):
        compatible = compatible_pg

        super(DialectPostgres, self).__init__('postgres',
            PY_TYPES_POSTGRES, compatible)
        
    @staticmethod
    def guess_data_type(item):
        if item is None:
            # Select option that would have least effect on choosing a type
            return 'bigint'
        elif isinstance(item, int):
            return 'bigint'
        elif isinstance(item, float):
            return 'double precision'
        else:
            # Strings and other types
            if item.isnumeric():
                return 'bigint'
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '-3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.' and '-', it should be
                '''
                return 'double precision'
            else:
                return 'text'

class DialectPostgresJSON(DialectPostgres):
    ''' A dialect for Postgres tables containing jsonb columns '''

    def __init__(self):
        super(DialectPostgresJSON, self).__init__()
    
    def guess_type(self, table, sample_n):
        ''' 
        Gets column types for a table, with possibilities:
        jsonb, text, double precision, bigint, boolean, datetime
        '''
       
        # Counter of data types per column
        data_types = [defaultdict(int) for col in table.col_names]
        check_these_cols = set([i for i, name in enumerate(table.col_names)])
        sample_n = min(len(table), sample_n)
        
        for i, row in enumerate(table):
            # Every 100 rows, check if jsonb is there already
            if i > sample_n:
                break
            if i%100 == 0:
                remove = [j for j in check_these_cols if data_types[j]['jsonb']]
                
                for j in remove:
                    check_these_cols.remove(j)
            if table.n_cols == 1:
                row = [row]
                
            # Loop over individual items
            for j in check_these_cols:
                data_types[j][self.guess_data_type(row[j])] += 1
        
        col_types = []
        
        for col in data_types:
        
            if col['jsonb']:
                this_col_type = 'jsonb'
            elif col['text']:
                this_col_type = 'text'
            elif bool(col['double precision'] or col['int']) + \
                bool(col['boolean']) + bool(col['datetime']) > 1:
                # If the above sum > 1, there are mixed incompatible types
                this_col_type = 'text'
            elif col['double precision']:
                this_col_type = 'double precision'
            elif col['bigint']:
                this_col_type = 'bigint'
            elif col['boolean']:
                this_col_type = 'boolean'
            # elif col['datetime']:
                # this_col_type = 'datetime'
            else:
                # Column of NULLs
                this_col_type = 'text'
            
            col_types.append(this_col_type)
            
        return col_types
        
    @staticmethod
    def guess_data_type(item):
        ''' A more extensive but also expensive type guesser for Postgres '''

        if item is None:
            return 'NULL'
        elif isinstance(item, dict):
            return 'jsonb'
        elif isinstance(item, bool):
            return 'boolean'
        elif isinstance(item, int):
            return 'bigint'
        elif isinstance(item, float):
            return 'double precision'
        else:
            # Strings and other types
            if item.isnumeric():
                return 'bigint'
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '-3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.' and '-', it should be
                '''
                return 'double precision'
            else:
                return 'text'
                
    @staticmethod
    def to_string(table):
        ''' Return table as a StringIO object for writing via copy() '''
        
        string = StringIO()
        writer = csv.writer(string, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        dict_encoder = json.JSONEncoder()
        
        jsonb_cols = set([i for i, j in enumerate(table.col_types) if j == 'jsonb'])
        
        for row in table:
            for i in jsonb_cols:
                row[i] = dict_encoder.encode(row[i])
        
            writer.writerow(row)
            
        string.seek(0)
        return string
                
def compatible_sqlite(a, b):
    ''' Return if type A can be stored in a column of type B '''
    
    if a == b or a == 'integer':
        return True
    else:
        # Map of types to columns they CANNOT be stored in
        compat = {
            'real': ['integer'],
            'text': ['integer', 'real'],
        }
        
        return bool(not(b in compat[a]))
            
def compatible_pg(a, b):
    ''' Return if type A can be stored in a column of type B '''
    
    if a == b or a == 'bigint':
        return True
    else:
        # Map of types to columns they CANNOT be stored in
        compat = {
            'double precision': ['bigint'],
            'text': ['bigint', 'double precision'],
        }
        
        return bool(not(b in compat[a]))