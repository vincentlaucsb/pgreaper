''' Functions for inferring and converting schemas '''

from sqlify.core.mappings import SymmetricIndex
from sqlify._globals import Singleton

from collections import defaultdict
from io import StringIO
import csv
import json

SQLITE_COMPAT = SymmetricIndex()
SQLITE_COMPAT['text'] = {
    'integer': 'text',
    'real': 'text',
}
SQLITE_COMPAT['real'] = {
    'integer': 'real'
}

POSTGRES_COMPAT = SymmetricIndex()
POSTGRES_COMPAT['text'] = {
        'bigint': 'text',
        'double precision': 'text',
        'datetime': 'text'
    }
POSTGRES_COMPAT['double precision'] = {
    'bigint': 'double precision'
}

COMPAT = dict(sqlite=SQLITE_COMPAT, postgres=POSTGRES_COMPAT)

class SQLType(object):
    '''
    Maps Python types to SQL types
    '''
        
    sqlite = defaultdict(lambda: 'text', {
        'str': 'text',
        'int': 'integer',
        'float': 'real',
        'boolean': 'integer'
    })

    postgres = defaultdict(lambda: 'text', {
        'str': 'text',
        'int': 'bigint',
        'dict': 'jsonb',
        'float': 'double precision',
        'boolean': 'boolean',
        'datetime': 'timestamp'
    })
    
    __slots__ = ['name', 'sqlite_name', 'postgres_name', 'table']
    
    def __init__(self, type_, table=None):
        '''
        Parameters
        -----------
        table:      Table
                    Pointer to Table this type is connected to
        '''
        
        super(SQLType, self).__init__()
        self.table = table
        
        name = type_.__name__
        self.name = name
        self.sqlite_name = self.sqlite[name]
        self.postgres_name = self.postgres[name]
        
    def __add__(self, other):
        '''
        Returns str
        
        Example
        --------
        SQLType: text + SQLType: int
        should return the type required to store both in the same column
        '''

        if self.name == other.name:
            return self
        else:
            return COMPAT[str(self.table.dialect)][str(self)][str(other)]

    def __repr__(self):
        return 'SQLType: {}'.format(self.name)
        
    def __str__(self):
        '''
        Return a string of its type corresponding to the current SQL dialect
        '''
    
        if self.table is not None:
            return getattr(self, '{}_name'.format(self.table.dialect))
        else:
            return self.name

class SQLDialect(metaclass=Singleton):
    '''
    Should be placed as an attribute for Tables so they can properly infer schema
    
    Arguments:
     * name:        Name of the SQL dialect ('sqlite' or 'postgres')
     * py_types:    Mapping of Python types to SQL types
     * guesser:     Function for guessing data types
     * table_exists:    A function for determining if a table exists
    '''
    
    def __init__(self, name):
        self.name = name
        
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
        
class DialectSQLite(SQLDialect):
    def __init__(self):
        super(DialectSQLite, self).__init__('sqlite')
        
class DialectPostgres(SQLDialect):
    def __init__(self):
        super(DialectPostgres, self).__init__('postgres')
            
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