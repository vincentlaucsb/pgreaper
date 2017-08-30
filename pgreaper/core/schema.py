''' Functions for inferring and converting schemas '''

from pgreaper.core.mappings import SymmetricIndex, TwoWayMap

from collections import defaultdict
import psycopg2

########################################
# Mapping of Python types to SQL Types #
########################################
PY_TYPES = {
    'sqlite': TwoWayMap(lambda: 'text', {
        'str': 'text',
        'int': 'integer',
        'float': 'real',
        'bool': 'integer',
        'NoneType': 'null'
    }),
    'postgres': TwoWayMap(lambda: 'text', {
        'str': 'text',
        'int': 'bigint',
        'dict': 'jsonb',
        'list': 'jsonb',
        'float': 'double precision',
        'bool': 'boolean',
        'datetime': 'timestamp',
        'NoneType': 'null'
})}

# numpy Types to Postgres Types
# Reference: https://docs.scipy.org/doc/numpy/user/basics.types.html
NUMPY_TO_PG = {
    'bool_': 'boolean',
    'int_': 'bigint',
    'intc': 'bigint',
    'int8': 'smallint',
    'int16': 'smallint',
    'int32': 'integer',
    'int64': 'bigint',
    'float_': 'double precision',
    'float8': 'real',
    'float16': 'real',
    'float32': 'real',
    'float64': 'double precision'
}
    
#########################
# Compatibility Indexes #
#########################

SQLITE_COMPAT = SymmetricIndex()
SQLITE_COMPAT['null'] + {
    'integer': 'integer',
    'real': 'real',
    'text': 'text',
}
SQLITE_COMPAT['text'] + {
    'integer': 'text',
    'real': 'text',
}
SQLITE_COMPAT['real'] + {
    'integer': 'real'
}

POSTGRES_COMPAT = SymmetricIndex()
POSTGRES_COMPAT['jsonb'] + {
    'null': 'jsonb'
}
POSTGRES_COMPAT['text'] + {
    'bigint': 'text',
    'double precision': 'text',
    'datetime': 'text',
    'boolean': 'text'
}
POSTGRES_COMPAT['double precision'] + {
    'bigint': 'double precision'
}

# Make everything compatible with itself
# Also make null + <any type> = <any type>
for k in SQLITE_COMPAT:
    SQLITE_COMPAT[k] + {k: k}
    SQLITE_COMPAT['null'] + {k: k}

for k in POSTGRES_COMPAT:
    POSTGRES_COMPAT[k] + {k: k}
    POSTGRES_COMPAT['null'] + {k: k}

COMPAT = dict(sqlite=SQLITE_COMPAT, postgres=POSTGRES_COMPAT)

# Add numpy types
for k, v in zip(NUMPY_TO_PG.keys(), NUMPY_TO_PG.values()):
    PY_TYPES['postgres'][k] = v

class SQLType(object):
    ''' Maps Python types to SQL types '''
    
    __slots__ = ['name', 'table']
    
    def __init__(self, type_, table=None):
        '''
        Parameters
        -----------
        table:      Table
                    Pointer to Table this type is connected to
        type_       type() or str
                    Python type this type corresponds to
                    
        Attributes
        -----------
        name:       Python name of this type
        '''
        
        super(SQLType, self).__init__()
        self.table = table
        
        try:
            self.name = type_.__name__
        except AttributeError:
            if isinstance(type_, str):
                self.name = type_
            else:
                raise ValueError('type_ must be a type or str')
        
    def __add__(self, other):
        '''
        Returns str
        
        Example
        --------
        SQLType: text + SQLType: int
        should return the type required to store both in the same column
        '''

        if isinstance(other, SQLType) and (self.name == other.name):
            return self
        elif isinstance(other, type) and self.table:
            # Allow comparison with type objects
            # Shortcut if Table dialect is defined
            other = PY_TYPES[self.table.dialect][other.__name__]
            return COMPAT[self.table.dialect][str(self)][other]
        else:
            # Allow comparison with type objects
            if isinstance(other, type):
                other = SQLType(other, table=self.table)
            return COMPAT[self.table.dialect][str(self)][str(other)]

    def __eq__(self, other):
        '''
        If compared to a string that is the same as the type's name return True
        '''
        
        if isinstance(other, str):
            if (other == self.name) or \
                (other == PY_TYPES[self.table.dialect][self.name]):
                return True
        else:
            return super(SQLType, self).__eq__(other)
            
    def __repr__(self):
        return 'SQLType: {}'.format(self.name)
        
    def __str__(self):
        '''
        Return a string of its type corresponding to the current SQL dialect
        by looking at the right dictionary
        '''
    
        if self.table is not None:
            return PY_TYPES[self.table.dialect][self.name]
        else:
            return self.name