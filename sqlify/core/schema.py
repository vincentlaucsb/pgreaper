''' Functions for inferring and converting schemas '''

from sqlify.core.mappings import SymmetricIndex
from .schema_numpy import numpy_to_pg

from collections import defaultdict
import psycopg2

###########################
# Giant pile of duct tape #
###########################

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
    'datetime': 'text'
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

class SQLType(object):
    '''
    Maps Python types to SQL types
    '''
        
    sqlite = defaultdict(lambda: 'text', {
        'str': 'text',
        'int': 'integer',
        'float': 'real',
        'boolean': 'integer',
        'NoneType': 'null'
    })

    postgres = defaultdict(lambda: 'text', {
        'str': 'text',
        'int': 'bigint',
        'dict': 'jsonb',
        'list': 'jsonb',
        'float': 'double precision',
        'boolean': 'boolean',
        'datetime': 'timestamp',
        'NoneType': 'null'
    })
    
    # Add numpy types
    for k, v in zip(numpy_to_pg.keys(), numpy_to_pg.values()):
        postgres[k] = v
    
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

        if isinstance(other, SQLType) and (self.name == other.name):
            return self
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
            if other == self.name or other == getattr(
                self, '{}_name'.format(self.table.dialect)):
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
            return getattr(self, '{}_name'.format(self.table.dialect))
        else:
            return self.name