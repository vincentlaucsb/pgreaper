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
        'defaultdict': 'jsonb',
        'OrderedDict': 'jsonb',
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