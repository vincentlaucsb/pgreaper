from collections import defaultdict

# Reference: https://docs.scipy.org/doc/numpy/user/basics.types.html

numpy_to_pg = defaultdict(lambda: 'text', {
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
})