'''
.. currentmodule:: sqlify

pandas Integration
===================

Uploading DataFrames to Postgres
----------------------------------
.. autofunction:: pandas_to_pg

'''

from .core._core import alias_kwargs
from .core.table import Table
from .postgres import table_to_pg
from .postgres.conn import postgres_connect

import functools

try:
    import pandas
    PANDAS_INSTALLED = True
except ImportError:
    PANDAS_INSTALLED = False
    
def _assert_pandas(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if PANDAS_INSTALLED:
            return func(*args, **kwargs)
        else:
            raise ImportError('The pandas package must be installed for this feature.')
            
    return inner
    
@_assert_pandas
def table_to_pandas(table):
    ''' Takes a Table or PgTable and returns a pandas DataFrame '''
    
    return pandas.DataFrame(
        data = list(table),
        columns = table.col_names
    )
    
@_assert_pandas
def pandas_to_table(df, engine='sqlite', mutable=True):
    '''
    Takes a pandas DataFrame and returns a Table
    
    Arguments:
     * mutable:     Should table be editable (i.e. convert tuples)
    '''
    
    col_names = df.columns.values.tolist()
    
    # Map pandas types to SQLite types
    pandas_types = {
        'int64': 'INTEGER',
        'bool': 'BOOLEAN',
        'object': 'TEXT',
        'float64': 'REAL'   
    }
    
    col_types = [pandas_types[str(dtype)] for dtype in df.dtypes]
    
    new_table = Table(
                    dialect=engine, n_cols=len(col_names),
                    col_names=col_names,
                    col_types=col_types,
                    name="pandas DataFrame")
    
    for row in df.itertuples(index=False):
        if mutable:
            new_table.append(list(row))
        else:
            new_table.append(row)

    return new_table
    
@alias_kwargs
@postgres_connect
def pandas_to_pg(df, name, conn=None, **kwargs):
    '''
    Upload a pandas DataFrame to a PostgreSQL database
     * This function uses the schema inferred by pandas

    Parameters
    ----------
    df:       pandas DataFrame
              A pandas DataFrame
    name:     str
              Name of table to create
    database: Database to upload to
    '''
    
    table_to_pg(pandas_to_table(df, mutable=False),
        name=name, null_values='nan', conn=conn, find_rejects=False)