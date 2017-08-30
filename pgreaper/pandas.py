'''
.. currentmodule:: pgreaper

pandas Integration
===================

Uploading DataFrames to Postgres
----------------------------------
.. autofunction:: pandas_to_pg

'''

from pgreaper._globals import import_package
from .core.table import Table
from .postgres import table_to_pg
from .postgres.conn import postgres_connect

import functools
pandas = import_package('pandas')
    
def _assert_pandas(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if pandas:
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
def pandas_to_table(df, dialect='postgres', mutable=True):
    '''
    Takes a pandas DataFrame and returns a Table
    
    Arguments:
     * mutable:     Should table be editable (i.e. convert tuples)
    '''
    
    col_names = df.columns.values.tolist()
    new_table = Table(dialect=dialect,
        col_names=col_names,
        name="pandas DataFrame")
    
    for row in df.itertuples(index=False):
        if mutable:
            new_table.append(list(row))
        else:
            new_table.append(row)
            
    new_table.guess_type()
    return new_table

@_assert_pandas
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
    
    table = pandas_to_table(df, dialect='postgres', mutable=False)
    table_to_pg(table, name=name, null_values='nan', conn=conn, find_rejects=False)