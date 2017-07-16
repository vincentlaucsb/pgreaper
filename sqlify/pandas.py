from .core import Tabulate

try:
    import pandas
    PANDAS_INSTALLED = True
except ImportError:
    PANDAS_INSTALLED = False
    
def _assert_pandas(func):
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
    
def pandas_to_table(df, engine='sqlite'):
    ''' Takes a pandas DataFrame and returns a Table '''
    
    col_names = None
    new_table = None
    
    for row in df.itertuples(index=False):
        if not col_names:
            col_names = row._fields
        
            new_table = Tabulate.factory(
                engine, n_cols=len(col_names), col_names=col_names,
                name="pandas DataFrame")
                
        new_table.append(row)
        
    return new_table