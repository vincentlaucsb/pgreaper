''' Factories and functions for creating and converting Tables '''

from sqlify.core.table import Table
from sqlify.core.schema import convert_schema, DialectSQLite, DialectPostgres

class Tabulate(object):
    ''' Factory for Table objects '''
    
    # Attributes to be copied when converting between Tables
    copy_attr = ['name', 'col_names', 'col_types', 'p_key']
    
    def factory(engine, n_cols=0, *args, **kwargs):
        ''' Arguments:
         * engine:          SQLite or Postgres
         * n_cols:          Number of columns table has
         * args, kwargs:    Arguments to be passed to Table constructors
        '''
        
        try:
            col_names = kwargs['col_names']
        except KeyError:
            col_names = None
        
        if not n_cols:
            try:
                n_cols = len(col_names)
            except TypeError:
                raise ValueError('Please specify at least one column name.')
        
        if engine == "sqlite":
            dialect = DialectSQLite()
        if engine == "postgres":
            dialect = DialectPostgres()
        
        return Table(dialect=dialect, *args, **kwargs)
    
    factory = staticmethod(factory)