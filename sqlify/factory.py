from sqlify.table import Table
from sqlify.postgres.table import PgTable

class Tabulate(object):
    ''' Factory for Table objects '''
    
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
            tbl_obj = Table
        if engine == "postgres":
            tbl_obj = PgTable
            
        return tbl_obj(*args, **kwargs)
    
    factory = staticmethod(factory)