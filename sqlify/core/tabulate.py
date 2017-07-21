''' Factories and functions for creating and converting Tables '''

from sqlify.core.table import Table
from sqlify.core.pgtable import PgTable
from sqlify.core._core import convert_schema

class Tabulate(object):
    ''' Factory for Table objects '''
    
    # Attributes to be copied when converting between Tables
    copy_attr = ['name', 'col_names', 'col_types', 'p_key']
    
    # def copy(table, row_values=None):
        # ''' Return a Table with the same attributes (but without the row_values) '''
        
        # table_kwargs = {}
        
        # for attr in Tabulate.copy_attr:
            # table_kwargs[attr] = getattr(table, attr)
            
        # return type(table)(row_values=row_values, **table_kwargs)
    
    def as_table(table):
        ''' Convert a PgTable to Table '''
        
        table_kwargs = {}
        
        for attr in Tabulate.copy_attr:
            table_kwargs[attr] = getattr(table, attr)
            
        return Table(row_values=table, **table_kwargs)
    
    def as_pgtable(table):
        ''' Convert a Table to PgTable '''
        
        table_kwargs = {}
        
        for attr in Tabulate.copy_attr:
            table_kwargs[attr] = getattr(table, attr)
            
        table_kwargs['col_types'] = convert_schema(table_kwargs['col_types'])
            
        return PgTable(row_values=table, **table_kwargs)
            
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
    as_table = staticmethod(as_table)
    as_pgtable = staticmethod(as_pgtable)