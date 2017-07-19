from .table import Table
from ._guess_dtype import PYTYPES_PG, guess_data_type_pg, compatible_pg

import collections
from collections import Counter

class PgTable(Table):
    '''
    Python representation of a PostgreSQL Table
    
    Arguments:
     * See comments for Table from sqlify.table
    '''
    
    __slots__ = ['name', 'n_cols', 'col_names', 'col_types', 'p_key',
        'pytypes', 'guess_func', 'compat_func', 'index']

    def __init__(self, *args, **kwargs):
        super(PgTable, self).__init__(
            pytypes=PYTYPES_PG,
            guess_func=guess_data_type_pg,
            compat_func=compatible_pg,
            *args, **kwargs)
        
        # Used when dealing with errors in copy_from()
        self.index = 0

    def __getitem__(self, key):
        # TO DO: Make slice operator return a Table object not a list

        if isinstance(key, slice):
            return PgTable(
                name=self.name,
                col_names=self.col_names,
                col_types=self.col_types,
                row_values=super(PgTable, self).__getitem__(key))
        else:
            return super(PgTable, self).__getitem__(key)
        
    # The read() and readline() methods make this class mimic a file-like
    # object so it can be used with copy_from()
    def read(self, *args):
        ''' Returns the next line from the Table as a tab separated string'''
        
        self.index += 1
        
        try:
            return "\t".join(self[self.index - 1]) + "\n"
            
        # Need to type cast to string
        except TypeError:
            return "\t".join(str(i) for i in self[self.index - 1]) + "\n"
            
        # End of Table
        except IndexError:
            return ""
        
    def readline(self, *args):
        ''' Ignores arguments given (byte-size) and just returns one line '''
        return self.read()