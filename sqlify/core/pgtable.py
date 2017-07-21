from .table import Table
from ._guess_dtype import PYTYPES_PG, guess_data_type_pg, compatible_pg

from io import StringIO
from csv import writer as csv_writer, QUOTE_MINIMAL
from collections import Counter
import collections

class PgTable(Table):
    '''
    Python representation of a PostgreSQL Table
    
    Arguments:
     * See comments for Table from sqlify.table
    '''
    
    __slots__ = ['name', 'n_cols', 'col_names', 'col_types', 'p_key',
        'pytypes', 'guess_func', 'compat_func', 'read_index']

    def __init__(self, *args, **kwargs):
        super(PgTable, self).__init__(
            pytypes=PYTYPES_PG,
            guess_func=guess_data_type_pg,
            compat_func=compatible_pg,
            *args, **kwargs)
            
        # self.read_index = -1

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
        
    # def read(self, *args):
        # self.read_index += 1
    
        # try:
            # return '|'.join(i for i in self[self.read_index]) + '\n'
        # except TypeError:
            # return '|'.join(str(i) for i in self[self.read_index]) + '\n'
        # except IndexError:  # List is empty
            # return ''
            
    # def readlines(self, *args):
        # return self.read()
        
    def to_string(self):
        ''' Return this table as a StringIO object for writing via copy() '''
        
        string = StringIO()
        writer = csv_writer(string, delimiter=",", quoting=QUOTE_MINIMAL)
        
        for row in self:
            writer.writerow(row)
            # string.write(str('|').join(i for i in row) + '\n')
            
        string.seek(0)
            
        return string