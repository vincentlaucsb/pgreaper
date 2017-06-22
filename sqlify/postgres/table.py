from sqlify.table import Table

import collections
from collections import Counter

def _guess_data_type_pg(item):
    if item is None:
        # Select option that would have least effect on choosing a type
        return 'BIGINT'
    elif isinstance(item, int):
        return 'BIGINT'
    elif isinstance(item, float):
        return 'DOUBLE PRECISION'
    else:
        # Strings and other types
        if item.isnumeric():
            return 'BIGINT'
        elif (not item.isnumeric()) and (item.replace('.', '', 1).isnumeric()):
            '''
            Explanation:
             * A floating point number, e.g. '3.14', in string will not be 
               recognized as being a number by Python via .isnumeric()
             * However, after removing the '.', it should be
            '''
            return 'DOUBLE PRECISION'
        else:
            return 'TEXT'
 
# Python representation of a PostgreSQL Table
class PgTable(Table):
    '''
    Arguments:
     * See comments for Table from sqlify.table
    '''

    def __init__(self, *args, **kwargs):
        super(PgTable, self).__init__(*args, **kwargs)
        
        # Used when dealing with errors in copy_from()
        self.index = 0
    
    def guess_type(self):
        '''        
        Guesses column data type by trying to accomodate all data, i.e.:
         * If a column has TEXT, INTEGER, and REAL, the column type is TEXT
         * If a column has INTEGER and REAL, the column type is REAL
         * If a column has REAL, the column type is REAL
        '''
        
        data_types_by_col = [list() for col in self.col_names]
        
        '''
        Get data types by column
         -> Use first 100 rows
        '''
        
        # Temporary: Ignore first row
        for row in self[1: 100]:
            # Each row only has one column
            if not isinstance(row, collections.Iterable):
                row = [row]

            # Loop over individual items
            for i in range(0, len(row)):
                data_types_by_col[i].append(_guess_data_type_pg(row[i]))
        
        # Get most common type
        col_types = []
        
        for col in data_types_by_col:
            counts = Counter(col)
            
            if counts['TEXT']:
                this_col_type = 'TEXT'
            elif counts['DOUBLE PRECISION']:
                this_col_type = 'DOUBLE PRECISION'
            else:
                this_col_type = 'BIGINT'
            
            col_types.append(this_col_type)
            
        return col_types
        
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
        try:
            self.index += 1
            return "\t".join(self[self.index - 1]) + "\n"
            
        # End of Table
        except IndexError:
            return ""
        
    def readline(self, *args):
        ''' Ignores arguments given (byte-size) and just returns one line '''
        return self.read()