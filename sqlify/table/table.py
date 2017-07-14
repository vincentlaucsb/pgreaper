from sqlify._sqlify import strip

import collections
import warnings
from collections import Counter, deque, OrderedDict
    
class Table(list):
    '''
    Python representation of a table

    Arguments:
     * name:       Name of the table (Required)
     * col_names:  A list specifying names of columns (Required)
      * col_types:  A list specifying the column types
      * row_values: A list of rows (i.e. a list of lists)
      * col_values (can be used instead of row_values): A list of column values
     * p_key:      Index of column used as a primary key
    Output:
     ...
    '''
    
    def __init__(self, name, col_names=None, col_types=None, p_key=None, *args, **kwargs):
        self.name = name
        self.col_names = list(col_names)
        self.n_cols = len(self.col_names)
        
        # Set column names and row values        
        if 'col_values' in kwargs:
            # Convert columns to rows
            n_rows = range(0, len(kwargs['col_values'][0]))
            row_values = [[col[row] for col in kwargs['col_values']] for row in n_rows]
        elif 'row_values' in kwargs:
            row_values = kwargs['row_values']
        else:
            row_values = []
        
        super(Table, self).__init__(row_values)
        
        # Set column types
        # Note: User input not completely validated, e.g. whether the data 
        # type is an actual sqlite data type is not checked
        if col_types:
            if isinstance(col_types, list) or isinstance(col_types, tuple):
                if len(col_types) != len(self.col_names):
                    warnings.warn('Table has {0} columns but {1} column types are specified. The shorter list will be filled with placeholder values.'.format(len(self.col_names), len(col_types)))
                    
                    if len(col_types) < len(self.col_names):
                        while len(col_types) < len(self.col_names):
                            col_types.append('TEXT')
                    else:
                        while len(self.col_names) < len(col_types):
                            self.col_names.append('col')
                            
                self.col_types = col_types
                        
            # If col_types is a single string, set each column's type to 
            # that string
            elif isinstance(col_types, str):
                self.col_types = [col_types for col in self.col_names]
            else:   
                raise ValueError('Column types should either be a list, tuple, or string.')
        else:
            # No column types specified --> set to TEXT
            self.col_types = ['TEXT' for i in self.col_names]

        # Set primary key
        self.p_key = p_key
        
        if p_key is not None:
            try:
                self.col_types[p_key] += ' PRIMARY KEY'
            except:
                # No col_types
                pass
        
        # Additional file metadata
        self.raw_header = []
        self.raw_skip_lines = []
        
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
                data_types_by_col[i].append(_guess_data_type(row[i]))
        
        # Get most common type
        col_types = []
        
        for col in data_types_by_col:
            counts = Counter(col)
            
            if counts['TEXT']:
                this_col_type = 'TEXT'
            elif counts['REAL']:
                this_col_type = 'REAL'
            else:
                this_col_type = 'INTEGER'
            
            col_types.append(this_col_type)
            
        return col_types
    
    def __repr__(self):
        ''' Print a short and useful summary of the table '''
    
        def trim(string, length=15):
            ''' Trim string to specified length '''
            string = str(string)
            
            if len(string) > length:
                return string[0: length - 3] + "..."
            else:
                return string
            
        def trim_row(row_start, row_end, cols=8):
            ''' Trim a row to a limited number of columns
             * row_start: First row to print 
             * row_end:   Last row to print
             * cols:      Number of columns from rows to show
            '''
            
            text = ""
            
            for row in self[row_start: row_end]:
                text += "".join(['| {:^15} '.format(trim(item)) for item in row[0:8]])
                text += "\n"
            
            return text
            
        ''' Only print out first 5 and last 5 rows '''
        text = "".join(['| {:^15} '.format(trim(name)) for name in self.col_names[0:8]])
        text += "\n"
        
        # Add column types
        text += "".join(['| {:^15} '.format(trim(ctype)) for ctype in self.col_types[0:8]])
        text += "\n"
        
        text += '-' * min(len(text), 120)
        text += "\n"
        
        # Add first first rows of data
        text += trim_row(row_start=0, row_end=5, cols=8)
            
        # Add ellipsis           
        text += '...\n'*3
            
        # Add last five rows of data
        text += trim_row(row_start=-5, row_end=-1, cols=8)
            
        return text
    
    def _repr_html_(self, id_num=None):
        '''
        Pretty printing for Jupyter notebooks
        
        Arguments:
         * id_num:  Number of Table in a sequence
        '''
        
        row_data = ''
        
        # Print only first 100 rows
        for row in self[0: min(len(self), 100)]:
            row_data += '<tr><td>{0}</td></tr>'.format(
                '</td><td>'.join([str(i) for i in row]))
        
        if id_num is not None:
            title = '<h2>[{0}] {1}</h2>'.format(id_num, self.name)
        else:
            title = '<h2>{}</h2>'.format(self.name)
        
        html_str = title + '''
        <table>
            <tr><th>{col_names}</th></tr>
            {row_data}
        </table>'''.format(
            col_names = '</th><th>'.join([col_name + '<br />' + self.col_types[i] for i, col_name in enumerate(self.col_names)]),
            row_data = row_data
        )
        
        return html_str
    
    def __getitem__(self, key):
        # TO DO: Make slice operator return a Table object not a list
        # Also make sure all attributes are passed down

        if isinstance(key, slice):
            return Table(
                name=self.name,
                col_names=self.col_names,
                col_types=self.col_types,
                row_values=super(Table, self).__getitem__(key))
        else:
            return super(Table, self).__getitem__(key)
            
    def __setitem__(self, key, value):
        return super(Table, self).__setitem__(key, value)
    
    def __setattr__(self, attr, value):
        ''' Attribute Modification Safety Checks
        
        1. Primary Key Change
           - If attribute being modified is the primary key, update column
             types as well
        2. Column Name Insertion
           - Always make sure column names are SQL safe        
        '''
        
        if attr == 'p_key':
            try:
                # Remove 'PRIMARY KEY' from previous primary key
                self.col_types[self.p_key] = \
                    self.col_types[self.p_key].replace(' PRIMARY KEY', '')
                
                # Change p_key
                super(Table, self).__setattr__(attr, value)
                self.col_types[self.p_key] += ' PRIMARY KEY'
                
            # Either p_key not defined (AttrError) or is None (TypeError)
            except (AttributeError, TypeError):
                super(Table, self).__setattr__(attr, value)
                
        elif attr == 'col_names':
            super(Table, self).__setattr__(
                attr, [strip(name) for name in value])
                
        # Some other attribute being changed
        else:
            super(Table, self).__setattr__(attr, value)
            
    ''' Table manipulation functions '''
    
    def _remove_empty(self):
        ''' Remove all empty rows '''
        
        # Need something in LIFO order
        remove = deque()
        
        for i, row in enumerate(self):
            non_empty = sum([bool(j or j == 0) for j in row])
            
            if not non_empty:
                remove.append(i)
            
        while remove:
            del self[remove.pop()]
    
    # TODO: Make this a decorator
    def _parse_col(self, col):
        '''
        Helper function: Find out what column is being referred to
        
        Arguments:
         * col (string):    Delete column named col
         * col (integer):   Delete column at position col
        '''
        
        if isinstance(col, int):
            return col
        elif isinstance(col, str):
            try:
                return self.col_names.index(col)
            except ValueError:
                raise ValueError("Couldn't find a column named {0}.".format(col))
        else:
            raise ValueError('Please specify either an index (integer) or column name (string) for col.')
                
    def delete(self, col):
        '''
        Delete a column
        
        Arguments:
         * col (string):    Delete column named col
         * col (integer):   Delete column at position col
         
        '''
        
        index = self._parse_col(col)
        
        del self.col_names[index]
        del self.col_types[index]
        
        for row in self:
            del row[index]
            
    def apply(self, col, func, i=False):
        ''' Apply a function to all entries in a column
        
        Arguments:
         * col:     Index or name of column
         * func:    Function to be applied
         * i:       Should func receive row index as argument        
        '''
        
        index = self._parse_col(col)
        
        for row_index, row in enumerate(self):
            arguments = OrderedDict()
            
            if i:
                arguments['i'] = row_index
        
            row[index] = func(row[index], **arguments)
            
    def aggregate(self, col, func=lambda x: x):
        '''
        Apply an aggregate function a column
        
        Arguments:
         * col:     Index or name of column
         * func:    Function to be applied
        '''
        
        index = self._parse_col(col)

        return func([self[row][index] for row in self])
        
    def mutate(self, col, func, *args):
        '''
        Create a new column from existing ones
        
        Arguments:
         * col:     Name of new column     
         * func:    Function to apply
         * Arguments should be any combination of indices and column names
        '''
            
        source_indices = [self._parse_col(i) for i in args]

        try:
            col_index = self.col_names.index(col)
        except ValueError:
            col_index = None
            
        if col_index:
            raise ValueError('{} already exists. Use apply() to transform existing columns.'.format(col))
        else:
            self.col_names.append(col)
            self.col_types.append('TEXT')
        
        for row in self:
            row.append(func(*[row[i] for i in source_indices]))
        
    def reorder(self, *args):
        '''
        Return a new reordered Table

        Time Complexity:
         * Reference: https://wiki.python.org/moin/TimeComplexity
         * Let m = width of new table, n = number of rows
         * List Comp: [row[i] for i in org_indices]
           * List access is O(1), so this list comp is O(2m) ~ O(m)
             where m is width of new table
         * For Loop: Executes inner listcomp for every row
         * So reorder() is O(mn)
        '''
        
        orig_indices = [self._parse_col(i) for i in args]
        
        # TO DO: Change col names and col_types
        new_table = Table(
            name = self.name,
            col_names = [self.col_names[i] for i in orig_indices],
            col_types = [self.col_types[i] for i in orig_indices])
        
        for row in self:
            new_table.append([row[i] for i in orig_indices])
        
        return new_table
        
# Try to guess what data type a given string actually is
def _guess_data_type(item):
    if item is None:
        return 'INTEGER'
    elif isinstance(item, int):
        return 'INTEGER'
    elif isinstance(item, float):
        return 'REAL'
    else:
        # Strings and other types
        if item.isnumeric():
            return 'INTEGER'
        elif (not item.isnumeric()) and (item.replace('.', '', 1).isnumeric()):
            '''
            Explanation:
             * A floating point number, e.g. '3.14', in string will not be 
               recognized as being a number by Python via .isnumeric()
             * However, after removing the '.', it should be
            '''
            return 'REAL'
        else:
            return 'TEXT'