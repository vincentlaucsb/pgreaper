from sqlify._sqlify import strip

import collections
import warnings
from collections import Counter
    
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
            if len(str(string)) > length:
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
    
    def _repr_html_(self):
        ''' Pretty printing for Jupyter notebooks '''
        
        row_data = ''
        
        # Print only first 100 rows
        for row in self[0: min(len(self), 100)]:
            row_data += '<tr><td>{0}</td></tr>'.format(
                '</td><td>'.join(row))
        
        html_str = '''
        <h2>{tbl_name}</h2>
        <table>
            <tr><th>{col_names}</th></tr>
            {row_data}
        </table>'''.format(
            tbl_name = self.name,
            col_names = '</th><th>'.join([col_name + '<br />' + self.col_types[i] for i, col_name in enumerate(self.col_names)]),
            row_data = row_data
        )
        
        return html_str
    
    def __getitem__(self, key):
        ''' Get the values of a column by specifying the column name as a key '''
        try:
            if isinstance(key, str):
                column_index = self.col_names.index(key)
                return Column([row[column_index] for row in self],
                              index=column_index, table=self)
            else:  # Don't overload default list indexing/slicing
                return super(Table, self).__getitem__(key)                
                
        except ValueError:
            raise KeyError("'{0}' is not a column name".format(key))
            
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