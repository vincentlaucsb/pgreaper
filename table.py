from collections import Counter

# Python representation of a table
class Table(list):
    '''
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
    
    def __init__(self, name, col_names, col_types=None, p_key=None, *args, **kwargs):
        self.name = name
        self.col_names = col_names
        self.p_key = p_key
        
        # Set column names and row values        
        if 'col_values' in kwargs:
            # Convert columns to rows
            n_rows = range(0, len(kwargs['col_values'][0]))
            row_values = [[col[row] for col in kwargs['col_values']] for row in n_rows]
        elif 'row_values' in kwargs:
            row_values = kwargs['row_values']
        else:
            raise TypeError('Please specify the table data using either col_values or row_values.')
            
        super(Table, self).__init__(row_values)

        # Set column types
        # Note: User input not completely validated, e.g. whether the data 
        # type is an actual sqlite data type is not checked
        if col_types:
            if isinstance(col_types, list) or isinstance(col_types, tuple):
                if len(col_types) == len(self.col_names):
                    self.col_types = col_types
                else:
                    raise ValueError('Table has {0} columns but only {1} column types are specified.'.format(len(self.col_names), len(col_types)))
            elif isinstance(col_types, str):
                self.col_types = [col_types for col in self.col_names]
                
        # No column types specified --> guess them
        else:
            self.col_types = self.guess_type()
            
        # Set primary key
        if p_key is not None:
            self.col_types[p_key] += ' PRIMARY KEY'
        
    def guess_type(self):
        '''
        Guesses column data type by trying to accomodate all data, i.e.:
         * If a column has TEXT, INTEGER, and REAL, the column type is TEXT
         * If a column has INTEGER and REAL, the column type is REAL
         * If a column has REAL, the column type is REAL
        '''
        
        # Get first and last ten rows
        rows = [row for row in self[0: 10] + self[-10:-1]]
        
        data_types_by_col = [list() for col in self.col_names]
        
        # Get data types by column
        for row in rows:
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
    
        def trim(string, length=10):
            ''' Trim string to specified length '''
            if len(string) > length:
                return string[0: length - 3] + "..."
            else:
                return string
            
        ''' Only print out first 5 and last 5 rows '''
        text = "".join(['| {:^10} '.format(trim(name)) for name in self.col_names])
        text += "\n"
        
        # Add column types
        text += "".join(['| {:^10} '.format(trim(ctype)) for ctype in self.col_types])
        text += "\n"
        
        text += '-'*len(text)
        text += "\n"
        
        # Add first first rows of data
        for row in self[0: 5]:
            text += "".join(['| {:^10} '.format(trim(item)) for item in row])
            text += "\n"
            
        # Add ellipsis           
        text += '...\n'*3
            
        # Add last five rows of data
        for row in self[-5: -1]:
            text += "".join(['| {:^10} '.format(trim(item)) for item in row])
            text += "\n"
            
        return text
    
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
            
    def __setitem__(self, key, value):
        print(key, value)
        
        return super(Table, self).__setitem__(key, value)
            
    def get_col(self, key):
        ''' Get the values of a column given an index '''
        return [row[key] for row in self]
        
class Column(list):
    '''
    Goal: 
     * Act as a table column
     * Allow users to reassign values using column names as indices
     
    Arguments:
     * column_index: The index of the column in the original Table
     * table:        The original containing Table
     
    Not intended to be created by end users directly
    '''
    
    def __init__(self, args, index, table):
        super(Column, self).__init__([*args])
        self.column_index = index
        self.parent = table
    
    def __setitem__(self, key, value):
        self.parent[key][self.column_index] = value
        
    def apply(self, func):
        '''
        Apply a function to every entry in this column.
        * func: A reference to a function
        
        Example:
        >>> def strip_ws(data):
        >>>    return data.strip(' ', '')
        >>>
        >>> tbl['col1'].apply(strip_ws)
        '''
        for i in range(0, len(self) - 1):
            self[i] = func(self[i])
        
# Take a subset of a Table       
def subset(obj, *args, name=''):
    '''
    Arguments:
     * name:      Name of the returned table
     * Indices of columns to take
      * Valid arguments: integers, tuples (start, stop), list of integers and/or tuples
     * Names of the columns to take
    '''
    
    indices = []
    
    for cols in args:
        if isinstance(cols, int):
            indices.append(cols)
                
        elif isinstance(cols, tuple):
            indices += list(range(cols[0], cols[1] + 1))
            
        elif isinstance(cols, str):
            indices.append(obj.col_names.index(cols))
            
        else:
            raise ValueError("Column indices must either be integers, tuples of integers, or column names.")
        
    return subset_by_indices(obj, indices, name=name)
        
# Return table subset by column indices
def subset_by_indices(obj, indices, name=''):
    '''
     * obj:      Table object
     * indicies: List of column indices to grab
    '''
    
    # If original Table had a PRIMARY KEY column, copy it over as well
    if obj.p_key and (obj.p_key not in indices):
    
        # Make PRIMARY KEY first column
        indices.insert(0, obj.p_key)
        p_key = 0
    
    else:
        p_key = obj.p_key
        
    col_names = [obj.col_names[index] for index in indices]
    new_rows = [[row[index] for index in indices] for row in obj]
    
    return Table(name, col_names=col_names, row_values=new_rows,
                 p_key=p_key)
    
# Try to guess what data type a given string actually is
def _guess_data_type(item):
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