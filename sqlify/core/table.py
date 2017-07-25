from ._core import strip, convert_schema
from ._guess_dtype import PYTYPES, guess_data_type, compatible

import csv
import json
import os

from collections import Counter, defaultdict, deque, OrderedDict, Iterable
import collections
import copy
import functools
import re
import warnings

def _default_file(file_ext):
    ''' Provide a default filename given a Table object '''
    
    def decorator(func):
        @functools.wraps(func)
        def inner(obj, file=None, *args, **kwargs):
            if not file:
                file = obj.name.lower()
            
                # Strip out bad characters
                for char in ['\\', '/', ':', '*', '?', '"', '<', '>', '|', 
                '\t']:
                    file = file.replace(char, '')
                    
                # Remove trailing and leading whitespace in name
                file = re.sub('^(?=) *|(?=) *$', repl='', string=file)
                    
                # Replace other whitespace with underscore
                file = file.replace(' ', '_')
                    
                # Add file extension
                file += file_ext
                
            return func(obj, file, *args, **kwargs)
        return inner
        
    return decorator

def _create_dir(func):
    ''' Creates directory if it doesn't exist. Also modifies file argument 
        to include folder. '''

    @functools.wraps(func)
    def inner(obj, file, dir, *args, **kwargs):
        if dir:
            file = os.path.join(dir, file)
            os.makedirs(dir, exist_ok=True)
        return func(obj, file, dir, *args, **kwargs)
        
    return inner
    
class Table(list):
    '''
    Two-dimensional data structure reprsenting a CSV or SQL table
     * Subclass of list
     * Each entry is also a list--representing a row, so a `Table` is a list of lists
    
    ====================  ===================================  ===========================================================
    Attributes
    ----------------------------------------------------------------------------------------------------------------------
    Attribute             Description                          Modifiable [1]_
    ====================  ===================================  ===========================================================
    name                  Name of the column (string)          Yes
    n_cols                Number of columns                    Yes but should be equal to length of col_names
    col_names             Names of columns (list of strings)   Yes but should be as long as all rows in Table
    col_types             Data type of columns                 Yes but generally not necessary (also see: `col_names`)
    p_key                 Index of primary key column          Yes
    ====================  ===================================  ===========================================================
    
    .. [1] No one can really stop you from modifying class attributes that 
       you shouldn't be, but do so at your own peril :-)
   
    .. note:: All Table manipulation actions modify a Table in place unless otherwise specified
    '''
    
    # Define attributes to save memory
    __slots__ = ['name', 'n_cols', 'col_names', 'col_types', 'p_key',
        'pytypes', 'guess_func', 'compat_func']
        
    # Attributes that should be copied when creating identical tables
    copy_attr_ = ['name', 'col_names', 'col_types', 'p_key']
    
    def __init__(self, name, col_names=None, col_types=None, p_key=None,
        pytypes=PYTYPES, guess_func=guess_data_type, compat_func=compatible,
        *args, **kwargs):
        '''
        Arguments:
        
         * name:       Name of the table (Required)
         * col_names:  A list specifying names of columns (Required)
          * col_types:  A list specifying the column types
          * row_values: A list of rows (i.e. a list of lists)
          * col_values (can be used instead of row_values): A list of column values
         * p_key:      Index of column used as a primary key
         * pytypes:    Mapping of Python types to SQL types
         * guess_func: Function used for guessing data types
         * compat_func:Function which determines if two types are compatible
         
        '''
        self.name = name
        self.col_names = list(col_names)
        self.n_cols = len(self.col_names)
        self.pytypes = pytypes
        self.guess_func = guess_func
        self.compat_func = compat_func
        
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
               
    def guess_type(self, sample_n=2000):
        '''
        Guesses column data type by trying to accomodate all data, i.e.:
         * If a column has TEXT, INTEGER, and REAL, the column type is TEXT
         * If a column has INTEGER and REAL, the column type is REAL
         * If a column has REAL, the column type is REAL
         
        Arguments:
         * sample_n:    Sample size of first n rows
        '''
        
        # Counter of data types per column
        data_types = [defaultdict(int) for col in self.col_names]
        check_these_cols = set([i for i in range(0, self.n_cols)])
        
        sample_n = min(len(self), sample_n)
        
        for i, row in enumerate(self):
            # Every 100 rows, check if TEXT is there already
            if i%100 == 0:
                remove = [j for j in check_these_cols if data_types[j]['TEXT']]
                
                for j in remove:
                    check_these_cols.remove(j)
            
            if i > sample_n:
                break
        
            # Each row only has one column
            if not isinstance(row, collections.Iterable):
                row = [row]

            # Loop over individual items
            for j in check_these_cols:
                data_types[j][guess_data_type(row[j])] += 1
        
        # Get most common type
        # col_types = [max(data_dict, key=data_dict.get) for data_dict in data_types]
        
        col_types = []
        
        str_type = self.pytypes['str']
        float_type = self.pytypes['float']
        int_type = self.pytypes['int']
        
        for col in data_types:
            if col[str_type]:
                this_col_type = str_type
            elif col[float_type]:
                this_col_type = float_type
            else:
                this_col_type = int_type
            
            col_types.append(this_col_type)
            
        return col_types
    
    def find_reject(self, col_types=None):
        ''' Returns a list of row indices where the rows conflict with the 
        established schema
        
        TODO: Rewrite this function in C++ since it has significant overhead        
        '''
        
        if not col_types:
            col_types = self.col_types
            
        check_these = [i for i, col in enumerate(self.col_types) if col != "TEXT"]
            
        rejects = []
            
        # Only worry about numeric columns
        for i, row in enumerate(self):
            for j in check_these:
                if not self.compat_func(self.guess_func(row[j]), col_types[j]):
                    rejects.append(i)
                    break
                    
        return rejects
    
    # def __eq__(self, other):
        # ''' Return True if other item is an iterable with the same contents '''
        
        # # Same row length
        # if len(self) != len(other):
            # return False
        
        # if not isinstance(other, Iterable):
            # return False            
        
        # for i, row in enumerate(self):
            # for j, col in enumerate(row):
                # try:
                    # if not (other[i][j] == col):
                        # return False
                # except IndexError:
                    # return False
                    
        # return True
            
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
        <style type="text/css">
            table.sqlify-table {
                border: 1px solid #555555;
            }
        
            table.sqlify-table * {
                border-style: none;
            }
            
            tr:nth-child(2n) {
                background: #eeeeee;
            }

            th {
                background: #555555;
                color: #ffffff;
            }
        </style> ''' + '''
        
        <table class="sqlify-table">
            <thead>
                <tr><th>{col_names}</th></tr>
                <tr><th>{col_types}</th></tr>
            </thead>
            <tbody>
                {row_data}
            </tbody>
        </table>'''.format(
            col_names = '</th><th>'.join([i for i in self.col_names]),
            col_types = '</th><th>'.join([i for i in self.col_types]),
            row_data = row_data
        )
        
        return html_str
    
    def __getitem__(self, key):
        # Make slice operator return a Table object not a list
        if isinstance(key, slice):
            return Table(
                name=self.name,
                col_names=self.col_names,
                col_types=self.col_types,
                row_values=super(Table, self).__getitem__(key))
        else:
            return super(Table, self).__getitem__(key)
    
    def __setattr__(self, attr, value):
        ''' Attribute Modification Safety Checks
        
        1. Primary Key Change
           - If attribute being modified is the primary key, update column
             types as well 
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
        else:
            super(Table, self).__setattr__(attr, value)
            
    ''' Table merging functions '''
    def widen(self, w, placeholder='', in_place=True):
        '''
        Widen table until it is of width w
         * Fills in new columns with placeholder
         * in_place:    Widen in place if True, else return copy
        '''
        
        add_this_much = w - self.n_cols
        self.n_cols = w
        
        if in_place:
            for row in self:
                row += [placeholder]*add_this_much
        else:
            new_table = self.copy()
            new_table.widen(w)
            
            return new_table
            
    def copy_attr(self, row_values=None):
        ''' Returns a new Table with just the same attributes '''
        
        kwargs = {}
        
        for attr in Table.copy_attr_:
            kwargs[attr] = getattr(self, attr)
        
        #return Table(row_values=row_values, **kwargs)
        return type(self)(row_values=row_values, **kwargs)
    
    def __add__(self, other):
        ''' 
        For Tables:
            Merge two tables vertically (returns new Table)
             * Column names are from first table
             * Less wide tables auto-filled with placeholders
             
        For Others:
            * Call parent method       
        '''
        
        if isinstance(other, Table):
            widen_this_much = max(self.n_cols, other.n_cols)
            
            if self.n_cols > other.n_cols:
                other.widen(widen_this_much, in_place=False)
            elif other.n_cols < self.n_cols:
                self.widen(widen_this_much, in_place=False)
            
            return self.copy_attr(row_values =
                super(Table, self).__add__(other))
        else:
            return super(Table, self).__add__(other)
    
    def rbind(self, other):
        ''' Alias for + '''
        return self.__add__(other)
        
    def union(self, *args, **kwargs):
        ''' Alias for + '''
        return self.__add__(other)
            
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
         * Perform a case-insensitive search
        
        Arguments:
         * col (string):    Delete column named col
         * col (integer):   Delete column at position col
        '''
        
        if isinstance(col, int):
            return col
        elif isinstance(col, str):
            col = col.lower()
            col_names = [i.lower() for i in self.col_names]
        
            try:
                return col_names.index(col)
            except ValueError:
                raise ValueError("Couldn't find a column named {0}.".format(col))
        else:
            raise ValueError('Please specify either an index (integer) or column name (string) for col.')
                
    def delete(self, col):
        '''
        Delete a column
        
        ====================  ===============================
        Argument              Description
        ====================  ===============================
        col (string)          Delete column named col
        col (integer)         Delete column at position col
        ====================  ===============================
        
        '''
        
        index = self._parse_col(col)
        
        del self.col_names[index]
        del self.col_types[index]
        
        for row in self:
            del row[index]
            
    def as_header(self, i=0):
        '''
        Replace the current set of column names with the data from the 
        ith column. Defaults to first row.
        '''
        
        self.col_names = copy.copy(self[i])
        del self[i]
            
    def apply(self, col, func, i=False, index_error='drop'):
        '''
        Apply a function to all entries in a column, i.e. modifes the values in a 
        column based on the return value of func.
         * `func` will always receive an individual entry as a first argument
         * If `i=True`, then `func` receives `i=<some row number>` as the second argument

        ====================  =======================================================
        Argument              Description
        ====================  =======================================================
        col                   Index or name of column (int or string)
        func                  Function to be applied (function or lambda)
        i                     Should func receive row index as argument (boolean)        
        index_error           Action to take on rows which occur an index error
        ====================  =======================================================

        ''' 
        
        index = self._parse_col(col)
        drop_rows = []
        
        for row_index, row in enumerate(self):
            arguments = OrderedDict()
            
            if i:
                arguments['i'] = row_index
            
            try:
                row[index] = func(row[index], **arguments)
                
            except IndexError:
                if index_error == 'drop':
                    drop_rows.append(row_index)
                else:
                    pass
                    
        # Drop erroneous rows
        while drop_rows:
            malformed_index = drop_rows.pop()
            
            warnings.warn('Dropping malformed row. {}'.format(
                self[malformed_index]))
            del self[malformed_index]
            
    def aggregate(self, col, func=lambda x: x):
        '''
        Apply an aggregate function a column
         * func should expect a list of column values as the only argument
         * If func is not specified, this returns a list of column values
        
        ======================  =======================================================
        Argument                Description
        ======================  =======================================================
        col                     Index or name of column
        func                    Function or lambda to be applied
        ======================  =======================================================
        '''

        index = self._parse_col(col)

        return func([self[row][index] for row in self])
    
    def label(self, col, label, col_type="TEXT"):
        '''
        Add a label to the dataset
         * Creates a column containing the same value for every record in the Table
         * Useful when combining several datasets
         
        Arguments:
         * col:         Name of column
         * label:       Value to be inserted
         * col_type:    Data type of label
        '''
        
        self.col_names.append(col)
        self.col_types.append(col_type)
        
        for row in self:
            row.append(label)
    
    def mutate(self, col, func, *args):
        '''
        Similar to `apply()`, but creates a new column--instead of modifying 
        a current one--based on the values of other columns.
        
        ======================  =======================================================
        Argument                Description
        ======================  =======================================================
        col                     Name of new column (string)
        func                    Function or lambda to apply
        (additional arguments)  Names of indices of columns that func needs
        ======================  =======================================================
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
        Return a **new** Table in the specified order (instead of modifying in place)
         * Arguments should be names or indices of columns
         * Can be used to take a subset of the current Table
         * Method runs in O(mn) time where m = number of columns in new Table
           and n is number of rows
        '''

        '''
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
    
@_default_file(file_ext='.csv')
@_create_dir
def table_to_csv(obj, file=None, dir=None, header=True, delimiter=','):
    '''
    Convert a Table object to CSV
    
    Arguments:
     * obj:     Table object to be converted
     * file:    Name of the file (default: Table name)
     * header:  Include the column names

    '''
     
    with open(file, mode='w', newline='\n') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"')
        
        if header:
            csv_writer.writerow(obj.col_names)
        
        for row in obj:
            csv_writer.writerow(row)
            
@_default_file(file_ext='.json')
@_create_dir
def table_to_json(obj, file=None, dir=None):
    '''
    TODO: Write unit test for this
    
    Arguments:
     * obj:     Table object to be converted
     * file:    Name of the file (default: Table name)
     * dir:     Directory to save to (default: None --> Current dir)

    Convert a Table object to JSON according to this specification

    +---------------------------------+--------------------------------+
    | Original Table                  | JSON Output                    |
    +=================================+================================+
    |                                 |                                |
    |                                 | .. code-block:: python         |
    |                                 |                                |
    | +---------+---------+--------+  |    [{'col1': 'Wilson',         |
    | | col1    | col2    | col3   |  |      'col2': 'Sherman',        |
    | +=========+=========+========+  |      'col3': 'Lynch'           |
    | | Wilson  | Sherman | Lynch  |  |     },                         |
    | +---------+---------+--------+  |     {'col1': 'Brady',          |
    | | Brady   | Butler  | Edelman|  |      'col2': 'Butler',         |
    | +---------+---------+--------+  |      'col3': 'Edelman'         |
    |                                 |     }]                         |
    +---------------------------------+--------------------------------+

    '''

    new_json = []
    
    for row in obj:
        json_row = {}
        
        for i, item in enumerate(row):
            json_row[obj.col_names[i]] = item
            
        new_json.append(json_row)
        
    with open(file, mode='w') as outfile:
        outfile.write(json.dumps(new_json))