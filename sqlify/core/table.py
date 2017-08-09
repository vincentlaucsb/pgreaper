'''
Table
=======
A general two-dimensional data structure
'''

from sqlify._globals import SQLIFY_PATH
from ._base_table import BaseTable
from ._core import strip
from .column_list import ColumnList
from .schema import convert_schema, SQLDialect, DialectSQLite, DialectPostgres

from collections import Counter, defaultdict, deque, Iterable
from inspect import signature
import re
import copy
import functools
import warnings

def assert_table(func=None, dialect=None):
    '''
    Makes sure the 'table' argument is actually a Table
    
    Parameters
    -----------
    dialect:    Subclass of DialectPostgres
                Which dialect the Table should have    
    '''
    
    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            table_arg = signature(func).bind(*args, **kwargs).arguments['table']
            if not isinstance(table_arg, Table):
                raise TypeError('This function only works for Table objects.')
            else:
                if str(table_arg.dialect) != dialect:
                    table_arg.dialect = dialect # Should also convert schema
            return func(*args, **kwargs)
        return inner
        
    if func:
        return decorator(func)
    
    return decorator

def _check_malformed(func):
    '''
    Decorator for Table class methods which manipulate the table
     * Drops malformed rows (too short, too long) so index errors don't occur
     * Only does this once since this is a somewhat expensive operation
    '''
    
    @functools.wraps(func)
    def inner(table, *args, **kwargs):
        if not table._malformed_check:
            table._malformed_check = True
            table.drop_malformed()
            
        return func(table, *args, **kwargs)
    return inner

class Table(BaseTable):
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
    __slots__ = ['name', 'columns', '_dialect', '_malformed_check']
        
    # Attributes that should be copied when creating identical tables
    _copy_attr = ['name', 'columns', 'dialect']
    
    def __init__(self, name, dialect='sqlite', col_names=[], col_types=[],
        p_key=None, *args, **kwargs):
        '''
        Arguments:
         * name:        Name of the table (Required)
         * dialect:     A SQLDialect object
         * col_names:   A list specifying names of columns (Required)
         * col_types:   A list specifying the column types
         * row_values:  A list of rows (i.e. a list of lists)
         * col_values   (can be used instead of row_values): A list of lists (column values)
                        Should be of uniform length
         * p_key:       Index of column used as a primary key
        '''
        
        self.dialect = dialect
        self.columns = ColumnList(col_names, col_types, p_key=p_key)
        
        # Make sure to drop malformed rows before user modifies Table
        self._malformed_check = False
        
        if 'col_values' in kwargs:
            # Convert columns to rows
            n_rows = range(0, len(kwargs['col_values'][0]))
            row_values = [[col[row] for col in kwargs['col_values']] for row in n_rows]
        elif 'row_values' in kwargs:
            row_values = kwargs['row_values']
        else:
            row_values = []
        
        super(Table, self).__init__(name=name, row_values=row_values)
        
    @property
    def col_names(self):
        return self.columns.col_names
        
    @col_names.setter
    def col_names(self, value):
        self.columns.col_names = value
        
    @property
    def col_types(self):
        return self.columns.col_types
        
    @col_types.setter
    def col_types(self, value):
        self.columns.col_types = value
        
    @property
    def n_cols(self):
        return self.columns.n_cols()
        
    @property
    def p_key(self):
        return self.columns.p_key
        
    @p_key.setter
    def p_key(self, value):
        self.columns.p_key = value
        
    @property
    def dialect(self):
        return self._dialect
        
    @dialect.setter
    def dialect(self, value):
        if isinstance(value, SQLDialect):
            self._dialect = value
        elif value == 'sqlite':
            self._dialect = DialectSQLite()
        elif value == 'postgres':
            self._dialect = DialectPostgres()
        else:
            raise ValueError("'dialect' must either 'sqlite' or 'postgres'")
        
        # Convert column names when dialect changes
        try:
            self.columns.col_names = convert_schema(
                self.col_names,
                from_=str(self._dialect),
                to_=str(value))
        except AttributeError:
            pass
    
    @staticmethod
    def copy_attr(table_, row_values=[]):
        ''' Returns a new Table with just the same attributes '''
        new_table = Table(name=table_.name, row_values=row_values)
        for attr in Table._copy_attr:
            setattr(new_table, attr, getattr(table_, attr))
        return new_table
        
    def guess_type(self, sample_n=2000):
        '''
        Guesses column data type by trying to accomodate all data, i.e.:
         * If a column has TEXT, INTEGER, and REAL, the column type is TEXT
         * If a column has INTEGER and REAL, the column type is REAL
         * If a column has REAL, the column type is REAL
         
        Arguments:
         * sample_n:    Sample size of first n rows
        '''
        
        self.col_types = self.dialect.guess_type(self, sample_n)
    
    @_check_malformed
    def find_reject(self, col_types=None):
        '''
        Returns a list of row indices where the rows conflict with the
            established schema        
            
        TODO: Rewrite this function in C++ since it has significant overhead        
        '''
        
        if not col_types:
            col_types = self.col_types
            
        guess_data_type = self.dialect.guess_data_type
        compatible = self.dialect.compatible
        check_these = [i for i, col in enumerate(self.col_types) if col != "TEXT"]
        rejects = []
            
        # Only worry about numeric columns
        for i, row in enumerate(self):
            for j in check_these:
                if not compatible(guess_data_type(row[j]), col_types[j]):
                    rejects.append(i)
                    break
                    
        return rejects
    
    def __getitem__(self, key):
        if isinstance(key, slice):
            # Make slice operator return a Table object not a list
            return self.copy_attr(self,
                row_values=super(Table, self).__getitem__(key))
        elif isinstance(key, str):
            # Support indexing by column name
            return [row[self.columns.index(key)] for row in self]
        else:
            return super(Table, self).__getitem__(key)
    
    def to_string(self):
        ''' Return this table as a StringIO object for writing via copy() '''
        return self.dialect.to_string(self)
    
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
            
            return self.copy_attr(self, row_values =
                super(Table, self).__add__(other))
        else:
            return super(Table, self).__add__(other)
            
    ''' Table Manipulation Methods '''
    
    def drop_empty(self):
        ''' Remove all empty rows '''
        remove = deque()  # Need something in LIFO order
        
        for i, row in enumerate(self):
            if not sum([bool(j or j == 0) for j in row]):
                remove.append(i)
            
        while remove:
            del self[remove.pop()]
            
    def drop_malformed(self):
        ''' Drop rows which don't have the right length '''
        remove = deque()
        
        for i, row in enumerate(self):
            if len(row) != self.n_cols:
                remove.append(i)
                
        while remove:
            del self[remove.pop()]
    
    def as_header(self, i=0):
        '''
        Replace the current set of column names with the data from the 
        ith column. Defaults to first row.
        '''
        
        self.col_names = copy.copy(self[i])
        del self[i]
    
    @_check_malformed
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
        self.columns.del_col(index)
        
        for row in self:
            del row[index]
            
    @_check_malformed
    def apply(self, *args, **kwargs):
        super(Table, self).apply(*args, **kwargs)

    @_check_malformed
    def aggregate(self, col, func=None):
        super(Table, self).aggregate(col, func)
    
    @_check_malformed
    def add_col(self, col, fill, type='TEXT'):
        ''' Add a new column to the Table
        
        Parameters
        -----------
        col:        str
                    Name of new column
        fill:      
                    What to put in new column
        type:       str
                    Data type of new column        
        '''
        self.columns.add_col(col, type)
        
        for row in self:
            row.append(fill)

    @_check_malformed
    def label(self, col, label, type='TEXT'):
        ''' Add a label to the dataset '''        
        self.add_col(col, label, type)
    
    @_check_malformed
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
            self.columns.add_col(col)
        
        for row in self:
            row.append(func(*[row[i] for i in source_indices]))
        
    @_check_malformed
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
        
        new_table = Table(
            name = self.name,
            dialect = self.dialect,
            col_names = [self.col_names[i] for i in orig_indices],
            col_types = [self.col_types[i] for i in orig_indices])
            
        # TEMPORARY: Update p_key
        if self.p_key in orig_indices:
            new_table.p_key = orig_indices.index(self.p_key)
        
        for row in self:
            new_table.append([row[i] for i in orig_indices])
        
        return new_table
        
    def subset(self, *cols):
        '''
        Return a subset of the Table with the specified columns
         * Really just an alias for reorder()
        '''
        return self.reorder(*cols)
        
    @_check_malformed
    def transpose(self, include_header=True):
        '''
        Swap rows and columns
        
        Parameters
        -----------
        include_header:     bool
                            Treat header as a row in the operation
        '''
        if include_header:
            row_values = [[col] + self[col] for col in self.col_names]
        else:
            row_values = [self[col] for col in self.col_names]
        
        return Table(
            name = self.name,
            col_names = ['col_{}'.format(i) for i in range(0, len(self))],
            row_values = row_values
        )
        
    @_check_malformed
    def groupby(self, col):
        ''' 
        Return a dict of Tables where the keys are unique entries
        in col and values are all rows with where row[col] = that key
        '''
        
        col_index = self._parse_col(col)
        table_dict = defaultdict(lambda: self.copy_attr(self))
        
        # Group by
        for row in self:
            table_dict[row[col_index]].append(row)
            
        # Set table names equal to key
        for k in table_dict:
            table_dict[k].name = k
            
        return table_dict
        
    @_check_malformed
    def _add_dicts(self, dicts, filter=False, extract={}):
        '''
        Appends a list of dicts to the Table. Each dict is viewed as
        a mapping of column names to column values.
        
        Parameters
        -----------
        dicts:      list
                    A list of JSON dicts
        filter:     bool (Default: False)
                    Should Table add extra columns found in JSON
        extract:    If adding nested dicts, pull out nested entries 
                    according to extract dict
        '''
        
        if filter:
            raise NotImplementedError
        
        # Add necessary columns according to extract dict
        for col, path in zip(extract.keys(), extract.values()):
            if col.lower() not in self.columns:
                self.add_col(col, None)
        
        for row in dicts:
            new_row = []
            
            # Extract values according to extract dict
            for col, path in zip(extract.keys(), extract.values()):
                try:
                    value = row
                    for k in path:
                        value = value[k]
                    row[col] = value
                except (KeyError, IndexError) as e:
                    row[col] = None
        
            # Add necessary columns
            # Use list to preserve order
            for key in [i for i in row if i.lower() not in self.columns]:
                self.add_col(key, None)
                
            # Map column indices to keys and create the new row
            map = self.columns.map(*row)
            for i in range(0, self.n_cols):
                try:
                    new_row.append(row[map[i]])
                except KeyError:
                    new_row.append(None)
                    
            self.append(new_row)