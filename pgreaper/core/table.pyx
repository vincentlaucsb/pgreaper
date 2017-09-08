'''
.. currentmodule:: pgreaper.core.table

Table
======
A general purpose two-dimensional data structure 

Structure of Type Counter
---------------------------
All tables (with some exceptions) have a type counter which records the number of different data
types in each column. The type counter is stored as the `_type_cnt` attribute. The exception to this rule are strongly-typed tables, which
don't need a type-counter because their types are fixed.

Example
~~~~~~~~
Suppose 'apples' and 'oranges' are column names

>>> { 'apples': {
>>>     'str': <Number of strings>,
>>>     'datetime': <Number of datetime objects>
>>> }, {
>>> 'oranges': {
>>>     'int': <Number of ints>,
>>>     'float': <Number of floats>
>>> }}

Maintaining the Type Counter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Operations which add or modify data are responsible for updating the type counter.
For example, all `Table` objects have a modified `append()` method which 
updates the type counter every time a new row is inserted.

.. automethod:: Table.append
'''

from pgreaper._globals import SQLIFY_PATH, PG_KEYWORDS
from ._base_table import BaseTable
from ._table import *
from .column_list import ColumnList
from .schema import SQLType

from collections import OrderedDict, defaultdict, deque, Iterable
from inspect import signature
import re
import copy
import types
import functools
import warnings

def assert_table(func=None, dialect=None):
    '''
    Makes sure the 'table' argument is actually a Table
    
    Args:
        dialect: Subclass of DialectPostgres
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
   
def update_type_count(func):
    ''' Brute force approach to updating a Table's type counter '''
    # THIS IS A TEMPORARY STOPGAP until I implement more efficient methods

    @functools.wraps(func)
    def inner(table, *args, **kwargs):
        # Run the function first
        ret = func(table, *args, **kwargs)
        
        try:
            # Re-build counter
            table._type_cnt.clear()
            for col in table.col_names:
                for i in table[col]:
                    table._type_cnt[col][type(i)] += 1
                    
            # Call guess_type() (it's cheap)
            table.guess_type()
            
        # Table doesn't have a type counter
        except AttributeError:
            pass
                
        return ret
    return inner
           
class Table(BaseTable):
    '''
    .. note:: All Table manipulation actions modify a Table in place unless otherwise specified
    
    Attributes:
        name:       str
                    Name of the Table
        col_names:  list
                    List of column names
        col_types:  list
                    List of column types, always lowercase
        p_key:      int or tuple[int]
                    Index or indicies of the primary key(s)
        rejects:    list
                    If strong_type = True, then this is a list of rows which
                    didn't fit the Table schema
        _type_cnt:  defaultdict
                    Mappings of column names to counters of data types for that column
    '''
    
    # Define attributes to save memory
    __slots__ = ['name', 'columns', 'rejects', 'null_col', 
        '_dialect', '_pk_idx', '_type_cnt']
        
    def __init__(self, name, dialect='postgres', columns=None, col_names=[],
        p_key=None, null_col=str, *args, **kwargs):
        '''
        Args:
            name:       str
                        Name of the Table
            dialect:    str (default: 'postgres')
                        SQL dialect ('sqlite' or 'postgres')
            col_names:   list
                        A list specifying names of columns (Either this or columns required)
            row_values: list
                        A list of rows (i.e. a list of lists)
            col_values: list
                        A list of column values
            p_key:      int
                        Index of column used as a primary key
            null_col:   type (default: str)
                        The data type of columns consisting entirely of NULL
        '''
        
        self.dialect = dialect
        self.null_col = null_col
        
        # Build content
        if 'col_values' in kwargs:
            # Convert columns to rows
            n_rows = range(0, len(kwargs['col_values'][0]))
            row_values = [[col[row] for col in kwargs['col_values']] for row in n_rows]
        elif 'row_values' in kwargs:
            row_values = kwargs['row_values']
        else:
            row_values = []
            
        # Set up column information
        if columns:
            self.columns = columns
            columns.table = self
        else:
            self.columns = ColumnList(col_names=col_names, p_key=p_key, table=self)
            
        self._pk_idx = {}
        
        # Add methods dynamically
        self.add_dicts = types.MethodType(add_dicts, self)
        self.guess_type = types.MethodType(guess_type, self)
        
        super(Table, self).__init__(name=name, row_values=row_values)
        
        # Build a type counter
        self._type_cnt = defaultdict(lambda: defaultdict(int))
        self._update_type_count()
    
    def _create_pk_index(self):
        ''' Create an index for the primary key column '''
        # Only non-composite indices are supported right now
        if isinstance(self.p_key, int):
            self._pk_idx = {row[self.p_key]: row for row in self}

    def _update_type_count(self):
        ''' Brute force method for updating type count '''
        self._type_cnt.clear()
        for col in self.col_names:
            for i in self[col]:
                self._type_cnt[col][type(i)] += 1
            
    @property
    def col_names(self):
        return self.columns.col_names
        
    @col_names.setter
    def col_names(self, value):
        rename = {x: y for x, y in zip(self.col_names, value)}
        self.columns.col_names = value
        
        # Re-build counter
        for old_name, new_name in zip(rename.keys(), rename.values()):
            self._type_cnt[new_name] = self._type_cnt[old_name]
            del self._type_cnt[old_name]
            
    @property
    def col_names_sanitized(self):
        if self.dialect == 'postgres':
            return self.columns.sanitize(PG_KEYWORDS)
        else:
            return self.columns.sanitize()
        
    @property
    def col_types(self):
        return self.columns.col_types
        
    @col_types.setter
    def col_types(self, value):
        self.columns.col_types = value
        
    @property
    def n_cols(self):
        return self.columns.n_cols
        
    @property
    def p_key(self):
        return self.columns.p_key
        
    @p_key.setter
    def p_key(self, value):
        self.columns.p_key = value
        self._create_pk_index()
        
    @property
    def dialect(self):
        return self._dialect
        
    @dialect.setter
    def dialect(self, value):
        if value in ['sqlite', 'postgres']:
            self._dialect = value
        else:
            raise ValueError("'dialect' must either 'sqlite' or 'postgres'")

    @staticmethod
    def copy_attr(table_, row_values=[]):
        ''' Returns a new Table with just the same attributes '''
        return Table(name=table_.name, dialect=table_.dialect,
            columns=table_.columns, row_values=row_values)
    
    def __getitem__(self, key):
        '''
        In addition to the standard Python slice syntax for lists,
        Table objects also support...
        
        Slicing:
            by column:
                >>> Table[column_name]
            by primary key (only supported for single primary keys):
                >>> Table[primary key, ]
            by primary key + column:
                >>> Table[primary key, column name]
        '''
    
        if isinstance(key, slice):
            # Make slice operator return a Table object not a list
            return self.copy_attr(self,
                row_values=super(Table, self).__getitem__(key))
        elif isinstance(key, tuple):
            # Support indexing by primary key
            if len(key) == 1:
                return self._pk_idx[key[0]]
            else:
                if isinstance(key[1], str):
                    return self._pk_idx[key[0]][self.columns.index(key[1])]
                else:
                    return self._pk_idx[key[0]][key[1]]
        elif isinstance(key, str):
            # Support indexing by column name
            return [row[self.columns.index(key)] for row in self]
        else:
            return super(Table, self).__getitem__(key)
    
    def append(self, value):
        '''
        Don't append rows with the wrong length and update 
        type-counter
        '''
        
        cdef int n_cols = self.n_cols
        cdef int value_len = len(value)
        cdef int i
        
        if n_cols != value_len:
            ''' Future: Add a warning before dropping '''
            print('Dropping {} due to width mismatch'.format(value))
        else:
            # Add to type counter
            for i, j in enumerate(value):
                self._type_cnt[self.columns._idx[i]][type(j)] += 1
                
            super(Table, self).append(value)
    
    def to_string(self):
        ''' Return this table as a StringIO object for writing via copy() '''
        return to_string(self)
    
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
        '''
        drop_empty(self)
        Remove all empty rows
        
        Motivation:
            This feature was motivated by my experience parsing HTML
            tables, where a lot of rows were empty because they were 
            used as spacing (a huge antipattern, but popular nonetheless)
        '''
        remove = deque()  # Need something in LIFO order
        
        for i, row in enumerate(self):
            if not sum([bool(j or j == 0) for j in row]):
                remove.append(i)
            
        # Remove from bottom first
        while remove:
            del self[remove.pop()]
    
    @update_type_count
    def as_header(self, i=0):
        '''
        as_header(self, i=0)
        Replace the current set of column names with the data from the 
        ith column. Defaults to first row.
        '''
        
        self.col_names = copy.copy(self[i])
        del self[i]
    
    @update_type_count
    def delete(self, col):
        '''
        Delete a column
        
        Args:
            col:        str or int
                        Delete column named col or at position col
        '''
        
        index = self._parse_col(col)
        self.columns.del_col(index)
        
        for row in self:
            del row[index]
            
    @update_type_count
    def apply(self, *args, **kwargs):
        super(Table, self).apply(*args, **kwargs)

    @update_type_count
    def aggregate(self, col, func=None):
        super(Table, self).aggregate(col, func)
    
    def add_col(self, col, fill):
        '''
        add_col(self, col, fill)
        Add a new column to the Table with a placeholder value
        
        Args:
            col:        str
                        Name of new column
            fill:      
                        What to put in new column
        '''        
        self.columns.add_col(col)
        
        for row in self:
            row.append(fill)
            
        # Update type counter
        try:
            self._type_cnt[col][type(fill)] = len(self)
        except AttributeError:
            # No type counter
            pass

    def mutate(self, col, func, *args):
        '''
        Similar to `apply()`, but creates a new column--instead of modifying 
        a current one--based on the values of other columns.
        
        Args:
            col:            str
                            Name of new column (string)
            func:           function
                            Function or lambda to apply
            args:          str, int
                            Names of indices of columns that func needs
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
        
    def reorder(self, *args):
        '''
        reorder(self, *args)
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
            col_names = [self.col_names[i] for i in orig_indices])
            
        # TEMPORARY: Update p_key
        if self.p_key in orig_indices:
            new_table.p_key = orig_indices.index(self.p_key)
        
        for row in self:
            new_table.append([row[i] for i in orig_indices])
            
        new_table.guess_type()
        
        return new_table
        
    def subset(self, *cols):
        '''
        subset(self, *cols)
        Return a subset of the Table with the specified columns
        
        .. note:: This function is really just an alias for reorder()
        '''
        return self.reorder(*cols)
        
    def transpose(self, include_header=True):
        '''
        transpose(self, include_header=True)
        Return a new Table where the rows and columns have been swapped
        
        Args:
            include_header:     bool
                                Include the header in the transpose
        '''
        if include_header:
            row_values = [[col] + self[col] for col in self.col_names]
        else:
            row_values = [self[col] for col in self.col_names]
        
        return Table(
            name = self.name,
            row_values = row_values
        )
        
    def groupby(self, col):
        ''' 
        groupby(self, col)
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
        
    def apply(self, col, func, i=False):
        '''
        apply(self, col, func, i=False)
        Apply a function to all entries in a column
        
         * `func` will always receive an individual entry as a first argument
         * If `i=True`, then `func` receives `i=<some row number>` as the second argument

        Args:
            col:        int or str
                        Index or name of column (int or string)
            func:       function or lambda
                        Function to be applied
            i:          bool
                        Should func receive row index as argument (boolean)        
        ''' 
        
        index = self._parse_col(col)
        
        for row_index, row in enumerate(self):
            arguments = OrderedDict()
            
            if i:
                arguments['i'] = row_index
            
            row[index] = func(row[index], **arguments)
        
    def add_dict(self, dict, *args, **kwargs):
        ''' Add a single dict to to the Table '''
        self.add_dicts([dict], *args, **kwargs)