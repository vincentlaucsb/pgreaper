from inspect import signature
import functools
import warnings

class ColumnList(object):
    '''
    A class which makes it easier to work with columns
     * Supports "mathematical" operations with columns
     * Does some validity checks
     
    Rules
     * Column names should be in whatever case they came in, but 
       comparisons should be case-insensitive
        * Returns a lowercased list of column names when iterated on
     * Column types should always be lower-cased
     
    Attributes
    -----------
    _idx:           dict
                    An auto-updating mapping of integer indices to column names
    _inverted_idx:  dict
                    An auto-updating mapping of column names to integer indices
    '''
    
    __slots__ = ['n_cols', '_col_names', '_col_types', '_p_key', '_idx', '_inverted_idx']
    
    def __init__(self, col_names=[], col_types=[], p_key=None):
        self.col_names = col_names
        self.col_types = col_types
        self.p_key = p_key
        self._update_idx()
        
    def add_col(self, name, type='text'):
        ''' The correct method for adding a column '''
        self._col_names.append(name)
        self._col_types.append(type)
        self._update_idx()
        
    def del_col(self, index):
        ''' Remove column at index '''
        del self._col_names[index]
        del self._col_types[index]
        self._update_idx()
        
    def as_tuples(self):
        ''' Return a list of (column name, column type) tuples '''
        return [(x, y) for x, y in zip(self.col_names, self.col_types)]
        
    def _update_idx(self):
        ''' Update self._idx and self._inverted_idx '''
        self.n_cols = len(self.col_names)
        self._idx = {i: j.lower() for i, j in enumerate(self.col_names)}
        self._inverted_idx = {j.lower(): i for i, j in enumerate(self.col_names)}
        
    def index(self, name):
        '''
        Return index of column name or raise a KeyError
         * Case insensitive
         
        Parameters
        -----------
        name:           str
                        Column name
        '''
        try:
            return self._inverted_idx[name.lower()]
        except KeyError:
            raise KeyError('There was no column named {} in {}'.format(
                name, self.col_names))
                
    def map(self, *cols):
        '''
        Given a list of keys return a map of indices to keys         
         * Should NOT lowercase keys, but performs a case-insensitive lookup
        '''
        
        map_ = {}

        for col in cols:
            try:
                i = self._inverted_idx[col.lower()]
                map_[i] = col
            except KeyError:
                pass
                
        return map_
        
    @property
    def col_names(self):
        return self._col_names
    
    @col_names.setter
    def col_names(self, value):
        if not value:
            self._col_names = []
        else:
            self._col_names = value
        
        self.n_cols = len(self._col_names)
        
    @property
    def col_types(self):
        return self._col_types
        
    @col_types.getter
    def col_types(self):
        ''' Tack on PRIMARY KEY label if appropriate '''
        
        col_types = [str(i) for i in self._col_types]
    
        if self.p_key:
            col_types[self.p_key] += ' primary key'
            return col_types
        else:
            return col_types
        
    @col_types.setter
    def col_types(self, value):
        if value is None or value == []:
            # No column types specified --> set to text
            value = ['text'] * self.n_cols
        elif isinstance(value, list) or isinstance(value, tuple):
            if len(value) != len(self.col_names):
                warnings.warn('''
                    Table has {0} columns but {1} column types are specified.
                    The shorter list will be filled with placeholder values.'''.format(
                        len(self.col_names), len(value)))
                
            if len(value) < len(self.col_names):
                while len(value) < len(self.col_names):
                    value.append('text')
            else:
                while len(self.col_names) < len(value):
                    self.col_names.append('col')
        elif isinstance(value, str):
            # If col_types is a single string, set each column's type to that string
            value = [value] * self.n_cols
        else:   
            raise ValueError('Column types should either be a list, tuple, or string.')
    
        self._col_types = value
            
    @property
    def p_key(self):
        return self._p_key
            
    @p_key.setter
    def p_key(self, value):
        '''
         * If integer, assert that column exists
         * If string (representing column name), set to integer index of col
        '''
        
        if value is None:
            self._p_key = None
        elif isinstance(value, int):
            # TODO: Make sure this doesn't cause an index error
            self._p_key = value
        elif isinstance(value, str):
            self._p_key = self.index(value)
        else:
            raise TypeError('Primary key must either be an integer index of column name.')
        
    def __iter__(self):
        return iter([i.lower() for i in self.col_names])
        
    def __bool__(self):
        return bool(len(self.col_names))
        
    def __gt__(self, other):
        '''
        Partial Ordering
         * Return "True" if column names are STRICT superset of other
        '''
        return set(self.col_names).issuperset(set(other.col_names)) and \
            set(self.col_names) != set(other.col_names)
            
    def __lt__(self, other):
        ''' Return "True" if column names are a STRICT subset of other '''
        return set(self.col_names).issubset(set(other.col_names)) and \
            set(self.col_names) != set(other.col_names)
            
    def __eq__(self, other):
        '''
        Return
         * 0:   If column names are not the same, even when disregarding order
         * 1:   If column names are the same, but only when reordering
         * 2:   Column names are the same with the same order
        '''

        self_names = [i.lower() for i in self.col_names]
        other_names = [i.lower() for i in other.col_names]
        
        if self_names == other_names:
            return 2
        elif set(self_names) == set(other_names):
            return 1
        else:
            return 0
            
    def __add__(self, other):
        '''
        Return a new ColumnList object with the union of the column
        names of types of the original ColumnList
         * Not commutative: The order of the first ColumnList is preserved
        '''
        
        new_columns = ColumnList(self.col_names, self.col_types)
        for x, y in other.as_tuples():
            if x not in self.col_names:
                new_columns.add_col(x, y)
        
        return new_columns
        
    def __sub__(self, other):
        '''
        Return all columns in this ColumnList minus those also in second
        '''
        
        new_columns = ColumnList()
        for x, y in self.as_tuples():
            if x not in other.col_names:
                new_columns.add_col(x, y)
        
        return new_columns