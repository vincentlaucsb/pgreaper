class ColumnList(object):
    ''' A class which makes it easier to do "math" with lists of columns '''
    
    def __init__(self, col_names=[], col_types=[]):
        self.col_names = col_names
        self.col_types = col_types
        
    def add_col(self, name, type):
        ''' The current method for adding a column '''
        self._col_names.append(name)
        self._col_types.append(type)
        
    @property
    def col_names(self):
        return self._col_names
    
    @col_names.getter
    def col_names(self):
        return [i.lower() for i in self._col_names]
    
    @col_names.setter
    def col_names(self, value):
        if not value:
            self._col_names = []
        else:
            self._col_names = value
        
    @property
    def col_types(self):
        return self._col_types
        
    @col_types.getter
    def col_types(self):
        return [i.lower() for i in self._col_types]
        
    @col_types.setter
    def col_types(self, value):
        if not value:
            self._col_types = []
        else:
            self._col_types = value
        
    def n_cols(self):
        ''' Return number of columns '''
        return len(self.col_names)
        
    def __iter__(self):
        return iter(self.col_names)
        
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

        if self.col_names == other.col_names:
            return 2
        elif set(self.col_names) == set(other.col_names):
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
        
    def as_tuples(self):
        ''' Return a list of (column name, column type) tuples '''
        return [(x, y) for x, y in zip(self.col_names, self.col_types)]