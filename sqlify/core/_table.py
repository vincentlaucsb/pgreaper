'''
Table Methods
- Contains table methods that have no Cython components
- Allows for easier debugging
'''

from sqlify._globals import PYTHON_VERSION
from .mappings import CaseInsensitiveDict
from .schema import SQLType

def add_dicts(self, dicts, filter=False, extract={}):
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
        for key in [str(i) for i in row if str(i).lower() not in self.columns]:
            self.add_col(key, None)
            
        # Map column indices to keys and create the new row
        map = self.columns.map(*row)
        for i in range(0, self.n_cols):
            try:
                new_row.append(row[map[i]])
            except KeyError:
                new_row.append(None)
                
        self.append(new_row)
        
def guess_type(self):
    ''' Guesses column data type by trying to accomodate all data '''
    # Maps column names to data types
    # final_types = {}
    final_types = CaseInsensitiveDict()
    
    # Looping over column names
    for i in self._type_cnt:
        # Looping over data types
        for type in self._type_cnt[i]:
            if i not in final_types:
                final_types[i] = SQLType(type, table=self)
            else:
                final_types[i] = final_types[i] + SQLType(type, table=self)
                
    # Remove NULLs --> Float
    for k, v in zip(final_types.keys(), final_types.values()):
        if v == 'NoneType':
            final_types[k] = SQLType(float, table=self)
    
    # Note: Assumes dicts are ordered
    # self.col_types = list(final_types.values())
    
    self.col_types = [final_types[i] for i in self.col_names]