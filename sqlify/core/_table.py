from .schema import SQLType

def guess_type(self):
    ''' Guesses column data type by trying to accomodate all data '''
    final_types = {}
    
    # Looping over column names
    for i in self._type_cnt:
        # Looping over data types
        for type in self._type_cnt[i]:
            if i not in final_types:
                final_types[i] = SQLType(type, table=self)
            else:
                final_types[i] = final_types[i] + SQLType(type, table=self)
            
    self.col_types = list(final_types.values())