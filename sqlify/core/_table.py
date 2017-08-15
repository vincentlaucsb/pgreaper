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