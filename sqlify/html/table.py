from sqlify.table import Table

class HTMLTable(Table):
    ''' Python representation of an HTML table '''
    
    def __init__(self, n_cols, *args, **kwargs):
        
        # Initialize with placeholder arguments    
        super(HTMLTable, self).__init__(
            name=None, col_names=None
        )
        
        self.n_cols = n_cols
        
        if 'col_types' not in kwargs:
            self.col_types = ['TEXT' for i in range(0, self.n_cols)]