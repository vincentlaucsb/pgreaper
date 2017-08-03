''' Base class for Table and SQLTable objects '''

from sqlify.notebook.css import SQLIFY_CSS

from collections import OrderedDict

class BaseTable(list):
    def __init__(self, name=None, row_values=[], *args, **kwargs):
        self.name = name
        super(BaseTable, self).__init__(row_values)
    
    def __repr__(self):
        ''' Print a short and useful summary of the table '''

        def trim(data, length=12, quote_string=True):
            '''
             * Trim data to specified length
             * Add quotes if data was originally a string            
            '''            
            string = str(data)
            
            if isinstance(data, str) and quote_string:
                length -= 2  # Account for quotes
            
            if len(string) > length:
                string = string[0: length - 2] + '..'
                
            if isinstance(data, str) and quote_string:
                return "'{}'".format(string)
            
            return string
            
        text = '\n'
        text += '{}\n'.format(self.name)
        text += '{} rows x {} columns\n\n'.format(len(self), self.n_cols)
            
        # Column names and data types
        text += ''.join([' {:^12} '.format(trim(name, quote_string=False)) \
            for name in self.col_names[0:8]])
        text += '\n'
        text += ''.join([' {:^12} '.format(trim(dtype, quote_string=False)) \
            for dtype in self.col_types[0:8]])
        text += '\n' + '-' * 112 + '\n'
            
        # Print first 10 rows
        for i, row in enumerate(self):
            if i >= 10:
                break
            for j, col in enumerate(row):
                if j >= 8:
                    break
                text += ' {:^12} '.format(trim(col))
            text += '\n'
            
        return text
        
    def _repr_html_(self, n_rows=100, id_num=None, plain=False):
        '''
        Pretty printing for Jupyter notebooks
        
        Arguments:
         * id_num:  Number of Table in a sequence
         * plain:   Output as a plain HTML table
        '''
        
        def trim(data, length=100, quote_string=True):
            '''
             * Trim data to specified length
             * Add quotes if data was originally a string            
            '''            
            string = str(data)
            
            if isinstance(data, str) and quote_string:
                length -= 2  # Account for quotes
            
            if len(string) > length:
                string = string[0: length - 2] + '..'
                
            if isinstance(data, str) and quote_string:
                return "'{}'".format(string)
            
            return string
        
        row_data = ''
        
        # Print only first 100 rows and limit individual cells to 100 chars
        for i, row in enumerate(self):
            if i > n_rows:
                break
            row_data += '<tr><td>[{index}]</td><td>{data}</td></tr>\n'.format(
                index = i,
                data = '</td><td>'.join([trim(i) for i in row]))
        
        try:
            name = "{} ({})".format(self.name, self.source)
        except AttributeError:
            name = self.name
        
        if id_num is not None:
            title = '<h2>[{0}] {1}</h2>\n'.format(id_num, name)
        else:
            title = '<h2>{}</h2>\n'.format(name)
            
        subtitle = '<h3>{} rows x {} columns</h3>\n'.format(
            len(self), self.n_cols)
        
        if plain:
            html_str = title + subtitle
        else:
            html_str = SQLIFY_CSS + title + subtitle
        
        html_str += '''
            <table class="sqlify-table">
                <thead>
                    <tr><th></th><th>{col_names}</th></tr>
                    <tr><th></th><th>{col_types}</th></tr>
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
        
    ''' Table Manipulation Methods '''
    def _parse_col(self, col):
        ''' Finds the column index a column name is referering to '''
        
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
            
    def apply(self, col, func, i=False):
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
        ====================  =======================================================

        ''' 
        
        index = self._parse_col(col)
        
        for row_index, row in enumerate(self):
            arguments = OrderedDict()
            
            if i:
                arguments['i'] = row_index
            
            row[index] = func(row[index], **arguments)
            
    def aggregate(self, col, func=None):
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

        if func:
            return func([row[index] for row in self])
        else:
            return [row[index] for row in self]