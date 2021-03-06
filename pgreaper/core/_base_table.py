''' Base class for Table and SQLTable objects '''

from pgreaper.notebook.css import SQLIFY_CSS
from collections import OrderedDict

class BaseTable(list):
    def __init__(self, name=None, row_values=[], *args, **kwargs):
        self.name = name
        super(BaseTable, self).__init__(row_values)
    
    def __repr__(self):
        ''' Print a short and useful summary of the table '''

        self.guess_type()
        
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
        text += '{} rows x {} columns\n'.format(len(self), self.n_cols)
        
        # Composite keys
        if isinstance(self.p_key, tuple):
            text += 'Composite Primary Key: {}'.format(
                ', '.join(self.col_names[i] for i in self.p_key))
                
        text += '\n\n'
            
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
        
    def _repr_html_(self, n_rows=100, id_num=None, plain=False, clean=False):
        '''
        Pretty printing for Jupyter notebooks
        
        Parameters
        -----------
        id_num:     int
                    Number of Table in a sequence
        n_rows:     int
                    Number of rows to print. Set to -1 to print all.
        plain:      bool
                    Output as a plain HTML table
        clean:      bool
                    Clean up table for export as an external HTML file
                     - No trimming
                     - No header or table metadata
        '''
        
        self.guess_type()
        
        def trim(data, length=100, quote_string=True):
            # Make string HTML friendly
            string = str(data).replace('<', '&lt;').replace('>', '&gt;')
            if clean:
                return string
            else:
                # Trim data to specified length
                # Add quotes if data was originally a string                
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
            if (i > n_rows) and (not clean):
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
                
        # Composite primary key information
        if isinstance(self.p_key, tuple):
            subtitle += '<h3>Composite Primary Key: {}</h3>\n'.format(
                ', '.join(self.col_names[i] for i in self.p_key))
        
        # Strip out metadata if clean = True
        if not clean:
            if plain:
                html_str = title + subtitle
            else:
                html_str = SQLIFY_CSS + title + subtitle
        else:
            html_str = ''
        
        html_str += '''
            <table class="pgreaper-table">
                <thead>
                    <tr><th></th><th>{col_names}</th></tr>
                    <tr><th></th><th>{col_types}</th></tr>
                </thead>
                <tbody>
                    {row_data}
                </tbody>
            </table>'''.format(
                col_names = '</th><th>'.join([i for i in self.col_names]),
                col_types = '</th><th>'.join([str(i) for i in self.col_types]),
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
                raise KeyError("Couldn't find a column named {0} from {1}.".format(
                    col, self.col_names))
        else:
            raise ValueError('Please specify either an index (integer) or column name (string) for col.')