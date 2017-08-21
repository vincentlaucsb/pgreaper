TABLEBROWSER_MAX_REPR = 30

from sqlify._globals import import_package
from .table import html_table
from ._parser import *
from .tree import HTMLNode
requests = import_package('requests')

from collections import deque
from copy import deepcopy
from html.parser import HTMLParser
import functools

def _assert_requests(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if requests:
            return func(*args, **kwargs)
        else:
            raise ImportError('The requests package must be installed for this feature.')
            
    return inner

class HTMLTreeParser(HTMLParser):
    ''' Parses through an HTML document and creates a tree '''
    
    ignore = set(['br', 'hr', 'script'])
    containers = set(['td', 'th'])

    def __init__(self):
        super(HTMLTreeParser, self).__init__()
        
        # Should store the <html> tag if parsing a complete HTML document
        self.head_node = None
        self.current_node = None
        
    def handle_starttag(self, tag, attrs):
        if tag not in HTMLTreeParser.ignore:
            if self.head_node:
                '''
                Example of what attrs looks like:
                
                ('src', 'python-logo.png')
                ('alt', 'The Python logo')
                '''
            
                attr_dict = {}
                
                for attr in attrs:
                    attr_dict[attr[0]] = attr[1]
            
                new_node = HTMLNode(tag=tag,
                    parent=self.current_node, **attr_dict)
                    
                self.current_node = new_node
            else:
                self.head_node = self.current_node = HTMLNode(tag=tag)

    def handle_endtag(self, tag):
        # If current_node = None, do nothing
        if (tag not in HTMLTreeParser.ignore) and self.current_node:
            self.current_node = self.current_node.parent
            
    def handle_data(self, data):
        '''
        Rules:
         1. If current node is a "container", e.g. a table cell, add data as a string to children
         2. Otherwise, store in data attribute
        '''
        try:
            if self.current_node['tag'] in HTMLTreeParser.containers:
                self.current_node['children'].append(data)
            else:
                self.current_node['data'] += data
                
        # There's data but no current node (!!!)
        except TypeError:
            pass

class TableBrowser(list):
    '''
    **Purpose**
    
     * Stores a list of tables and supports pretty displays
     * Returns copies of Tables instead of the original Table when indexing
       operator is used
     * Intended to be used by, but not created by end users
     
    **Grabbing Individual Tables**
    
    .. code-block:: python
    
       import sqlify.html as html_table
        
       # This returns a TableBrowser object
       tables = html_table.from_file('nfl-playoffs.html')
        
       # This returns a Table
       player_stats = tables[4]
    '''
    
    def __init__(self, source=None):
        '''
        Arguments:
         * source:   Filename or URL of parsed HTML file
        '''
        
        super(TableBrowser, self).__init__()
        self.source = source
    
    def __getitem__(self, key):
        ''' Return a copy of a Table instead of the original Table '''
        return deepcopy(super(TableBrowser, self).__getitem__(key))
    
    def __repr__(self):
        repr_str = "{source}\n{num_tbl} tables found".format(
            num_tbl = len(self),
            source = self.source
        )
        
        return repr_str
    
    def _repr_html_(self):
        ''' Pretty summaries for Jupyter notebooks '''
        
        # Short Summary
        html_str = "<h2>{source}</h2><h3>{num_tbl} tables found</h3>".format(
            num_tbl = len(self),
            source = self.source
        )
        
        # Short summary of tables contained
        for i, tbl in enumerate(self):
            if i >= TABLEBROWSER_MAX_REPR:
                break
        
            try:
                html_str += tbl._repr_html_(id_num=i).replace('<h2>','<h4>').replace('</h2>', '</h4>')
            except:
                html_str += "<h4>Parsing Error</h4>"
        
        return html_str
        
    def append(self, table):
        ''' Don't append empty tables '''
        
        placeholder_col_names = ['col{}'.format(i) for i in range(0, table.n_cols)]
        
        is_empty = bool((placeholder_col_names == table.col_names) \
            and len(table) == 0)

        if not is_empty:
            super(TableBrowser, self).append(table)
            
    def rbind(self, i, j=None, **kwargs):
        ''' Merge tables together 
        
        i should either be:
         * Row index
         * Slice object
         
        j should be:
         * Row index
         * Do not use with i
         
        Other Arguments:
         * kwargs:      Attributes of the new table
        '''

        if isinstance(i, slice):
            k = i.start  # Index of current table
            
            if i.stop:
                stop = i.stop
            else:
                stop = len(self)
            if i.step:
                step = i.step
            else:
                step = 1
            
            # Initialize
            new_table = self[k]
            k += step
            
            while k < stop:
                new_table += self[k]
                k += step
        else:
            new_table = i + j
            
        for attr in kwargs:
            setattr(new_table, attr, kwargs[attr])
            
        return new_table

class _SavedRowspan(dict):
    '''
    Keeps track of multiple rowspan arguments
     * When adding to other SavedRowspan objects, combine unique
       keys and values
    '''
    
    def __init__(self, *args, **kwargs):
        super(_SavedRowspan, self).__init__(*args, **kwargs)

    def __add__(self, other):
        new_dict = _SavedRowspan()
        
        for key in self:
            new_dict[key] = self[key]
        
        for key in other:
            new_dict[key] = other[key]
            
        return new_dict
        
class TableParser(object):
    ''' Given an HTML tree, parse a Table '''
    
    def __init__(self, tree):
        self.html_tree = tree
        
        # Temporary flag variables for individual tables
        self.has_column_names = False
        
        # Should be a deque of SavedRowspan dicts
        self.saved_rowspans = deque()
    
    def _find_tables(self):
        ''' Find HTML tables in tree 
        
        Rules:
        1. Nested tables get unnested 
        '''
        
        tables = []
        
        if self.html_tree['tag'] == 'table':
            tables.append(self.html_tree)
            
        # tables += self.html_tree.search(tag='table')
        tables += self.html_tree.search_tag('table')
        
        return tables
    
    def _handle_tbody(self, table, node):
        '''
        Handle the actual contents of an HTML table
         * table = Table object to append data to
         * Despite the name of this function, node can either:
            * A tbody node or
            * A table node (if it has no thead or tbody)
        '''
        
        for row in node['children']:
            child_tags = row.get_child_tags()
            th = child_tags['th']
            td = child_tags['td']

            if th and (not td):
                # Case 1: Only th
                if not self.has_column_names:
                    # If table has no column names, treat as column names
                    table.col_names = self._handle_row(table, row)
                    self.has_column_names = True
                else:
                    # Otherwise, treat as data
                    table.append(self._handle_row(table, row))
            else:
                # Case 2: Only td or Case 3: Mixed th + td
                # Treat as data
                table.append(self._handle_row(table, row))
    
    def _handle_row(self, table, node):
        '''
        Given a node representing a row of cells, parse it
         - i.e. return list of data
        
        Arguments:
         * table:   Table being operated on
         * node:    tr node
        
        Rules:
         1. Expand colspans
         2. Save rowspans
         3. Account for previously saved rowspans
        '''
        
        row = []
        cell_index = 0
        last_cell = 0   # Index of last cell from this tr to be pulled
        
        # Previous rowspans
        if self.saved_rowspans:
            prev_rowspans = self.saved_rowspans.popleft()
        else:
            prev_rowspans = {}
                    
        while True:                    
            # Add anything from previous rowspans
            while cell_index in prev_rowspans:
                row.append(prev_rowspans[cell_index])
                cell_index += 1
            
            if cell_index >= table.n_cols:
                break
            
            # Get cells from this row
            try:
                cell = node['children'][last_cell]
                cell_data = self._handle_cell(node=cell, i=cell_index)
                last_cell += 1
                
                # Account for colspan
                row += cell_data
                cell_index += len(cell_data)
            except IndexError:
                # IndexErrors can still happen if original HTML was malformed
                row.append('')   # Fill with empty string
                cell_index += 1
                    
        return row
        
    def _handle_cell(self, node, i):
        '''
        Given a node representing a td, parse it
         * Returns a list of either one or multiple repeated values
           * (Due to colspan)
        
        Arguments:
         * node:    td node
         * i:       Index of current node
        '''
        
        # This check inspired by terrible CIA World Factbook HTML
        # Sometimes in poorly formatted HTML text "data" ends up in 
        # tables but not contained in td or th elements
        if isinstance(node, HTMLNode):
            cells = []
            cell_data = node.get_data()
            colspan = rowspan = 1
            
            if 'colspan' in node.keys():
                colspan = node['colspan']
            if 'rowspan' in node.keys():
                rowspan = node['rowspan']
             
            # If cell has rowspan attribute, save for future use
            saved_rowspan = _SavedRowspan()
                
            while colspan:
                cells += [cell_data]
                saved_rowspan[i] = cell_data
                
                i += 1
                colspan -= 1
                
            rowspan -= 1
            
            # Add rowspan to instance queue
            for j in range(0, rowspan):
                try:
                    # Case 1: Previous cells at this row also had rowspan
                    self.saved_rowspans[j] += saved_rowspan
                except IndexError:
                    # Case 2: No other cells with rowspan
                    self.saved_rowspans.append(saved_rowspan)

            return cells
    
        return ''
    
    def parse(self):
        ''' Convert HTML tables into Table objects '''
        
        table_nodes = self._find_tables()
        tables = TableBrowser()           # A list of Table objects
        
        for table in table_nodes:
            # Reset table parsing metadata
            self.has_column_names = False
            self.saved_rowspans.clear()
        
            # Determine how wide the table is
            # For robustness, use first 10 rows instead of just one        
            n_cols = count_cols(table.search_tag(tags='tr', n=10))
            # n_cols = count_cols(table.search(tag='tr', n=10))
            
            new_table = html_table(n_cols=n_cols)
            
            # <caption> Handling
            # import pdb; pdb.set_trace()
            caption = table.get_child('caption')
            
            if caption:
                new_table.name = caption['data']
            
            # <thead> Handling
            thead = table.get_child('thead')
            
            if thead:
                # import pdb; pdb.set_trace()
                new_table.col_names = [cell.get_data() for cell in thead.search_tag(
                    tags=['td', 'th'])]
                self.has_column_names = True
            
            # <tbody> Handling
            tbody = table.get_child('tbody')
            
            if tbody:
                self._handle_tbody(new_table, tbody)
            else:
                self._handle_tbody(new_table, table)
            
            # Look at preceeding headers and spans for table name if necessary
            if not new_table.name:
                try:
                    new_table.name = table.before(
                        tags=['h1', 'h2', 'h3', 'h4', 'h5', 'h6']).get_data()
                except AttributeError:
                    pass
            
            new_table.drop_empty()
            
            '''
            Get column types
             - IndexError occurs if original HTML table is malformed
               - i.e. didn't have enough tds
            '''
            
            try:
                new_table.col_types = new_table.guess_type()
            except IndexError:
                pass
                
            tables.append(new_table)
        
        return tables
        
def html_to_tree(html):
    ''' Converts a string of HTML code to a tree-like structure '''
    
    parser = HTMLTreeParser()
    parser.feed(html)
    
    return parser.head_node
    
def tree_to_table(tree):
    ''' Given a HTML tree, find tables and return a TableBrowser '''
    
    parser = TableParser(tree)
    return parser.parse()

def get_tables(html):
    raise NotImplementedError
    # ''' Parse HTML code from direct input '''

    # html_tree = html_to_tree(html.replace('\n', ''))
    
    # tables = tree_to_table(html_tree)
    # tables.source = "Direct Input"
    
    # return tables
    
def get_tables_from_file(file, encoding='utf-8'):
    '''
    Given a filename, parse it and return a list of tables.
    
    Basic Usage:
     >>> import sqlify.html as html_table
     >>> tables = html_table.from_file(filename)
     >>> tables
    '''
    
    if file:
        with open(file, encoding=encoding, mode='r') as html_file:
            html = ''.join(html_file.readlines()).replace('\n', '')
            
    html_tree = html_to_tree(html)
    
    tables = tree_to_table(html_tree)
    tables.source = file    
    
    return tables
    
@_assert_requests
def get_tables_from_url(url):
    '''
    .. note:: This feature requires that the `requests` package be installed.
    
    Given a URL, parse it and return a list of tables.
    
    Basic Usage:
     >>> import sqlify
     >>> tables = sqlify.html.from_url(url)
     >>> tables
    
    '''

    http_get = requests.get(url)
    
    html = http_get.text.replace('\n', '')
    html_tree = html_to_tree(html)
    
    tables = tree_to_table(html_tree)
    tables.source = url
    
    return tables