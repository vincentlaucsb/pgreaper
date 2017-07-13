try:
    import requests
    REQUESTS_INSTALLED = True
except ModuleNotFoundError:
    REQUESTS_INSTALLED = False    

from sqlify.html._parser import *
from sqlify.html.table import html_table
from sqlify.html.tree import HTMLNode

from html.parser import HTMLParser
       
class HTMLTreeParser(HTMLParser):
    ''' Parses through an HTML document and creates a tree '''
    
    ignore = ['br', 'hr']

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
        if tag not in HTMLTreeParser.ignore:
            self.current_node = self.current_node.parent
            
    def handle_data(self, data):
        self.current_node['data'] += data

class TableBrowser(list):
    ''' Stores a list of tables and supports pretty displays '''
    
    def __init__(self, source=None):
        ''' Arguments:
        
        * source:   Filename or URL
        '''
        
        self.source = source
        
        super(TableBrowser, self).__init__()
    
    def __repr__(self):
        repr_str = "{source}\n{num_tbl} tables found".format(
            num_tbl = len(self),
            source = self.source
        )
        
        return repr_str
    
    def _repr_html_(self):
        ''' Pretty printing for Jupyter notebooks '''
        
        # Short Summary
        html_str = "<h2>{source}</h2><h3>{num_tbl} tables found</h3>".format(
            num_tbl = len(self),
            source = self.source
        )
        
        # Short summary of tables contained
        for tbl in self:
            try:
                html_str += tbl._repr_html_().replace('<h2>','<h4>').replace('</h2>', '</h4>')
            except:
                html_str += "<h4>Parsing Error</h4>"
        
        return html_str

class TableParser(object):
    ''' Given an HTML tree, parse a Table '''
    
    def __init__(self, tree):
        self.html_tree = tree
        
        # Temporary flag variables for individual tables
        self.has_column_names = False
    
    def _find_tables(self):
        ''' Find HTML tables in tree 
        
        Rules:
        1. Nested tables get unnested 
        '''
        
        tables = []
        
        if self.html_tree['tag'] == 'table':
            tables.append(self.html_tree)
            
        tables += self.html_tree.search(tag='table')
        
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
                    # If table has not column names, treat as column names
                    table.col_names = [cell.get_data() for cell in row['children']]
                    self.has_column_names = True
                else:
                    # Otherwise, treat as data
                    table.append([cell.get_data() for cell in row['children']])
            else:
                # Case 2: Only td or Case 3: Mixed th + td
                # Treat as data
                table.append([cell.get_data() for cell in row['children']])
                
    def parse(self):
        ''' Convert HTML tables into Table objects '''
        
        table_nodes = self._find_tables()
        tables = TableBrowser()           # A list of Table objects
        
        for table in table_nodes:
            # Reset table parsing metadata
            self.has_column_names = False
        
            # Determine how wide the table is
            # For robustness, use first 10 rows instead of just one        
            n_cols = count_cols(table.search(n=10, tag='tr'))
            
            new_table = html_table(n_cols=n_cols)
            
            # <caption> Handling
            caption = table.get_child('caption')
            
            if caption:
                new_table.name = caption['data']
            
            # <thead> Handling
            thead = table.get_child('thead')
            
            if thead:
                new_table.col_names = [cell.get_data() for cell in thead['children']]
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
            
            # Get column types: Doesn't work yet until I add colspan + rowspan handling
            # new_table.col_types = new_table.guess_type()
            
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
        
def get_tables_from_file(file):
    ''' Given a filename, grab it, parse it, and return a list of tables '''
    
    if file:
        with open(file, 'r') as html_file:
            html = ''.join(html_file.readlines()).replace('\n', '')
            
    html_tree = html_to_tree(html)
    
    tables = tree_to_table(html_tree)
    tables.source = file    
    
    return tables
    
def get_tables_from_url(url):
    ''' Given a URL, parse it and return a list of tables '''
    
    if not REQUESTS_INSTALLED:
        raise ModuleNotFoundError("The 'requests' package is required for this functionality.")
    
    http_get = requests.get(url)
    
    html = http_get.text.replace('\n', '')
    html_tree = html_to_tree(html)
    
    tables = tree_to_table(html_tree)
    tables.source = url
    
    return tables