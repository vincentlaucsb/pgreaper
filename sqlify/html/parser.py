from sqlify.html._parser import *
from sqlify.html.table import HTMLTable
from sqlify.html.tree import HTMLNode

from html.parser import HTMLParser
       
class HTMLTreeParser(HTMLParser):
    ''' Parses through an HTML document and creastes a tree '''

    def __init__(self):
        super(HTMLTreeParser, self).__init__()
        
        # Set to True when <body> encountered
        self.currently_parsing = False
        
        # Should store the <body> tag
        self.head_node = None
        self.current_node = None
        
    def handle_starttag(self, tag, attrs):
        if self.currently_parsing:
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
            if tag == 'body':
                self.currently_parsing = True
                body_node = HTMLNode(tag='body')
                self.head_node = self.current_node = body_node

    def handle_endtag(self, tag):
        if self.currently_parsing:
            if tag == 'body':
                self.currently_parsing = False
            else:
                try:
                    self.current_node = self.current_node.parent
                except:
                    import pdb; pdb.set_trace()

    def handle_data(self, data):
        if self.currently_parsing:
            self.current_node['data'] += data
            
def html_to_tree(html):
    ''' Converts a string of HTML code to a tree-like structure '''
    
    parser = HTMLTreeParser()
    parser.feed(html)
    
    return parser.head_node
    
def tree_to_table(tree):
    ''' Given the head node of an HTML tree, find all tables
    
    Rules:
    1. Nested tables get unnested
    
    '''
    
    tables_raw = tree.search(tag='table')
    
    # Should be a list of Table objects
    tables = []
    
    for tbl in tables_raw:
        # import pdb; pdb.set_trace()
        # Determine how wide the table is
        sample_row = tbl.search(n=1, tag='tr')
        
        n_cols = 0
        
        for cell in sample_row[0]['children']:
            if cell['tag'] in ['td', 'th']:
                try:
                    n_cols += cell['colspan']
                except KeyError:  # No colspan
                    n_cols += 1
    
        new_table = HTMLTable(n_cols=n_cols)
    
        # TO DO: Implement tbody and thead handling
        for row in [i for i in tbl['children'] if i['tag'] not in ['tbody', 'thead', 'caption']]:
            #try:
            if row['children'][0]['tag'] == 'th':
                handle_th(new_table, row)
            else:
                new_table.append([cell.get_data() for cell in row['children']])
            # except IndexError:
            #     import pdb; pdb.set_trace()
    
        # Create a Table with placeholder values
        tables.append(new_table)
    
    return tables