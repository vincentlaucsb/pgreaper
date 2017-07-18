from collections import Counter, defaultdict

# TODO: Implement a container for HTMLNode children which subclasses list
# and has a dictionary of tags and pointers to instances of them 
# for faster specific tag lookup

class HTMLNode(dict):
    ''' Represents an HTML tag with all its attributes and child nodes '''
    
    def __init__(self, tag, parent=None, **kwargs):
        ''' Arguments:
        
        * tag:      What HTML tag this is
        * parent:   Pointer to parent node
        * kwargs:   HTML attributes
        '''
        
        super(HTMLNode, self).__init__()
        
        self['tag'] = tag
        self['data'] = ''
        self['children'] = HTMLNodeChildren()
        
        # Load attributes
        for attr in kwargs:
            self[attr] = kwargs[attr]
        
        self.parent = parent
        
        # Add itself to parent's list of children
        if parent:
            parent['children'].append(self)
                
    def __setitem__(self, key, attr):
        # Type-cast numeric attributes to numbers
        if key in ['colspan', 'rowspan']:
            super(HTMLNode, self).__setitem__(key, int(attr))
        else:
            super(HTMLNode, self).__setitem__(key, attr)
    
    def get_child_tags(self):
        ''' From immediate children, count number of each tag '''
        return Counter(self['children'].tags_())
        
    def before(self, tags=None, n_look=10):
        '''
         * Get the node before
         * If tag is specified, go up to 10 nodes back and
           return nearest node matching tag '''
        
        node_index = self.parent['children'].index(self)
        
        if (not tags) and node_index:
            return self.parent['children'][node_index - 1]
        
        if node_index < n_look:
            n_look = node_index
           
        i = 0
           
        while i < n_look:
            current_node = self.parent['children'][node_index - i]
            
            if isinstance(current_node, HTMLNode) and \
                current_node['tag'] in tags:
                return current_node
                
            i += 1
            
        return None
    
    def get_data(self):
        ''' Unnest children which contain text data e.g. link anchors,
            em, b, etc... '''
            
        target_tags = set(['a', 'abbr', 'span', 'em', 'b', 'strong', 'i', 'font', 'div'])
        
        data = ''
        data += self['data']
        
        for node in self['children']:
            if isinstance(node, str):
                data += node
            elif node['tag'] in target_tags:
                data += node.get_data()  # Recursive part
            
        return data
    
    def get_child(self, tag, n=0):
        ''' Get the n-th child with specified tag '''
        
        try:
            if tag in self['children'].tags_():
                return self['children'].tags[tag][n]
            else:
                return None
        except IndexError:
            return None
    
    def search_tag(self, tags, n=-1, recurse=True):
        ''' Like search() but less flexible (can only search for tags)
            but faster
           
            Arguments:
             * tags:    A tag or list of tags            
        '''
        
        # Only one tag to parse
        if isinstance(tags, str):
            tags = [tags]
        
        results = []
            
        # Add tags that are immediate children
        for tag in tags:
            results += self['children'].tags[tag]
            
        # Recursively walk down tree
        if recurse:
            for node in self['children']:
                if (n > 0) and (len(results) > n):
                    break
                elif isinstance(node, HTMLNode):
                    results += node.search_tag(tag)
            
        return results             
            
    def search(self, n=-1, recurse=True, **kwargs):
        ''' Returns a list of all child nodes at any level matching the
            search criterion
            
        Arguments:
         * n:        Number of results to get. -1 --> Get all
         * recurse:  Perform a recursive search
         * kwargs:   Keys are fields to be searched, values are strings to be found            
        '''
        
        search_params = set(kwargs.keys())
        
        results = []
        
        for child in self['children']:
            if (n > 0) and (len(results) > n):
                break
            elif isinstance(child, HTMLNode):
                for key in search_params.intersection(child.keys()):
                    if child[key] == kwargs[key]:
                        results.append(child)
                        
                # Recursive part
                if recurse:
                    results += child.search(**kwargs)
                        
        return results
        
class HTMLNodeChildren(list):
    ''' A list of child nodes which also keeps some dictionaries for 
        faster lookups '''
        
    def __init__(self, *args):
        super(HTMLNodeChildren, self).__init__(*args)
        self.tags = defaultdict(lambda: [])
        
    def append(self, node):
        super(HTMLNodeChildren, self).append(node)
    
        if isinstance(node, HTMLNode):
            self.tags[node['tag']].append(node)
            
        # Other case: Strings containing data
        
    def tags_(self):
        ''' Return tags of child nodes '''
        return self.tags.keys()