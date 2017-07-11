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
        self['children'] = []
        
        # Load attributes
        for attr in kwargs:
            self[attr] = kwargs[attr]
        
        self.parent = parent
        
        # Add itself to parent's list of children
        if parent:
            self._add_to_parent(parent)
            
    def __setitem__(self, key, attr):
        # Type-cast numeric attributes to numbers
        if key in ['colspan', 'rowspan']:
            super(HTMLNode, self).__setitem__(key, int(attr))
        else:
            super(HTMLNode, self).__setitem__(key, attr)

    def _add_to_parent(self, parent):
        parent['children'].append(self)
        
    def get_data(self):
        ''' Unnest children which contain text data e.g. link anchors,
            em, b, etc... '''
            
        target_tags = set(['a', 'span', 'em', 'b', 'strong', 'i'])
        
        data = ''
        data += self['data']
        
        for node in self['children']:
            if node['tag'] in target_tags:
                data += node['data']
                
            # Expand abbreviations
            if node['tag'] == 'abbr':
                data += node['data']
                
                # Actually forget about that for now
                # data += node['title']
        
        return data
        
    def search(self, n=-1, **kwargs):
        ''' Returns a list of all child nodes at any level matching the
            search criterion
            
            Arguments:
            
            * n:        Number of results to get. -1 --> Get all
            * kwargs:   Keys are fields to be searched, values are strings to be found
            
        '''
        
        search_params = set(kwargs.keys())
        
        results = []
        
        for child in self['children']:
            if (n > 0) and (len(results) > n):
                break
        
            for key in search_params.intersection(child.keys()):
                if child[key] == kwargs[key]:
                    results.append(child)
                    
            # Recursive part
            recurse = child.search(**kwargs)
            
            if recurse:  # Don't add empty results
                results += recurse
                        
        return results