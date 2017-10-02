''' General Purpose Dict-Based Classes '''

from itertools import chain
from collections import defaultdict

class CaseInsensitiveDict(dict):
    '''
    A dictionary which allows case-insensitive lookups 
    
    Unlike the implementation in `requests` (see: https://github.com/
        requests/requests/blob/v1.2.3/requests/structures.py#L37)
    this also supports non-string keys
    '''    
    
    def __init__(self, *args, **kwargs):
        for item in args:
            if isinstance(item, dict):
                item = { k.lower():v for k, v in item.items() }
        kwargs = { k.lower():v for k, v in kwargs.items() }
        super(CaseInsensitiveDict, self).__init__(*args, **kwargs)
    
    def __setitem__(self, key, value):
        if isinstance(key, str):
            key = key.lower()        
        super(CaseInsensitiveDict, self).__setitem__(key, value)
        
    def __getitem__(self, key):
        if isinstance(key, str):
            key = key.lower()
        return super(CaseInsensitiveDict, self).__getitem__(key)
        
    def __delitem__(self, key):
        if isinstance(key, str):
            key = key.lower()
        super(CaseInsensitiveDict, self).__delitem__(key)
                
class SymmetricIndex(dict):
    '''    
    Let f be this index, and a, b be any keys or values. Then,
    f[a][b] = f[b][a]
    
    Motivation:
    Suppose this index returns the data type that makes two disparate
    types compatible, then an example would be
    
    postgres_type['integer']['double precision'] = 'double precision'
    postgres_type['double precision']['integer'] = 'double precision'
    
    Usage:
    - Only one insert or update needs to be performed for any pair of keys
    - This implementation handles the rest
    - Deleting keys is currently not implemented (or necessary yet)
    
    Caveats:
    This index simply maintains that:
        - With any pair of keys, the same value is returned regardless of order
        - Not one-to-one, e.g.
            - postgres_type['integer']['text'] = 'text'
            - postgres_type['text']['datetime'] = 'text'
          is possible and a valid use
        - If f[a][b] is set to c, but then f[b][a] is set to d, then
          f[a][b] should auto-update to c
    '''
    
    class Node(defaultdict):
        ''' Dict which keeps track of the SymmetricIndex which spawned it '''
        def __init__(self, parent, preimage, dict_={}):
            '''
            Parameters
            -----------
            parent:     SymmetricIndex
                        The index this node belongs to
            preimage:   str
                        The key (of parent) that this node is stored under
            dict_:      dict
                        Intialization values
            '''
            self.parent = parent
            self.preimage = preimage
            
            super(SymmetricIndex.Node, self).__init__(lambda: 'text')
            
            # This also sets the inverse
            if dict_:
                for k, v in zip(dict_.keys(), dict_.values()):
                    self[k] = v
                    
        def __add__(self, other):
            # Update in place
            for k, v in zip(other.keys(), other.values()):
                self[k] = v
            
        def setitem(self, key, value):
            # Allows other nodes to update this node without triggering
            # an endless circle of __setitem__
            super(SymmetricIndex.Node, self).__setitem__(key, value)
            
        def __setitem__(self, key, value):
            # Update self
            super(SymmetricIndex.Node, self).__setitem__(key, value)
            
            # Update other
            self.parent[key].setitem(self.preimage, value)
    
    def __init__(self):
        super(SymmetricIndex, self).__init__()
        
    def __getitem__(self, key):
        if key not in self:
            super(SymmetricIndex, self).__setitem__(key,
                self.Node(parent=self, preimage=key))
        
        return super(SymmetricIndex, self).__getitem__(key)
        
    def __setitem__(self, key, value):
        if isinstance(value, dict):
            super(SymmetricIndex, self).__setitem__(key,
                self.Node(parent=self, preimage=key, dict_=value))
        else:
            raise TypeError('SymmetricIndex values must be dicts.')
            
    def __delitem__(self, key):
        raise NotImplementedError
        
class TwoWayMap(defaultdict):
    '''
    Credits:
    https://stackoverflow.com/questions/3318625/efficient-bidirectional-hash-table-in-python
    '''
    
    def __init__(self, *args, **kwargs):
        super(TwoWayMap, self).__init__(*args, **kwargs)
        self.inverse = {}
        for key, value in self.items():
            self.inverse.setdefault(value,[]).append(key) 

    def __setitem__(self, key, value):
        if key in self:
            self.inverse[self[key]].remove(key) 
        super(TwoWayMap, self).__setitem__(key, value)
        self.inverse.setdefault(value,[]).append(key)        

    def __delitem__(self, key):
        self.inverse.setdefault(self[key],[]).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]: 
            del self.inverse[self[key]]
        super(TwoWayMap, self).__delitem__(key)