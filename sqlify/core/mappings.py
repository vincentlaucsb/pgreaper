''' General Purpose Dict-Based Classes '''

from itertools import chain
from collections import defaultdict

class RightUnionDict(dict):
    '''
    A dictionary where an "additional" operation produces a new 
    dictionary with a union of the two dicts' keys and values
     - For conflicting values, the values of the RIGHT are preserved
     - The RIGHT side can be seen as "updating" the left side
    
    {1: apples, 2: bananas}
    + {1: berries, 3: watermelons}
    = {1: apples, 2: bananas, 3: watermelons}
    '''

    def __add__(self, other):
        new_dict = {}
    
        for k, v in chain(
            zip(other.keys(), other.values()),
            zip(self.keys(), self.values())):
            if k not in new_dict:
                # Because RIGHT side was added first, duplicates are from
                # LEFT and should be dropped
                new_dict[k] = v
                
            return new_dict
                
class SymmetricIndex(dict):
    '''    
    Let f be this index, and a, b be any keys or values. Then,
    f[a][b] = f[b][a]
    
    Motivation
    -----------
    Suppose this index returns the data type that makes two disparate
    types compatible, then an example would be
    
    postgres_type['integer']['double precision'] = 'double precision'
    postgres_type['double precision']['integer'] = 'double precision'
    
    Usage
    ------
    - Only one insert or update needs to be performed for any pair of keys
    - This implementation handles the rest
    - Deleting keys is currently not implemented (or necessary yet)
    
    Caveats
    --------
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
            
            super(SymmetricIndex.Node, self).__init__(str)
            
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
        # Assuming value is some dict
        super(SymmetricIndex, self).__setitem__(key,
            self.Node(parent=self, preimage=key, dict_=value))
            
    def __delitem__(self, key):
        raise NotImplementedError