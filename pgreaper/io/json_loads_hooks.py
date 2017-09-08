''' Functions for Processing JSON Objects '''

from functools import wraps
from collections import defaultdict, deque
    
__all__ = ['filter_keys', 'flatten_dict']
    
def lower_keys(func):
    ''' Lowercase all the keys in a JSON dict '''
    
    @wraps(func)
    def inner(dict_, *args, **kwargs):
        return func({ k.lower(): v for k, v in dict_.items() },
                    *args, **kwargs)
    return inner
    
@lower_keys
def filter_keys(dict_, eval_keys):
    '''
    Given a JSON dict return a list of values by calling eval()
    on each statement in eval_keys
    
    Example:
    >>> j = {
    >>>     "key1": "val1",
    >>>     "key2": "val2",
    >>>     "key3": {
    >>>         "key3a": "val3a",
    >>>         "key3b": "val3b"
    >>>    }
    >>> filter_keys(j, eval_keys=["dict_[key1]": "dict_[key3][key3a]"])
    >>> ["val1", "val3a"]    
    '''
    
    row = []
    
    for col in eval_keys:
        try:
            row.append(eval(col))
        except (KeyError, IndexError):
            row.append(None)
            
    return row
                
def flatten_dict(d):
    ''' Completely flatten a dict using an iterative algorithm '''

    new_d = {}
    nested_d = deque()
    nested_d.append(d)
    
    while nested_d:
        for k, v in nested_d.pop().items():
            if isinstance(v, dict):
                nested_d.append(
                    {'{}.{}'.format(k, k2): v2 for k2, v2 in
                v.items() }
                )
            else:
                new_d[k] = v
        
    return new_d