from pgreaper.io.json_reader import JSONStreamingDecoder, flatten_dict
from functools import partial
import json
import timeit
import pgreaper

def load_test():
    load_func = partial(json.loads,
        object_hook=lambda x: [v for k, v in \
            x.items() if k in ['full_name', 'occupation', 'nationality']])

    file = open('persons_nested.json', mode='rb')
    x = JSONStreamingDecoder(source=file,
        loads=load_func)
        
    try:
        rows = [i for i in x]
    except:
        x.source.close()

    tbl = pgreaper.Table(name='test',
        col_names=['full_name', 'occupation', 'nationality'],
        row_values=rows)
       
    return tbl
    
def load_test2():
    load_func = partial(json.loads, object_hook=flatten_dict)

    file = open('persons_nested.json', mode='rb')
    x = JSONStreamingDecoder(source=file,
        loads=load_func)
    tbl = pgreaper.Table(name='test',
        col_names=['full_name', 'occupation', 'nationality'])
        
    for i in x:
        tbl.add_dict(i)
       
    return tbl
    
def load_test3():
    load_func = partial(json.loads)

    file = open('persons_nested.json', mode='rb')
    x = JSONStreamingDecoder(source=file,
        loads=load_func)
    tbl = pgreaper.Table(name='test',
        col_names=['full_name', 'occupation', 'nationality'])
        
    for i in x:
        tbl.add_dict(i)
       
    return tbl
    
print(timeit.timeit(load_test3, globals=globals(), number=1))
x = load_test3()