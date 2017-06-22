# Converts files into Tables instead of loading them into SQL

from sqlify.readers import yield_table

def text_to_table(file, **kwargs):
    # Load entire text file to Table object
    temp = yield_table(
        file, chunk_size=None, **kwargs)

    return_tbl = None
    
    # Only "looping" once to retrieve the only Table
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl

def csv_to_table(file, **kwargs):
    # Load entire CSV file to Table object
    temp = yield_table(
        file, type='csv', chunk_size=None, **kwargs)
    
    for tbl in temp:
        return_tbl = tbl
        
    return return_tbl
