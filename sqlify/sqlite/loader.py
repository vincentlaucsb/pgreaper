from sqlify.readers import yield_table

import sqlite3

def file_to_sqlite(file, database, type, delimiter, col_types=None, **kwargs):
    ''' Reads a file in separate chunks (to conserve memory) and 
        loads it via mass insert statements '''
    for tbl in yield_table(file=file, type=type, delimiter=delimiter,
    **kwargs):
        if not col_types:
            # Guess column types if not provided
            col_types = tbl.guess_type()
            tbl.col_types = col_types

        table_to_sqlite(obj=tbl, database=database, **kwargs)    

def table_to_sqlite(obj, database, name=None, **kwargs):
    '''
    Notes:
     * Fails if there are blank entries in primary key column
    '''
    
    conn = sqlite3.connect(database)
        
    # Create the table
    if name:
        table_name = name
    else:
        table_name = obj.name
        
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    # import pdb; pdb.set_trace()
    
    # TO DO: Strip "-" from table names
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    conn.execute(create_table)    
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))

    # import pdb; pdb.set_trace()
        
    conn.executemany(insert_into, obj)
    
    conn.commit()
    conn.close()