from .helpers import _sanitize_table, csv_to_table, text_to_table
from .table import Table, subset

import sqlite3

# Store a Table object in SQL database
def table_to_sql(obj, database, name=''):
    # Create multiple tables based on name dictionary
    if isinstance(name, dict):
        for table_name, columns in zip(name.keys(), name.values()):
            if isinstance(columns, list):
                new_tbl = subset(obj, *columns, name=table_name)
            else:
                new_tbl = subset(obj, columns, name=table_name)
            
            _sanitize_table(new_tbl)
            single_table_to_sql(obj=new_tbl, database=database)

    else:
        _sanitize_table(obj)
        single_table_to_sql(obj, database)

# Store a single Table object in SQL database
def single_table_to_sql(obj, database):
    '''
    Arguments:
     * obj:      Table object
     * database: Database file
    '''
    
    conn = sqlite3.connect(database)
    
    # Create the table
    table_name = obj.name
    num_cols = len(obj.col_names)
    
    # cols = [(column name, column type), ..., (column name, column type)]
    cols_zip = zip(obj.col_names, obj.col_types)
    cols = []
    
    for name, type in cols_zip:
        cols.append("{0} {1}".format(name, type))
    
    create_table = "CREATE TABLE {0} ({1})".format(table_name, ", ".join(cols))
    
    # import pdb; pdb.set_trace()
    
    conn.execute(create_table)
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))
    
    import pdb; pdb.set_trace()
    conn.executemany(insert_into, obj)
    
    conn.commit()
    conn.close()

# Convert text file to SQL
def text_to_sql(file, database, name='', delimiter='', header=True, **kwargs):
    '''
    Arguments:
     * file:      Data file
     * database:  sqlite3 database to store in
     * name:      Name of the SQL table (default: name of the file)
      * Alternatively, name can be a dictionary where keys are table names and values are column names or indices
     * p_key:     Specifies column index to be used as a used as a primary
                  key for all tables
     * header:    True if first row is a row of column names
     * delimiter: Delimiter of the data
     * col_types: Column types
    '''
    
    tbl = text_to_table(file=file, name=name, delimiter=delimiter, header=header, **kwargs)
   
    table_to_sql(obj=tbl, database=database, name=name)

# Convert CSV file to SQL
def csv_to_sql(file, database, name='', delimiter=',', skip_lines=0, header=True, **kwargs):
    import pdb; pdb.set_trace()
    tbl = csv_to_table(file=file, name=name, delimiter=delimiter, header=header, skip_lines=skip_lines, **kwargs)

    table_to_sql(obj=tbl, database=database, name=name)