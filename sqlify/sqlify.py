from .helpers import _sanitize_table, csv_to_table, text_to_table
from .table import Table, subset, type_check

import sqlite3

# Store a Table object in SQL database
def table_to_sql(obj, database, name='', **kwargs):
    # Create multiple tables based on name dictionary
    if isinstance(name, dict):
        for table_name, columns in zip(name.keys(), name.values()):
            if isinstance(columns, list):
                new_tbl = subset(obj, *columns, name=table_name)
            else:
                new_tbl = subset(obj, columns, name=table_name)
            
            _sanitize_table(new_tbl)
            single_table_to_sql(obj=new_tbl, database=database, **kwargs)

    else:
        _sanitize_table(obj)
        single_table_to_sql(obj, database, **kwargs)

# Store a single Table object in SQL database
def single_table_to_sql(obj, database, check_type=False):
    '''
    Arguments:
     * obj:        Table object
     * database:   Database file
     * check_type: Print out warnings if data entries do not match
                   column type
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
    
    create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table_name, ", ".join(cols))
    
    conn.execute(create_table)    
    
    # Insert columns
    insert_into = "INSERT INTO {0} VALUES ({1})".format(
        table_name, ",".join(['?' for i in range(0, num_cols)]))

    conn.executemany(insert_into, obj)
    
    conn.commit()
    conn.close()
    
    if check_type:
        type_check(obj)

# Convert text file to SQL
def text_to_sql(file, database, name='', delimiter='', header=True, low_memory=False, **kwargs):
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
    
    if delimiter == '\\t':
        delimiter = '\t'
    
    # import pdb; pdb.set_trace()
    
    if low_memory:
        pass
    
    else:
        tbl = text_to_table(file=file, name=name, delimiter=delimiter, header=header, **kwargs)
       
        table_to_sql(obj=tbl, database=database, name=name)

def big_ass_txt_to_sql(file, database, headers=[], header=True, name='', delimiter='', **kwargs):
    # headers: List of column names
    
    # Split one line according to delimiter
    def split_line(line):
        line = line.replace('\n', '')
    
        if delimiter:
            line = line.split(delimiter)
        
        return line
    
    with open(file, 'r') as infile:
        line_num = 0
        row_values = []
        
        for line in infile:
            # Header reading not implemented
            if header and (line_num == 0):
                col_names = split_line(line)
            
            else:
                row_values.append(split_line(line))
            
                # At 10,000 lines: Save and dump values
                if line_num % 10000 == 0:
                    if headers:
                        col_names = headers
                
                    tbl = Table(name, col_names=col_names, row_values=row_values, **kwargs)
                    tbl
                    
                    single_table_to_sql(tbl, database=database, check_type=False)
          
                    del tbl
                    row_values = []
                
            line_num += 1
            
        # End of loop --> Dump remaining data
        if row_values:
            if headers:
                col_names = headers
        
            tbl = Table(name, col_names=col_names, row_values=row_values, **kwargs)
            tbl
            
            single_table_to_sql(tbl, database=database, check_type=False)
        
# Convert CSV file to SQL
def csv_to_sql(file, database, name='', delimiter=',', skip_lines=0, header=True, **kwargs):
    if delimiter == '\\t':
        delimiter = '\t'
        
    #import pdb; pdb.set_trace()
    
    tbl = csv_to_table(file=file, name=name, delimiter=delimiter, header=header, skip_lines=skip_lines, **kwargs)

    table_to_sql(obj=tbl, database=database, name=name)