from sqlify.postgres.conn import postgres_connect_default
from sqlify.settings import POSTGRES_DEFAULT_USER, POSTGRES_DEFAULT_PASSWORD
from sqlify.helpers import _sanitize_table

import psycopg2
import os

# Used for previous method
# from psycopg2.extras import execute_batch

def file_to_postgres(
    obj,
    file,
    database,
    type,
    header=None,
    username=None,
    password=None,
    skip_lines=None,
    *args,
    **kwargs):
    
    '''
    Connect to default Postgres database to create a new database using
    the COPY command from a single file
    
    Arguments:
     * obj:         A Table object
     * file:        The original file
     * database:    The Postgres database to store to
     * type:        'text' or 'csv'
     * header:      Line number of the header (zero-indexed)
     * ...:         Self-explanatory
    '''
    
    base_conn = postgres_connect_default()
    
    # import pdb; pdb.set_trace()
    
    # Sanitize column names
    _sanitize_table(obj)
    
    # Temporary: Use 'TEXT' for all column values to avoid errors
    obj.col_types = ['TEXT' for i in enumerate(obj.col_names)]
    
    # Create database if not exists
    try:
        base_conn.execute('CREATE DATABASE {0}'.format(database))
    except psycopg2.ProgrammingError:
        pass  # Database already exists --> ignore

    if not username:
        username = POSTGRES_DEFAULT_USER
    if not password:
        password = POSTGRES_DEFAULT_PASSWORD
    
    # Use a context manager to auto-rollback changes if something bad happens
    with psycopg2.connect("dbname={0} user={1} password={2}".format(
        database, username, password)) as conn:
        
        cur = conn.cursor()
    
        # Create the table
        table_name = obj.name
        num_cols = len(obj.col_names)
        
        # cols = [(column name, column type), ..., (column name, column type)]
        cols_zip = zip(obj.col_names, obj.col_types)
        cols = []
        
        for name, col_type in cols_zip:
            cols.append("{0} {1}".format(name, col_type))
        
        create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
            table_name, ", ".join(cols))
        
        cur.execute(create_table)
        
        ''' Previous Method: Use prepared statement '''
        
        # # Prepare
        # cur.execute('''
            # PREPARE massinsert ({col_types}) AS
                # INSERT INTO {table_name} VALUES({values});
            # '''.format(
                # table_name = table_name,
                # col_types = ",".join(i.replace(' PRIMARY KEY', '') for i in obj.col_types),
                # values = ",".join(['$' + str(i) for i in range(1, num_cols + 1)])
                # )
        # )
        # # Insert columns
        # insert_into = "EXECUTE massinsert({values})".format(
            # values=",".join(['%s' for i in range(0, num_cols)]))
        
        # execute_batch(cur, insert_into, obj)
        
        # # Remove prepared statement
        # cur.execute("DEALLOCATE massinsert")
        
        '''
        Faster Method:
         1. Use the COPY command to copy the file
         2. Delete unneeded rows later
        '''
        
        # Load files into database
        if type == 'text':
            loader_func = text_to_postgres
        elif type == 'csv':
            loader_func = csv_to_postgres
                
        loader_func(cur=cur, obj=obj, file=file, table=table_name,
            header=header, *args, **kwargs)
    
        # Delete rows
        if ((skip_lines is not None) and (skip_lines > 0)) \
            or ((type == 'text') and (header is not None)):
            
            # Need a Table object
            delete_rows(obj=obj, cur=cur, header=header, skip_lines=skip_lines,
                        type=type)
    
        conn.commit()
        cur.close()
        
def text_to_postgres(cur, obj, file, table, header, delimiter=' ', *args, **kwargs):
    '''
    Helper function for file_to_postgres
    
    Arguments:
     * cur:         A psycopg2 Cursor object
     * obj:         A Table object
     * file:        Name of the original file
     * table:       Name of the SQL table to save to
     * header:      Line number of the header
     * delimiter:   How the data is separated
    '''
    
    '''
    Dealing with headers
     * There is no HEADER option for text files
     * Therefore, insert all rows and delete header row after
    '''
    
    # How are missing values encoded?
    if 'na_values' in kwargs:
        na_values = 'NULL {0},'.format(kwargs['na_values'])
    else:
        na_values = ""
    
    # Copy the file
    path_to_file = os.getcwd()
    
    new_path = path_to_file
    if '/' in file:
        for i in file.split('/'):
            new_path = os.path.join(new_path, i)
    
    cur.execute('''
            COPY {tbl_name}
                FROM '{file_name}'
                (FORMAT text,
                 DELIMITER '{delim}',
                 {na_values}
                 ENCODING 'ISO-8859-9')
        '''.format(
                tbl_name=obj.name,
                file_name=new_path,
                delim=delimiter,
                na_values=na_values
            )
        )
        
    # Drop header row (if there was one)
    # if header:
    # cur.execute('''  ''')
    
def csv_to_postgres(cur, obj, file, table, header, delimiter=',', *args, **kwargs):
    ''' 
    Helper function for file_to_postgres()
    
    Arguments: See text_to_postgres()
    '''
    
    # Is there a header row?
    if header == 0:
        header_option = "HEADER,"
    else:
        '''
        In this case, either:
         (a) There is no header row
         (b) The header row is not the first row
         
        For (a), do nothing.
        For (b), insert all rows and delete header row later. (implement later)
        '''
        header_option = ""
        
    # How are missing values encoded?
    if 'na_values' in kwargs:
        na_values = 'NULL {0},'.format(kwargs['na_values'])
    else:
        na_values = ""
    
    # Copy the file
    path_to_file = os.getcwd()
    
    new_path = path_to_file
    if '/' in file:
        for i in file.split('/'):
            new_path = os.path.join(new_path, i)
    
    # Encoding is a temporary fix
    
    sql_command = '''
        COPY {tbl_name} FROM '{file_name}'
        (FORMAT csv, 
         DELIMITER '{delim}', {na_values} {header}
         ENCODING 'ISO-8859-9'
        )'''.format(
                tbl_name=table,
                file_name=new_path,
                delim=delimiter,
                header=header_option,
                na_values=na_values
            )
    
    cur.execute(sql_command)
    
def delete_rows(obj, cur, type, header, skip_lines):
    '''
    Goal: Delete some of the first few rows from an SQL table
     
    Arguments:
     * obj:         A Table object
     * cur:         A psycopg2 cursor
     * type:        'text' or 'csv'
     * skip_rows:   How many rows to delete
    '''
    
    # import pdb; pdb.set_trace()
    
    if header is not None:
        # skip_lines counts the header row, but neither obj or SQL table contain it
        del_rows = list(range(0, skip_lines - 1))
    else:
        del_rows = list(range(0, skip_lines))
        
    del_rows_text = [obj[i] for i in del_rows]
    
    # Have to manually remove header for 'FORMAT text'
    if (header is not None) and (type == 'text'):
        del_rows_text.append(obj.col_names)
    
    # Delete rows one-by-one
    for row in del_rows_text:
        sql_delete_query = '''
            DELETE FROM {tbl_name}
            WHERE {where_clause};
            '''.format(
                tbl_name = obj.name,
                where_clause = " AND ".join(
                    "{col_name} LIKE '{col_text}'".format(
                        col_name=obj.col_names[i],
                        col_text=text) \
                    for i, text in enumerate(row)))
        
        # pdb.set_trace()
    
        cur.execute(sql_delete_query)