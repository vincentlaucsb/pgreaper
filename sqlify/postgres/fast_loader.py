from sqlify.postgres.conn import postgres_connect_default
from sqlify.settings import POSTGRES_DEFAULT_USER, POSTGRES_DEFAULT_PASSWORD
from sqlify.helpers import _sanitize_table
from sqlify.readers import head_table

import psycopg2
import os

'''
General (Fast) method for loading files
 1. Use the COPY command to copy the file
 2. Delete unneeded rows later
'''

class PgLoader:
    '''
    Loads data into a Postgres database when `load_data()` method is called
    
    Initialization:
     * Connect to default Postgres database to create a new database using
       the COPY command from a single file
    
    Arguments:
     * obj:         A Table object
     * file:        The original file
     * database:    The Postgres database to store to
     * type:        'text' or 'csv'
     * header:      Line number of the header (zero-indexed)
     * ...:         Self-explanatory
    '''
    
    def __init__(self,
        file, database, delimiter, type,
        username=None, password=None,
        header=None, skip_lines=None, na_values=None,
        *args, **kwargs):
       
        '''
        Read the first few lines of the file to get the header names 
        and other useful data
        '''
        
        self.file = file
        self.file_path = self._path_to_file(file)
        self.delimiter = delimiter
        self.head = head_table(
            file=file, type=type, delimiter=delimiter, header=header,
            skip_lines=skip_lines, engine='postgres',
            *args, **kwargs)
        self.header = header
        self.skip_lines = skip_lines
        
        # Create some aliases
        self.table_name = self.head.name
        self.col_names = self.head.col_names
        self.col_types = self.head.col_types
        
        # Sanitize column names
        _sanitize_table(self.head)
        
        # Create database if not exists
        base_conn = postgres_connect_default()
        
        try:
            base_conn.execute('CREATE DATABASE {0}'.format(database))
        except psycopg2.ProgrammingError:
            pass  # Database already exists --> ignore

        # Connect to the database
        if not username:
            username = POSTGRES_DEFAULT_USER
        if not password:
            password = POSTGRES_DEFAULT_PASSWORD
            
        # Set self.cur and self.conn
        self._connect(database, username, password)
    
        ''' Some shared SQL query parts '''

        # Dealing with NULL or NA values
        if na_values:
            self.na_values = "NULL '{0}',".format(na_values)
        else:
            self.na_values = ""
    
    def _path_to_file(self, file):
        ''' Get the absolute path to a file '''
        path_to_file = os.getcwd()
    
        new_path = path_to_file
        if '/' in file:
            for i in file.split('/'):
                new_path = os.path.join(new_path, i)
                
        return new_path
    
    def _connect(self, database, username, password):
        '''
        Connect to the specified database and return a cursor
         * Use a context manager to auto-rollback changes if something
           bad happens
        '''
        
        with psycopg2.connect("dbname={0} user={1} password={2}".format(
            database, username, password)) as conn:
            self.conn = conn
            self.cur = conn.cursor()
    
    def _create_table(self):
        ''' Given a Cursor object, create a table '''
        num_cols = len(self.head.col_names)
        
        # Use 'TEXT' for all column values to avoid errors
        #  -> Try to type-cast after data is loaded
        temp_col_types = ['TEXT' for i in enumerate(self.col_names)]
        
        # cols = [(column name, column type), ..., (column name, column type)]
        cols_zip = zip(self.head.col_names, temp_col_types)
        cols = []
        
        for name, col_type in cols_zip:
            cols.append("{0} {1}".format(name, col_type))
        
        create_table = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
            self.table_name, ", ".join(cols))
            
        self.cur.execute(create_table)
    
    def load_data(self, loader_func):
        ''' Load a file into PostgreSQL by calling loader_func '''
    
        self._create_table()
        
        # Load files into database
        loader_func(self)
    
        # Type cast non-text columns
        self._type_cast()
    
        self.conn.commit()
        self.conn.close()
        
    def delete_rows(self, rows):
        '''
        Delete some of the first few rows from an SQL table
         
        Arguments:
         * rows:    Text of rows to be deleted (list)
        '''
        
        # If rows is just one row, do this to make sure the next 
        # for loop works
        if not isinstance(rows[0], list):
            rows = [rows]            
        
        # Delete rows one-by-one
        for row in rows:
            self.cur.execute('''DELETE FROM
                {tbl_name} WHERE {where_clause}'''.format(
                    tbl_name = self.table_name,
                    where_clause = " AND ".join(
                        "{col_name} LIKE '{col_text}'".format(
                            col_name=self.head.col_names[i],
                            col_text=text) \
                        for i, text in enumerate(row))))
                        
    def _type_cast(self):
        # Create save point
        self.cur.execute('SAVEPOINT sqlify_typecast')
        
        for col_name, col_type in zip(self.col_names, self.col_types):
            if col_type != "TEXT":
                try:
                    self.cur.execute('''
                    ALTER TABLE {tbl_name}
                        ALTER COLUMN {col_name} TYPE {col_type}
                        USING {col_name}::{col_type}
                    '''.format(
                        tbl_name=self.table_name,
                        col_name=col_name,
                        col_type=col_type.replace(' PRIMARY KEY', '')))
                        
                    # Transaction Successfull --> new checkpoint
                    self.cur.execute('RELEASE SAVEPOINT sqlify_typecast')
                    self.cur.execute('SAVEPOINT sqlify_typecast')
                except psycopg2.DataError:
                    print("Could not type cast " + col_name + " to " + col_type)
                    self.cur.execute('ROLLBACK TO SAVEPOINT sqlify_typecast')
        
def text_to_postgres(pgloader):
    '''
    Load text file into Postgres
    
    Arguments:
     * pgloader:    A PgLoader object
    '''
    
    # Copy the file
    pgloader.cur.execute('''
        COPY {tbl_name}
            FROM '{file_name}'
            (FORMAT text, DELIMITER '{delim}', {na_values}
             ENCODING 'ISO-8859-9')
        '''.format(
                tbl_name=pgloader.table_name,
                file_name=pgloader.file_path,
                delim=pgloader.delimiter,
                na_values=pgloader.na_values
            )
        )
    
    # Delete header
    if pgloader.header is not None:
        pgloader.delete_rows(pgloader.head.raw_header)
        
    # Delete skipped lines
    if pgloader.skip_lines:
        pgloader.delete_rows(pgloader.head.raw_skip_lines)
    
def csv_to_postgres(pgloader):
    ''' Load CSV file into Postgres (arguments: See text_to_postgres()) '''
    
    # Is there a header row?
    if pgloader.header == 0:
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
        
    # Copy the file
    # Encoding is a temporary fix
    pgloader.cur.execute('''
        COPY {tbl_name} FROM '{file_name}'
        (FORMAT csv, DELIMITER '{delim}', {na_values} {header}
         ENCODING 'ISO-8859-9'
        )'''.format(
                tbl_name=pgloader.table_name,
                file_name=pgloader.file_path,
                delim=pgloader.delimiter,
                header=header_option,
                na_values=pgloader.na_values
    ))
    
    # Delete rows
    if pgloader.skip_lines:
        pgloader.delete_rows(pgloader.head.raw_skip_lines)