'''
Functions for operating on existing SQLite databases
'''

from sqlify.table import Table

from sqlite3 import PARSE_COLNAMES
import sqlite3 as sql

# A dictionary of registered functions
FUNCTIONS = {}

# Temporary: For debugging
def test(value):
    return str(value).upper()
    
# class QueryTable(list):
    # ''' A table that lazy loads the results of SQL queries
    
    # Arguments:
     # * table:   SQLTable object
     # * query:   SQL query to execute
    # '''
    # def __init__(self, table, query):
        # super(list, self).__init__()
        
        # self.table = table
        # self.query = query
    
def _sql_table(database):
    with sql.connect(database, detect_types=PARSE_COLNAMES) as conn:
        return conn
    
class SQLTable:
    ''' Manages access to a SQL table '''
    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.conn = _sql_table(database)
    
    def get_pkey(self):
        # Get the primary key column of a table
    
    def _mutate(self, **kwargs):
        col_names = kwargs.keys()
        
        # SELECT func(col1),func(col2),func(col3) FROM ... 
        select_what = ",".join(kwargs.values())
        
        function_names = set()
        
        # Parse function names
        for func_call in kwargs.values():
            function_name = func_call.split('(')[0]
            function_names.add(function_name)
            
        # Create functions
        for func in function_names:
            self.conn.create_function(func, narg=1, func=FUNCTIONS[func])
    
        results = self.conn.execute(
            "SELECT {select_what} FROM {table}".format(
                select_what=select_what,
                table=self.table))

        # Temporary: What if each row in results has more than one entry?
        row_values = [i[0] for i in results]
                
        '''
        Example of results.description for one column
        
        >>> results.description
        (('test(LOCATION)', None, None, None, None, None, None),)
        '''        

        return Table(name="mutation",
                    col_names=list(kwargs.keys()),
                    row_values=row_values)
    
    def preview_mutate(self, print_rows=10, **kwargs):
        ''' Preview the results of a mutate command before it is executed
    
        Arguments:
         * print_rows:  Number of rows to print
         * kwargs:      A dictionary of column names to function calls
                        (Function calls should be quoted)
        '''
        
        return self._mutate(**kwargs)
    
    def mutate(self, **kwargs):
        ''' Commit the results of a mutatation to the database '''
        
        # results is a Table object
        results = self._mutate(**kwargs)
        num_cols = len(results.col_names)
        
        # Temporary: Assuming one column
        # Allocate another column
        self.conn.execute(
            '''ALTER TABLE {table_name} 
               ADD {col_name} {data_type}
            '''.format(
            table_name=self.table,
            col_name=results.col_names[0],
            data_type=results.col_types[0]))
            
        # Insert new values
        insert_into = "INSERT INTO {table_name} VALUES ({values})".format(
            table_name=self.table, 
            values=",".join(['?' for i in range(0, num_cols)]))
        
        self.conn.executemany(insert_into, results)
            
        self.conn.commit()
    
def preview_mutate(table, *args, **kwargs):
    ''' Preview the results of a mutate command before it is executed
    
    Arguments:
     * table:       An SQLTable Object
     * print_rows:  Number of rows to print
     * kwargs:      A dictionary of column names to function calls
                    (Function calls should be quoted)
    '''
    
    return table.preview_mutate(*args, **kwargs)
    
def mutate(table, *args, **kwargs):
    return table.mutate(*args, **kwargs)
    
def register(func):
    global FUNCTIONS
    FUNCTIONS[func.__name__] = func