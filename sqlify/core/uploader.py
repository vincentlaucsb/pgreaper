''' A class for handling complex uploading procedures '''

from abc import ABCMeta, abstractmethod

from sqlify.core._core import sanitize_names2
from sqlify.postgres.database import DBPostgres
from .dbapi import DBSQLite

class SQLUploader(metaclass=ABCMeta):
    '''
    Represents (generally) one transactional block
     * Handles uploading data to one table
     * May gather necessary table metadata before loading
     
    Tables should be loaded by manually calling load()
     * This class will automatically determine whether to use copy or upsert
    '''
    
    def __init__(self, conn, table, dialect, name,
        on_p_key=None, add_cols=False, reorder=False):
        '''
        Parameters
        -----------
         conn:    
                    DBAPI connection
         table:   
                    A Table object (doesn't need to be the whole thing)
         dialect:   DBDialect object
         transact:  boolean
                    Is this a transaction uploading multiple tables
         name:      str
                    Name of the SQL table
         on_p_key:  str, list, or None
                    What to do when a row to be inserted has the same primary key
                    as a row in the database
                     * None      --> Do nothing
                     * 'expand'  --> Only fill in columns that didn't exist before
                     * 'replace' --> INSERT or REPLACE
                     * list      --> UPSERT values for columns in list
         add_cols:  boolean
                    Is uploader allowed to expand input (by adding NULLs) or
                    expand SQL table (using ALTER TABLE) to get the job done
         reorder:   boolean
                    Is uploader allowed to rearrange input columns to match destination
                    Implied True if add_cols is True
        '''
        
        self.dialect = dialect
        self.conn = conn
        
        # Sanitize table names
        self.dialect.sanitize_names(table)
        
        # Table Metadata
        self.name = name
        self.col_names = table.col_names
        self.col_types = table.col_types
        self.p_key = self.p_key()
        self.on_p_key = on_p_key
        
        sql_schema = self.get_schema(table)
        
        try:
            # table_exists and new_table need to be separate variables
            # when uploading multiple tables to one that didn't previously exist
            self.table_exists = True
            self.new_table = False
            sql_cols = sql_schema.col_names
            sql_types = sql_schema.col_types
        except AttributeError:
            # Table DNE
            self.table_exists = False
            self.new_table = True
        
        if self.table_exists:
            modify = self.widen(add_cols, table.col_names, table.col_types,
                sql_cols, sql_types, reorder=reorder)
            if modify is None:
                conn.close()
                raise ValueError('Schema mismatch. Aborting upload.')
            else:
                self.modify = modify
        else:
            # Table doesn't exist --> No need to modify input
            self.modify = []
            
    @staticmethod
    def widen(add_cols, input_cols, input_types, sql_cols, sql_types,
        reorder=False):
        '''
        Given the user's options and current schema, return the column
        and types of the new SQL table
        
        Returns:
         * A list of column name, column type pairs
         * Empty list (no changes needed)
         * None (abort upload)
        '''
        
        input_cols = [i.lower() for i in input_cols]
        input_types = [i.lower() for i in input_types]
        
        if set(input_cols) == set(sql_cols):
            # Case 1: Exactly the same --> All good
            if input_cols == sql_cols:
                return []
            # Case 2: Need to reorder --> Match SQL table
            elif reorder:
                return [(c, t) for c, t in zip(sql_cols, sql_types)]
            else:
                return None
        elif add_cols:
            # Case 1: Input is less wide --> Fill extra cols with nulls
            if set(input_cols).issubset(set(sql_cols)):
                return [(c, t) for c, t in zip(sql_cols, sql_types)]
            # Case 2: Need to widen SQL table or both
            else:
                # Start with columns in SQL table
                cols = [(c, t) for c, t in zip(sql_cols, sql_types)]
                
                # Then add the new ones
                cols += [(c, t) for c, t in zip(input_cols, input_types)
                         if c not in sql_cols]
            
                return cols
        else:
            return None
    
    def add_cols(self, col_names=[], col_types=[]):
        ''' ALTER TABLE to add columns '''
        for name, type in zip(col_names, col_types):
            self.dialect.add_col(name, type)
            
    def create_table(self, table):
        ''' Given a Table object, issue a CREATE TABLE command '''
        statement = self.dialect.create_table(table_name=self.name,
            col_names=self.col_names, col_types=self.col_types)
            
        self.conn.cursor().execute(statement)
        self.table_exists = True
        
    def get_schema(self, table):
        ''' Get schema for table '''
        self.dialect.get_schema(conn=self.conn, table=table)
        
    def p_key(self):
        ''' Return primary key if Table has one or None '''
        return self.dialect.p_key(conn=self.conn, table=self.name)
        
    def commit(self):
        self.conn.commit()
        self.conn.close()
        
    def _modify_table(self, table):
        '''
        Transform the input table if necessary
         * Should be called by _copy() and _upsert()
        '''
        
        table_cols = [(c, t) for c, t in zip(table.col_names, table.col_types)]
        modify_names = [c[0] for c in self.modify]
        
        if table_cols != self.modify:
            # Case 1: Reorder + Expand
            if set(table_cols).issubset(set(self.modify)) and \
                set(table_cols) != set(self.modify):
                new_table = table.reorder(
                    *[c for c in table.col_names if c in modify_names])
                
                for c in [c for c in self.modify if c[0] not in table.col_names]:
                    table.add_col(c[0], col_type=c[1])
                
            # Case 2: Reorder Only
            elif set(table_cols) == set(self.modify):
                new_table = table.reorder(*modify_names)
                
            # Case 3: SQL table, not input table should be modified
            else:
                return table
        else:
            return table
        
    def load(self, table):
        ''' Determine whether to use COPY, UPSERT, or both and do it '''
        if self.new_table:
            if not self.table_exists:
                self.create_table(table)
                
            self._copy(table)
        else:
            self._upsert(table)
    
    @abstractmethod
    def _copy(self, table):
        ''' Upload the Table into the database the fastest way '''
        pass
        
    @abstractmethod
    def _upsert(self, table):
        ''' Upsert the Table '''
        pass