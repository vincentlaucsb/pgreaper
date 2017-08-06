''' Functions with interacting with live PostgreSQL databases '''

from sqlify._globals import SQLIFY_PATH
from sqlify.core._core import alias_kwargs, sanitize_names2
from sqlify.core.dbapi import DBDialect
from sqlify.core.table import Table
from .conn import postgres_connect
from .schema import get_schema, get_pkey

from collections import deque, namedtuple
from psycopg2 import sql
import psycopg2
import os
import sys
import csv

# Load Postgres reserved keywords
with open(os.path.join(
    SQLIFY_PATH, 'data', 'pg_keywords.txt'), mode='r') as PG_KEYWORDS:
    PG_KEYWORDS = set([kw.replace('\n', '').lower() for kw in PG_KEYWORDS.readlines()])

class DBPostgres(DBDialect):
    def __init__(self):
        table_exists = table_exists_pg
        super(DialectPostgres, self).__init__(table_exists)
        
    def __eq__(self, other):
        ''' Make it so self == 'postgres' returns True '''
        if isinstance(other, str):
            if other == 'postgres':
                return True
        else:
            return super(DialectPostgres, self).__eq__(other)
        
    def __repr__(self):
        return "postgres"
        
    def sanitize_names(table):
        ''' Remove bad words and no-nos from column names '''
        sanitize_names2(table, reserved=PG_KEYWORDS)
        
    @staticmethod
    def get_primary_keys(conn, table):
        '''
        Return a set of primary keys from table
        
        Parameters
        -----------
        conn:       psycopg2 connection
        table:      str
                    Name of table       
        '''
    
        pkey_name = get_pkey(table, conn=conn).column
        cur = conn.cursor()
        cur.execute('''SELECT {} FROM {}'''.format(
            pkey_name, table))
        return set([i[0] for i in cur.fetchall()])
        
    @staticmethod
    def p_key(conn, table):
        ''' Return the name of the primary key col of a table '''
        
        try:
            return get_pkey(table, conn=conn).column
        except AttributeError:
            return None
        
    @staticmethod
    def get_schema(conn, table):
        '''
        Get table schema
        
        Return a named tuple with
         * col_names:   List of column names
         * col_types:   List of column types
        '''
        
        schema = namedtuple('Schema', ['col_names', 'col_types'])
        
        try:
            sql_schema = get_schema(conn=conn).groupby('Table Name')
            return schema(
                col_names=sql_schema['Column Name'],
                col_types=sql_schema['Column Type'])
        
        # Table does not exist
        except KeyError:
            return None
        
    @staticmethod
    def create_table(table_name, col_names, col_types):
        # cols_zip = [(column name, column type), ..., (column name, column type)]
        cols = ["{0} {1}".format(col_name, type) for col_name, type in zip(col_names, col_types)]
        
        return "CREATE TABLE IF NOT EXISTS {0} ({1})".format(
            table_name, ", ".join(cols))

@postgres_connect
def pg_to_csv(name, file=None, verbose=True, conn=None, **kwargs):
    ''' Export a Postgres table to CSV '''
    
    if not file:
        file = name + '.csv'
    
    cur = conn.cursor()
    
    with open(file, mode='w') as outfile:
        try:
            # Use this for regular tables
            cur.copy_expert('''
                COPY {} TO STDOUT WITH CSV HEADER
                '''.format(name),
                file=outfile)
        except psycopg2.ProgrammingError:
            # Need to use this form of COPY TO for views
            conn.rollback()
            cur.copy_expert('''
                COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER
                '''.format(name),
                file=outfile)
        
    if verbose:
        print('Done exporting {} to {}.'.format(name, file))
        
# class BulkRunner(object):
    # ''' Mass executes SQL statements that have the same general structure '''
    
    # def __init__(self, sql, union=False,
        # database=None, username=None, password=None, host=None):
        # '''
         # * sql:      A SQL statement with Python string formatting tokens
         # * union:    Create a single statement with multiple UNION clauses
        # '''
        
        # self.sql = sql + "\n"
        
        # # Stores database connection settings
        # self.db = {
            # 'database': database,
            # 'username': username,
            # 'password': password,
            # 'host': host
        # }
        
        # self.queue = []
        
    # def load(self, *args, **kwargs):
        # self.queue.append({
            # 'args': args,
            # 'kwargs': kwargs
        # })
        
    # def create_query(self):
        # ''' Create a SQL query '''
        # sql_query = "UNION\n".join(self.sql.format(*args['args'], **args['kwargs']) for args in self.queue)

        # return sql_query
        
    # def print_query(self):
        # ''' Print the SQL query generated '''
        
        # print(self.create_query())
        
# def agg_database(list_, *args):
    # '''
    # Generate a query that aggregates many columns from many tables in a database
    
    # Arguments:
     # * list_:     A list of columns to apply aggregate functions to
        # * Should be in this format
        
        # +------------------+----------------------------+
        # | table name       | column name                |
        # +------------------+----------------------------+
        
     # * args:     List of aggregate functions to apply
    # '''
    
    # select_queries = []
    
    # for table, col in list_:
        # # Expand list of aggregate functions
        # agg_part = ", ".join(['{func}({col})'.format(
            # func=f, col=col) for f in args])
        
        # # Add select query    
        # select_queries.append("SELECT {0} FROM {1}\n\tGROUP BY {2}".format(
            # agg_part, table, col))
    
    # return '\nUNION\n'.join(select_queries)
    
