'''
Functions which manage connections to a Postgres database

Postgres Function Argument Structure
======================================

Argument structure for any function that needs to connect to a Postgres database
 * Keyword Argument: conn=None
  * If not None, can be:
   * psycopg2 connection
   * sqlalchemy engine
 * Other arguments: **kwargs
  * database, username, password, host
  
Function can always expect the local variable `conn` to be a psycopg2 connection.
Decorators will be implemented to ensure this.
  
User can:
 * Specify just conn
 * Specify no arguments --> Use defaults
 * Specify one or all of the Postgres connection arguments, e.g. database
  * Fill in rest with defaults
  
However, conn is mutually exclusive with other arguments
'''

from sqlify.config import PG_DEFAULTS
    
from inspect import signature
import copy
import functools
import psycopg2

# Connect to the default Postgres database 
def postgres_connect_default():
    with psycopg2.connect(**PG_DEFAULTS) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn = conn.cursor()
        return conn

def postgres_connect(func):
    '''
    Makes sure the local variable `conn` in all functions this decorates
    is a usable psycopg2 connection
    '''
       
    # Keyword arguments which indicate user wants to connect to a Postgres database
    pg_conn_args = set(['dbname', 'user', 'password', 'host'])
       
    def connect(dbname, user, password, host):
        try:
            with psycopg2.connect(
                "dbname={0} user={1} password={2} host={3}".format(
                dbname, user, password, host)) as conn:
                return conn
        except psycopg2.OperationalError:
            # Database doesn't exist --> Create it
            base_conn = postgres_connect_default()
            base_conn.execute('CREATE DATABASE {0}'.format(dbname))
            
            with psycopg2.connect(
                "dbname={0} user={1} password={2}".format(
                dbname, user, password)) as conn:
                return conn
    
    @functools.wraps(func)
    def inner(*args, **kwargs):       
        # Inspect Arguments
        f_args = signature(func).bind(*args, **kwargs)
        f_args.apply_defaults()
        conn_arg = f_args.arguments['conn']
    
        if isinstance(conn_arg, psycopg2.extensions.connection):
            return func(*args, **kwargs)
        else:
            if set(kwargs.keys()).intersection(pg_conn_args):
                return func(conn=connect(**PG_DEFAULTS(**kwargs)),
                    *args, **kwargs)
            else:
                raise ValueError("Must either pass in a psycopg2 connection"
                " object, or describe one or more of 'database', 'host',"
                "'user', 'password'.")
  
    return inner