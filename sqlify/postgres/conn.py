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

from sqlify._globals import POSTGRES_CONN_KWARGS
from sqlify.core._core import alias_kwargs
from sqlify.config import PG_DEFAULTS
    
import copy
import functools
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the default Postgres database 
def postgres_connect_default():
    with psycopg2.connect(**PG_DEFAULTS) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        conn = conn.cursor()
        return conn

def postgres_connect(func=None, *args, **kwargs):
    '''
    Makes sure the local variable `conn` in all functions this decorates
    is a usable psycopg2 connection
    '''
       
    def alias_kwargs(kwargs):
        rep_key = {
            # Data file keywords
            'delim': 'delimiter',
            'sep': 'delimiter',
            'separator': 'delimiter',
        
            # Postgres connection keywords
            'db': 'dbname',
            'database': 'dbname',
            'hostname': 'host',
            'username': 'user',
            # 'pass': 'password', -- Can't do that because it's a Python keyword
            'pw': 'password'
        }
        
        # Alias keyword arguments
        for k in set(rep_key.keys()).intersection(kwargs.keys()):
            kwargs[rep_key[k]] = kwargs[k]
            del kwargs[k]

        return kwargs
        
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
    
    # Make this usable as a function or a decorator
    if not callable(func):
        return connect(**PG_DEFAULTS(**alias_kwargs(kwargs)))
    
    @functools.wraps(func)
    def inner(*args, **kwargs):
        new_kwargs = alias_kwargs(kwargs)
    
        try:
            if isinstance(kwargs['conn'], psycopg2.extensions.connection):
                return func(*args, **new_kwargs)
        except KeyError:        
            if set(kwargs.keys()).intersection(POSTGRES_CONN_KWARGS):
                return func(conn=connect(**PG_DEFAULTS(**new_kwargs)),
                    *args, **new_kwargs)
            else:
                raise ValueError("Must either pass in a psycopg2 connection"
                " object, or describe one or more of 'database', 'host',"
                "'user', 'password'.")
                
    return inner