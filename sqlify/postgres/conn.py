'''
Functions which manage connections to a Postgres database
'''

from sqlify.config import POSTGRES_DEFAULT_DATABASE, POSTGRES_DEFAULT_USER, \
    POSTGRES_DEFAULT_HOST, POSTGRES_DEFAULT_PASSWORD
    
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the default Postgres database 
def postgres_connect_default():
    with psycopg2.connect(
        dbname=POSTGRES_DEFAULT_DATABASE,
        user=POSTGRES_DEFAULT_USER,
        host=POSTGRES_DEFAULT_HOST,
        password=POSTGRES_DEFAULT_PASSWORD) as conn:
        
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        conn = conn.cursor()
        
        return conn
        
def postgres_connect(database=None, username=None, password=None, host=None):
    ''' Returns a Postgres connection '''

    if not database:
        database = POSTGRES_DEFAULT_DATABASE
    if not username:
        username = POSTGRES_DEFAULT_USER
    if not password:
        password = POSTGRES_DEFAULT_PASSWORD
    if not host:
        host = POSTGRES_DEFAULT_HOST
        
    try:
        with psycopg2.connect("dbname={0} user={1} password={2} host={3}".format(
            database, username, password, host)) as conn:
            return conn
    except psycopg2.OperationalError:
        # Database doesn't exist --> Create it    
        base_conn = postgres_connect_default()
        base_conn.execute('CREATE DATABASE {0}'.format(database))
        
        with psycopg2.connect("dbname={0} user={1} password={2}".format(
            database, username, password)) as conn:
            return conn