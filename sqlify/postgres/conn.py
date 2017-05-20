'''
Functions which manage connections to a Postgres database
'''

from sqlify.settings import POSTGRES_DEFAULT_DATABASE, POSTGRES_DEFAULT_USER, \
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