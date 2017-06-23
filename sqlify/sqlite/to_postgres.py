''' Loads a table from a SQLite database into a Postgres database '''

from sqlify.postgres.table import PgTable
from sqlify.postgres.loader import table_to_sqlite

import sqlite3
import psycopg2

def sqlite_to_pg(sqlite_db, pg_db, name,
    host="localhost", username=None, password=None):
    '''
    Arguments:
     * sqlite_db:   Name of the SQLite database
     * pg_db:       Name of the Postgres database
     * name:        Name of the table
     * host:        Host of the Postgres database
     * username:        Username for Postgres database
     * password:    Password for Postgres database
    '''
    
    # Connect to SQL databases
    sqlite_conn = sqlite3.connect(
        sqlite_db,
        detect_types=sqlite3.PARSE_COLNAMES
    )
    
    pg_conn = postgres_connect(database, username, password, host)
    pg_cur = pg_conn.cursor()
    
    sqlite_data = sqlite_conn.execute("SELECT * FROM {0}".format(name))

    while True:
        data_chunk = PgTable(
            name=name,
            col_names=,
            row_values=sqlite_data.fetchmany())
        
    