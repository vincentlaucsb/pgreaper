from sqlify.helpers import _sanitize_table, _preprocess

# Public Interface
from sqlify.config import settings
from sqlify.sqlite.loader import file_to_sqlite, table_to_sqlite
from sqlify.sqlite.to_postgres import sqlite_to_postgres
from sqlify.postgres.loader import file_to_pg, table_to_pg

# SQLite uploaders
def text_to_sqlite(*args, **kwargs):
    file_to_sqlite(type='text', *args, **kwargs)
    
def csv_to_sqlite(*args, **kwargs):
    file_to_sqlite(type='csv', *args, **kwargs)

# PostgreSQL uploaders
def text_to_pg(*args, **kwargs):
    file_to_pg(type='text', *args, **kwargs)
    
def csv_to_pg(*args, **kwargs):
    file_to_pg(type='csv', *args, **kwargs)