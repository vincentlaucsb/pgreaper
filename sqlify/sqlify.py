from sqlify._sqlify import sanitize_table, preprocess

# Public Interface
from sqlify.config import settings
from sqlify.sqlite.loader import file_to_sqlite, table_to_sqlite
from sqlify.sqlite.to_postgres import sqlite_to_postgres
from sqlify.postgres.loader import file_to_pg, table_to_pg

# Table API
from sqlify.table.converters import table_to_csv, table_to_json

# HTML API
from sqlify.html.parser import get_tables_from_file, get_tables_from_url

# Advanced Public Interface
from sqlify.table import Table
from sqlify.postgres.table import PgTable

# SQLite uploaders
def text_to_sqlite(*args, **kwargs):
    file_to_sqlite(type='text', *args, **kwargs)
    
def csv_to_sqlite(*args, **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','

    file_to_sqlite(type='csv', *args, **kwargs)

# PostgreSQL uploaders
def text_to_pg(*args, **kwargs):
    file_to_pg(type='text', *args, **kwargs)
    
def csv_to_pg(*args, **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = ','
        
    file_to_pg(type='csv', *args, **kwargs)