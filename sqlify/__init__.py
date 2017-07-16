'''
What is SQLify?
----------------
SQLify is a tool written in Python that allows for easy conversion of common data sources, like text files and comma-separated values (CSV) into SQL databases. Currently, SQLite and PostgreSQL and supported.

https://github.com/vincentlaucsb/sqlify

Dependencies
~~~~~~~~~~~~~
SQLify works best with the latest versions of Python 3, i.e. 3.5 and 3.6. But aside from that, it was developed to rely on as few outside packages as possible.

* **Required:**
   * `psycopg2` for PostgreSQL support
* **Optional:**
   * `requests` for grabbing HTML documents from the web
   * `pandas` for converting `Table` objects to pandas DataFrames

Capabilities
--------------

Support for now and the forseeable future is provided for SQLite (via the standard library `sqlite3`) and PostgreSQL (via `psycopg2`).

File-Based Formats to SQL
~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLify supports uploading from any of the file formats to a SQL database:
 * Text files (.TXT) with any delimiter
 * CSV files (.CSV), although the reader can be used for other similar delimiter-separated types
 * HTML files
    * A semi-automated HTML `<table>` parser is included
    * A module to serialize HTML files and store them in Postgres as `jsonb` is in the works
    
Programming with SQLify
~~~~~~~~~~~~~~~~~~~~~~~~~
SQLify provides a two-dimensional data structure called `Table` which is 
built off of Python's standard `list`.

 * Used internally for uploading operations
 * Can be created and modified by the user for their own purposes
 
Fast, Flexible, and Robust
~~~~~~~~~~~~~~~~~~~~~~~~~~~
SQLify uses the fastest possible methods to upload files, i.e. mass inserts for SQLite and COPY for Postgres. In addition, it also wraps these features in a way which simplifies and extends them.

 * Files with or without headers can be uploaded
    * Extraneous leading rows can be skipped with a `skip_lines` argument
 * Automatic parsing of column names and types is supported
 * For Postgres, rejected rows are sent to another table rather than aborting the COPY operation
'''

# Public API
from .config import settings
from .core import text_to_table, csv_to_table, table_to_csv, table_to_json, \
    Table, PgTable
from .html import from_file, from_url
from .text import *
from .sqlite import table_to_sqlite, sqlite_to_postgres
from .postgres import table_to_pg