# Public API
from .config import settings, PG_DEFAULTS
from .core import read_text, read_csv, read_json, \
    table_to_csv, table_to_json, table_to_html, table_to_md
from .core._core import strip
from .core.table import Table
from .html import from_file, from_url
from .text_to_sql import *
from .text_to_text import *
from .pandas import *
from .zip import read_zip
from .sqlite import table_to_sqlite, sqlite_to_postgres

# Make SQLAlchemy Extension Module Optional
try:
    from .alchemy import *
except ImportError:
    pass

import sqlify.postgres
from .postgres import table_to_pg, pg_to_csv