# Public API
from ._globals import PGREAPER_PATH
from .config import settings, PG_DEFAULTS
from .core import sample_file, read_text, read_csv, \
    table_to_csv, table_to_json, table_to_html, table_to_md, Table
from .io.json_reader import read_json
from .core._core import strip
from .core.table import Table
from .html import from_file, from_url
from .text_to_sqlite import *
from .text_to_text import *
from .pandas import *
from .zip import read_zip
from .sqlite import table_to_sqlite, sqlite_to_postgres
from .postgres import *

import pgreaper.postgres
import pgreaper.cli