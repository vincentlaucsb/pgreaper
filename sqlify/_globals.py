''' Global Constants '''

import os

SQLIFY_PATH = os.path.dirname(__file__)

# Keyword arguments which indicate user wants to connect to a Postgres database
POSTGRES_CONN_KWARGS = set(['database', 'username', 'password', 'host'])