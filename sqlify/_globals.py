''' Global Constants '''

import os

SQLIFY_PATH = os.path.dirname(__file__)

# Keyword arguments which indicate user wants to connect to a Postgres database
POSTGRES_CONN_KWARGS = set(['dbname', 'user', 'password', 'host'])

class Singleton(type):
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]