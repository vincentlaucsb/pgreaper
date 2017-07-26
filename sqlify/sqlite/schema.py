import sqlite3

def table_exists(database, table, engine=None):
    ''' Return True or False if a table exists '''

    if engine:
        conn = engine.connect()
        conn = conn.connection
    else:
        conn = sqlite3.connect(database)
        
    return conn.execute('''
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='table_name';
    ''')