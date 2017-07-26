from sqlify.sqlite.schema import table_exists as table_exists_sqlite
from sqlify.postgres.schema import table_exists as table_exists_pg

class SQLDialect(object):
    def __init__(self, table_exists):
        self.table_exists = table_exists
        
class DialectSQLite(SQLDialect):
    def __init__(self):
        table_exists = table_exists_sqlite
        super(DialectSQLite, self).__init__(table_exists)
        
    def __repr__(self):
        return "sqlite"
        
class DialectPostgres(SQLDialect):
    def __init__(self):
        table_exists = table_exists_pg
        super(DialectPostgres, self).__init__(table_exists)
        
    def __repr__(self):
        return "postgres"