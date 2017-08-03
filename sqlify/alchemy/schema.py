from sqlify.sqlite.schema import table_exists as table_exists_sqlite
from sqlify.postgres.schema import table_exists as table_exists_pg

class SQLDialect(object):
    def __init__(self, table_exists):
        self.table_exists = table_exists
        
class DialectSQLite(SQLDialect):
    def __init__(self):
        table_exists = table_exists_sqlite
        super(DialectSQLite, self).__init__(table_exists)
        
    def __eq__(self, other):
        ''' Make it so self == 'sqlite' returns True '''
        if isinstance(other, str):
            if other == 'sqlite':
                return True
        else:
            return super(DialectPostgres, self).__eq__(other)
        
    def __repr__(self):
        return "sqlite"
        
class DialectPostgres(SQLDialect):
    def __init__(self):
        table_exists = table_exists_pg
        super(DialectPostgres, self).__init__(table_exists)
        
    def __eq__(self, other):
        ''' Make it so self == 'postgres' returns True '''
        if isinstance(other, str):
            if other == 'postgres':
                return True
        else:
            return super(DialectPostgres, self).__eq__(other)
        
    def __repr__(self):
        return "postgres"