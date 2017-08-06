''' Provides a uniform interface for getting metadata from SQL databases '''

from abc import abstractmethod
from sqlify._globals import Singleton
from sqlify.sqlite.schema import table_exists as table_exists_sqlite

class DBDialect(metaclass=Singleton):
    '''
    Executes or generates SQL statements
     * Executes SQL statements if they don't modify structure or data
     * Otherwise, generates them as strings
    '''
    
    @abstractmethod
    def get_primary_keys(self):
        pass        
        
class DBSQLite(DBDialect):
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