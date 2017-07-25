from sqlify.core._core import alias_kwargs
from sqlify.config import POSTGRES_DEFAULT_DATABASE, POSTGRES_DEFAULT_HOST, \
    POSTGRES_DEFAULT_PASSWORD, POSTGRES_DEFAULT_USER
from sqlify.notebook.css import SQLIFY_CSS
from sqlify import postgres

from sqlalchemy import Table, MetaData, create_engine, \
    Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from collections import Iterable

@alias_kwargs
def new_engine(database=None, username=None, password=None, host='localhost'):
    if not username:
        username = POSTGRES_DEFAULT_USER
    if not password:
        password = POSTGRES_DEFAULT_PASSWORD
    if not host:
        host = POSTGRES_DEFAULT_HOST
    if not database:
        database = POSTGRES_DEFAULT_DATABASE

    return create_engine(
        'postgresql+psycopg2://{username}:{password}@{host}/{database}'.format(
            username=username, password=password, host=host, database=database)
    )

class SQLTable(list):
    '''
    Like Table, also represents a SQL Table
     * Same API for modifying contents
    
    Unlike Table
     * Is linked to a live database
     * Has commit() method to update an actual SQL table
     
    Arguments:
     * name:        Name of SQL Table
     * col_names:   Names of columns (set to None if table exists)
     * engine:      SQLAlchemy engine
    '''
    
    def __init__(self,
        name,
        engine,
        col_names=None,
        *args, **kwargs):
        super(SQLTable, self).__init__()
        
        ''' SQLAlchemy Session '''
        self.engine = engine
        self._sessionmaker = sessionmaker()
        self._sessionmaker.configure(bind=engine)
        self.session = self._sessionmaker()
        
        ''' SQLAlchemy-loadable object '''
        self._base = declarative_base()
        
        if col_names:
            self.col_names = col_names
            
            class Record_(self._base):
                # Represents one row of data
                __tablename__ = name
                id = Column(Integer, primary_key=True)
                
            # Dynamically add attributes (column names)
            for col_name in col_names:
                setattr(Record_, col_name, Column(String))
                
            self._record = Record_
        else:
            self._load_schema_from_db(name)
            self._load_data_from_db(name)
        
    def _load_schema_from_db(self, name):
        ''' Verify a Table exists and load it '''
        
        if postgres.table_exists(name, engine=self.engine):
            meta = MetaData(bind=self.engine)
            
            # Reflect schema from database
            class Record_(self._base):
                __table__ = Table(name, meta,
                    autoload=True, autoload_with=self.engine)
            
            self._record = Record_
            
            # Load column names
            self.col_names = [col.name for col in meta.tables[name].columns \
                if not col.primary_key]
        else:
            raise ValueError('Column names must be specified if {} does not exist yet.'.format(name))
        
    def _load_data_from_db(self, name):
        ''' Load records from a SQL Table '''
        
        for row in self.session.query(self._record).all():
            super(SQLTable, self).append(row)
    
    def _repr_html_(self):
        ''' Pretty printing for Jupyter notebooks '''
        
        row_data = ''
        
        # Print only first 100 rows
        for row in self[0: min(len(self), 100)]:
            row_data += '<tr><td>{0}</td></tr>'.format(
                '</td><td>'.join([str(getattr(row, col_name)) \
                    for col_name in self.col_names]))
        
        html_str = SQLIFY_CSS + '''     
            <table class="sqlify-table">
                <thead>
                    <tr><th>{col_names}</th></tr>
                </thead>
                <tbody>
                    {row_data}
                </tbody>
            </table>'''.format(
                col_names = '</th><th>'.join([i for i in self.col_names]),
                # col_types = '</th><th>'.join([i for i in self.col_types]),
                row_data = row_data
            )
        
        return html_str
    
    def append(self, row):
        ''' Parse a list of data and add it as a new Record '''
        
        if isinstance(row, Iterable):
            kwargs = {}
            
            if len(self.col_names) != len(row):
                raise ValueError('Mismatch between column names and row values.')
            
            for col_name, value in zip(self.col_names, row):
                kwargs[col_name] = value
                
            super(SQLTable, self).append(self._record(**kwargs))
        else:
            raise ValueError('append() only takes lists or other iterables.')
            
    def commit(self):
        ''' Load all rows into the database '''
        
        self._base.metadata.create_all(self.engine)  # Create table if necessary
        
        for row in self:
            self.session.add(row)
        
        self.session.new
        self.session.commit()