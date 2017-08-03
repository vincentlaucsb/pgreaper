''' Contains a two-dimensional data structure containing SQLAlchemy objects '''

from sqlify._globals import POSTGRES_CONN_KWARGS
from sqlify.config import PG_DEFAULTS
from sqlify.core._base_table import BaseTable
from sqlify.core._core import alias_kwargs
from .schema import DialectSQLite, DialectPostgres

import sqlalchemy
from sqlalchemy import Table, MetaData, create_engine, \
    Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from collections import Iterable
from types import MethodType
import re

@alias_kwargs
def new_engine(database=None, **kwargs):
    def new_engine_pg(
        database=PG_DEFAULTS['database'],
        username=PG_DEFAULTS['user'],
        password=PG_DEFAULTS['password'],
        host=PG_DEFAULTS['host']):
        
        return create_engine(
            'postgresql+psycopg2://{username}:{password}@{host}/{database}'.format(
            username=username, password=password, host=host, database=database))

    if POSTGRES_CONN_KWARGS.intersection(set(kwargs.keys())):
        return new_engine_pg(database, **kwargs)
    else:
        return create_engine('sqlite:///{}'.format(database))

def record_iter(self):
    return iter([getattr(self, i) for i in self.col_names])
        
class SQLTable(BaseTable):
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
    
    def __init__(self, name, engine, col_names=None,
        *args, **kwargs):
        
        super(SQLTable, self).__init__(name=name)
        
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
                
            self.record = Record_
        else:
            try:
                self._load_schema_from_db(name)
                self._load_data_from_db(name)
            except sqlalchemy.exc.ArgumentError as e:
                # If no pkey, but we're using Postgres, create updatable view
                if self.dialect == 'postgres':
                    self._load_schema_from_db(name=name, create_pkey=True)
                    self._load_data_from_db(name)
                    
                # Otherwise, return error
                else:
                    raise ValueError(e)                    
    
    def base_class(self):
        '''
        Creates a base class for storing table rows
        
        Arguments:
         * base:    SQLAlchemy declarative_base
        '''
        
        pass
            
    @property
    def engine(self):
        return self._engine
        
    @engine.setter
    def engine(self, value):
        self._engine = value
    
        # Automatically set the correct dialect
        url = str(value.url)
        
        if 'sqlite' in url:
            self.dialect = DialectSQLite()
        elif 'postgres' in url:
            self.dialect = DialectPostgres()
            
        # Get the database name
        self._database = url.split('/')[-1]
        
        # Don't save url since it contains uncensored password
        self.source = re.match('Engine\((.*)\)', str(value)).group(1)
        
    @property
    def col_names(self):
        return self._col_names
        
    @col_names.setter
    def col_names(self, value):
        self._col_names = value
        self.n_cols = len(value)  # Update n_cols
    
    def _table_exists(self, table):
        return self.dialect.table_exists(
            engine=self.engine, database=self._database, table=table)
    
    def _load_schema_from_db(self, name, create_pkey=False):
        '''
        Verify a Table exists and load it
        
        Arguments:
         * create_pkey:     (Postgres) Create an updatable view with a pkey        
        '''
        
        if create_pkey:
            self._create_pkey(name=name)
        
        if self._table_exists(name):
            meta = MetaData(bind=self.engine)
            
            # Reflect schema from database
            class Record_(self._base):
                __table__ = Table(name, meta,
                    autoload=True, autoload_with=self.engine)
                    
                # Add iteration
                def __iter__(self):
                    for i in Record_.col_names:
                        yield getattr(self, i)
                
                # Add indexing
                def __getitem__(self, x):
                    return getattr(self, x)
                    
                def __setitem__(self, key, value):
                    return setattr(self, key, value)
            
            # Load column names and types
            self.col_names = [col.name for col in meta.tables[name].columns \
                if not col.primary_key]
            self.col_types = [str(col.type) for col in meta.tables[name].columns \
                if not col.primary_key]
                
            self.record = Record_
            self.record.col_names = self.col_names  # Class attrib
        else:
            raise ValueError('Column names must be specified if {} does not exist yet.'.format(name))
        
    def _load_data_from_db(self, name):
        ''' Load records from a SQL Table '''
        for row in self.session.query(self.record).all():
            super(SQLTable, self).append(row)
    
    def _create_pkey(self, name):
        ''' Create an updatable view with a primary key (Postgres) '''
        
        conn = self.engine.connect()
        dbapi_conn = conn.connection
        cur = dbapi_conn.cursor()
        
        cur.execute('''
            ALTER TABLE {} ADD COLUMN id BIGSERIAL PRIMARY KEY
        '''.format(name))
        
        dbapi_conn.commit()
        
    ''' Table Manipulation Functions ''' 
    def _parse_col(self, col):
        ''' Finds the column index a column name is referering to '''
        return col
    
    ''' SQL Manipulation Functions '''
    def append(self, row):
        ''' Parse a list of data and add it as a new Record '''
        
        if isinstance(row, Iterable):
#            kwargs = {}
            
            if len(self.col_names) != len(row):
                raise ValueError('Mismatch between column names and row values.')
            
#            for col_name, value in zip(self.col_names, row):
#                kwargs[col_name] = value
                
            super(SQLTable, self).append(
                self.record(**{x:y for x, y in zip(self.col_names, row)}))
        else:
            raise ValueError('append() only takes lists or other iterables.')
            
    def commit(self):
        ''' Load all rows into the database '''
        
        self._base.metadata.create_all(self.engine)  # Create table if necessary
        
        for row in self:
            self.session.add(row)
        
        self.session.new
        self.session.commit()