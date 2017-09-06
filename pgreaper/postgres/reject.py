''' Tools for dealing with conflicting schema '''

__all__ = ['schema_diff', 'reject_diff', 'print_schema_diff']

from pgreaper.core import ColumnList
from pgreaper.core.table import Table
from .conn import postgres_connect
from .database import read_pg, get_schema, get_table_schema

from collections import defaultdict
import psycopg2
import json

@postgres_connect
def schema_diff(tables, conn=None, **kwargs):
    ''' Return the differences in schema between tables '''
    
    # Maps column names to maps of column types to table names
    cols = defaultdict(lambda: defaultdict(list))
    
    # Build up cols
    for i in tables:
        schema = get_table_schema(i, conn=conn)
        for cname, ctype in schema.as_tuples():
            cols[cname][ctype].append(i)
            
    # Filter out everything with the same column type
    return {name: d for name, d in cols.items() if len(d.keys()) > 1}
    
def print_schema_diff(*args, **kwargs):
    ''' schema_diff() with pretty printing '''
    
    sdiff = schema_diff(*args, **kwargs)
    for col in sdiff:
        print(col, '-'*75, sep='\n')
        for ctype in sdiff[col]:
            print(ctype)
            print('\n')
            print(sdiff[col][ctype])
            print('\n')
    
@postgres_connect
def reject_diff(conn=None, **kwargs):
    '''
    Print a detailed report of the differences between tables 
    that had rejected rows and create statements for merging them
    '''
    
    rejects = [i for i in get_schema(conn=conn, columns=False) \
        if i[-7:] == '_reject']
    pairs = [(i.replace('_reject', ''), i) for i in rejects]
        
    for orig, rej in pairs:
        sdiff = schema_diff(tables=[orig, rej], conn=conn)
        
        # Print 10 values from just conflicting columns
        print('{} vs. {}'.format(orig, rej))
        cols = list(sdiff.keys())
        
        select1 = 'SELECT {} FROM {} LIMIT 10'.format(','.join(cols), orig)
        select2 = 'SELECT {} FROM {} LIMIT 10'.format(','.join(cols), rej)
        
        print(read_pg(select1, conn=conn))
        print(read_pg(select2, conn=conn))
        
        # Print ALTER TABLE statements for changing dtype to rejects
        for i in cols:
            type_ = [j for j in sdiff[i].keys() if '_reject' in sdiff[i][j][0]][0]
            alter_table = ('ALTER TABLE {tbl} ALTER COLUMN {col} '
                           'TYPE {type_} USING {col}::{type_};').format(
                tbl=orig, col=i, type_=type_)
            print(alter_table)
            
        # Print INSERT statement
        print('INSERT INTO {} SELECT * FROM {};'.format(orig, rej))
        print('DROP TABLE {};'.format(rej))