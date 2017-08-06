from sqlify.core.schema import DialectPostgres
from sqlify.core.table import Table

from collections import defaultdict
from io import StringIO
import csv
import json

def json_to_table(json_data, name=None, flatten=1, extract=None):
    '''
    Arguments:    
     * json:    File or list of JSON dicts
     * flatten: Level of flattening to perform
     * extract: In addition to flattening, pull out records according to extract
                Should be in this format:
                 * {'new_column_name': 'key -> key -> key'}
     
    Adds JSON after performing one level of flattening
    '''
    
    # Parse extract dict
    new_extract = {}
    
    if extract:
        for col, path in zip(extract.keys(), extract.values()):
            new_extract[col] = path.split('->')
            
    if not (flatten == 0 or flatten == 1):
        raise NotImplementedError('Flattening is only supported up to one level.')
    
    # Parse json argument
    if isinstance(json_data, str):
        with open(json_data, mode='r') as json_file:
            json_str = ''
            for i in json_file:
                json_str += i
        json_data = json.loads(json_str)
    elif isinstance(json_data, dict):
        json_data = [json_data]
    elif not isinstance(json_data, list):
        raise ValueError("'Argument 'json_data' should either be a filename,"
            "a dict, or a list.")
        
    table = _json_to_table(json_data, name, flatten, extract=new_extract)
    table.guess_type()

    return table

def _json_to_table(json, name, flatten, extract):
    ''' Should return a properly formatted Table '''

    col_values = defaultdict(list)
    first_row = True
    
    # Needs to create lists of uniform length for each column
    for i in json:
        if flatten == 1:
            keys = set(i.keys()).union(set(col_values.keys())).difference(
                set(extract.keys()))
        
            for k in keys:
                # Did column previously exist... if not, fill in
                if not col_values[k] and not first_row:
                    n_rows = len(list(col_values.values())[0])
                    col_values[k] = [None] * n_rows
            
                try:
                    col_values[k].append(i[k])
                except KeyError:
                    col_values[k].append(None)
        else:
            col_values['json'].append(i)

        # Extract values according to extract dict
        for col, path in zip(extract.keys(), extract.values()):
            try:
                value = i
                for k in path:
                    value = value[k]
                    
                col_values[col].append(value)
            except (KeyError, IndexError) as e:
                col_values[col].append(None)
            
        if first_row:
            first_row = False

    if flatten == 0:
        import pdb; pdb.set_trace()
    
    return Table(dialect=DialectPostgresJSON(),
        name=name,
        col_names=col_values.keys(),
        col_values=list(col_values.values()))

class DialectPostgresJSON(DialectPostgres):
    ''' A dialect for Postgres tables containing JSONB columns '''

    def __init__(self):
        super(DialectPostgresJSON, self).__init__()
    
    def guess_type(self, table, sample_n):
        ''' 
        Gets column types for a table, with possibilities:
        JSONB, TEXT, DOUBLE PRECISION, BIGINT, BOOLEAN, DATETIME
        '''
       
        # Counter of data types per column
        data_types = [defaultdict(int) for col in table.col_names]
        check_these_cols = set([i for i, name in enumerate(table.col_names)])
        sample_n = min(len(table), sample_n)
        
        for i, row in enumerate(table):
            # Every 100 rows, check if JSONB is there already
            if i > sample_n:
                break
            if i%100 == 0:
                remove = [j for j in check_these_cols if data_types[j]['JSONB']]
                
                for j in remove:
                    check_these_cols.remove(j)
            if table.n_cols == 1:
                row = [row]
                
            # Loop over individual items
            for j in check_these_cols:
                data_types[j][self.guess_data_type(row[j])] += 1
        
        col_types = []
        
        for col in data_types:
        
            if col['JSONB']:
                this_col_type = 'JSONB'
            elif col['TEXT']:
                this_col_type = 'TEXT'
            elif bool(col['DOUBLE PRECISION'] or col['INT']) + \
                bool(col['BOOLEAN']) + bool(col['DATETIME']) > 1:
                # If the above sum > 1, there are mixed incompatible types
                this_col_type = 'TEXT'
            elif col['DOUBLE PRECISION']:
                this_col_type = 'DOUBLE PRECISION'
            elif col['BIGINT']:
                this_col_type = 'BIGINT'
            elif col['BOOLEAN']:
                this_col_type = 'BOOLEAN'
            # elif col['DATETIME']:
                # this_col_type = 'DATETIME'
            else:
                # Column of NULLs
                this_col_type = 'TEXT'
            
            col_types.append(this_col_type)
            
        return col_types
        
    @staticmethod
    def guess_data_type(item):
        ''' A more extensive but also expensive type guesser for Postgres '''

        if item is None:
            return 'NULL'
        elif isinstance(item, dict):
            return 'JSONB'
        elif isinstance(item, bool):
            return 'BOOLEAN'
        elif isinstance(item, int):
            return 'BIGINT'
        elif isinstance(item, float):
            return 'DOUBLE PRECISION'
        else:
            # Strings and other types
            if item.isnumeric():
                return 'BIGINT'
            elif (not item.isnumeric()) and \
                (item.replace('.', '', 1).replace('-', '', 1).isnumeric()):
                '''
                Explanation:
                 * A floating point number, e.g. '-3.14', in string will not be 
                   recognized as being a number by Python via .isnumeric()
                 * However, after removing the '.' and '-', it should be
                '''
                return 'DOUBLE PRECISION'
            else:
                return 'TEXT'
                
    @staticmethod
    def to_string(table):
        ''' Return table as a StringIO object for writing via copy() '''
        
        string = StringIO()
        writer = csv.writer(string, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        dict_encoder = json.JSONEncoder()
        
        jsonb_cols = set([i for i, j in enumerate(table.col_types) if j == 'JSONB'])
        
        for row in table:
            for i in jsonb_cols:
                row[i] = dict_encoder.encode(row[i])
        
            writer.writerow(row)
            
        string.seek(0)
        return string