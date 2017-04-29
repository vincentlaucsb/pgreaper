from .table import Table
import csv

# Remove bad characters from column names
def _sanitize_table(obj):
    '''
    Arguments:
     * obj = A Table object
    
    This function has no return value--it modifies Tables in place.
    '''
    
    new_col_names = [_strip(name) for name in obj.col_names]
    obj.col_names = new_col_names

# Removes or fixes no-nos from potential table and column names
def _strip(string):
    # Replace bad characters
    offending_characters = ['.', ',', '-', ';']
    new_str = ""
    
    for char in string:
        if char not in offending_characters:
            new_str += char
        else:
            new_str += '_'
            
    # Add underscore if name starts with a number
    numbers = list(range(0, 10))
    starts_with_number = bool(True in [string.startswith(str(n)) for n in numbers])
    
    if starts_with_number:
        new_str = "_" + new_str
    
    # Remove white space
    if ' ' in string:
        new_str = new_str.replace(' ','')
    
    return new_str
    
# Performs similar things for _text_to_table and _csv_to_table
def _preprocess(func):
    def inner(*args, **kwargs):
        # Use filename as default value for table name
        try:
            if not kwargs['name']:
                # Strip out file extension
                kwargs['name'] = _strip(kwargs['file'].split('.')[0])
        except KeyError:
            kwargs['name'] = _strip(kwargs['file'].split('.')[0])
                
        return func(*args, **kwargs)
    
    return inner
        
    
# Convert text file to Table object
@_preprocess
def text_to_table(file, name, delimiter, header=True, **kwargs):
    '''
    Arguments:
     * file:     text file
     * database: sqlite3.Connection object
     * header:   True if the first row contains column names
     * delimiter
    '''
    
    # import pdb; pdb.set_trace()
    
    # Split one line according to delimiter
    def split_line(line):
        line = line.replace('\n', '')
    
        if delimiter:
            line = line.split(delimiter)
        
        return line
    
    with open(file, 'r') as infile:
        line_num = 0
        row_values = []
    
        for line in infile:
            if header and (line_num == 0):
                col_names = split_line(line)
                
            else:
                row_values.append(split_line(line))
                
            line_num += 1
    
    return Table(name, col_names=col_names, row_values=row_values, **kwargs)
    
# Convert CSV file to Table object
@_preprocess
def csv_to_table(file, name, delimiter=',' , skip_lines=0, header=True, **kwargs):
    '''
    Arguments:
     * skip_lines: Skip the first n lines of the CSV file
    
    '''
    
    with open(file, newline='') as csvfile:
        data = csv.reader(csvfile, delimiter=delimiter)
        
        line_num = 0
        row_values = []
    
        for line in data:
            if header and (line_num == 0):
                col_names = line
                
            elif line_num + 1 > skip_lines:
                row_values.append(line)
                
            line_num += 1
            
    return Table(name, col_names=col_names, row_values=row_values, **kwargs)