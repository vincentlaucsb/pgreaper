import csv
import json

def table_to_csv(obj, file, header=True, delimiter=','):
    ''' Convert a Table object to CSV
    
    Arguments:
     * obj:     Table object to be converted
     * file:    Name of the file
     * header:  Include the column names    
    '''
    
    with open(file, mode='w', newline='\n') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"')
        
        csv_writer.writerow(obj.col_names)
        
        for row in obj:
            csv_writer.writerow(row)
            
def table_to_json(obj, file, header=True, delimiter=','):
    ''' Convert a Table object to JSON '''
    
    new_json = []
    
    for row in obj:
        json_row = {}
        
        for i, item in enumerate(row):
            json_row[obj.col_names[i]] = item
            
        new_json.append(json_row)
        
    with open(file, mode='w') as file:
        file.write(json.dumps(new_json))