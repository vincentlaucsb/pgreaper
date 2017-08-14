''' Things that don't need to be optimized in Cython should be kept here '''

from io import StringIO
import csv

def clean_line(line, table):
    '''
    Take in a line of strings and cast them to the proper type
    
    Parameters
    -----------
    line:       list
                List of strings from CSV reader
    table:      Table
    '''

    new_line = []
    
    for i in line:
        j = i.replace(' ', '')

        if not j:
            # Empty string
            new_line.append(j)
        elif j.isnumeric():
            new_line.append(int(j))
        elif (j[0] == '-') and j.count('.') <= 1:
            new_line.append(float(j))
        else:
            new_line.append(i)
                
    table.append(new_line)
    
# Helper class for lazy loading files
# def chunk_file(table, line_num, infile, reader, chunk_size=7500):
    # '''
    # Lazy load a file in separate chunks of StringIO objects
    # '''
    
    # string = StringIO()
    # writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    
    # try:
        # for line in reader:
            # line_num += 1
            # writer.writerow(line)
            
            # if (line_num != 0) and (line_num % chunk_size == 0):
                # yield string
                # string = StringIO()
                # writer = csv.writer(string, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    # except:
        # infile.close()            
    # yield string